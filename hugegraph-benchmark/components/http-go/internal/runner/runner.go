// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runner

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/apache/hugegraph-toolchain/hugegraph-benchmark/components/http-go/internal/compat"
)

var errDrain = errors.New("drain-timeout")
var errEvidence = errors.New("evidence-failure")

func ms(n float64) time.Duration { return time.Duration(n * 1e6) }
func pointer[T any](v T) *T      { return &v }
func (in Input) Validate() error {
	p := in.Profile
	e := p.Executor
	l := in.Load
	if in.SchemaVersion != 2 || p.SchemaVersion != 2 || p.Kind != "http-benchmark-profile" || e.Engine != Engine {
		return errors.New("unsupported Go request/profile protocol")
	}
	if in.ID == "" || in.Case.ID == "" || l.ID == "" || in.Repeat < 0 {
		return errors.New("missing request identity")
	}
	u, er := url.Parse(p.Endpoint)
	if er != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid endpoint")
	}
	if e.GOMAXPROCS < 1 || e.GOMAXPROCS > 32 || e.MaxPendingBytes < 65536 || e.MaxPendingBytes > 1048576 || e.CPUQuotaCores <= 0 || e.CPUQuotaCores > 32 || math.IsNaN(e.CPUQuotaCores) || math.IsInf(e.CPUQuotaCores, 0) || e.MaxRSSBytes < 67108864 || e.MaxRSSBytes > 34359738368 {
		return errors.New("invalid executor resource bounds")
	}
	for _, n := range []float64{p.WarmupMs, p.DurationMs, p.RequestTimeoutMs, p.DrainMs, l.Rate, l.MaxStartDelayMs} {
		if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
			return errors.New("invalid finite timing/load bound")
		}
	}
	if p.WarmupMs > 60000 || p.DurationMs < 100 || p.DurationMs > 300000 || p.RequestTimeoutMs < 10 || p.RequestTimeoutMs > 60000 || p.DrainMs < 10 || p.DrainMs > 60000 || p.MaxRequests < 1 || p.MaxRequests > 1000000 || p.MaxResponseBytes < 256 || p.MaxResponseBytes > 4194304 || p.MaxTraceBytes < 4096 || p.MaxTraceBytes > 1073741824 || in.RemainingTraceBytes < 0 || in.RemainingTraceBytes > p.MaxTraceBytes {
		return errors.New("invalid execution budget")
	}
	if p.ResponseEncoding != "identity" && p.ResponseEncoding != "gzip" {
		return errors.New("invalid response encoding")
	}
	if (l.Model == "closed" && (l.Concurrency < 1 || l.Concurrency > 256)) || (l.Model == "open" && (l.Rate < 0.1 || l.Rate > 10000 || l.MaxInFlight < 1 || l.MaxInFlight > 256 || l.MaxStartDelayMs < 1 || l.MaxStartDelayMs > 1000 || math.Ceil(math.Max(p.WarmupMs, p.DurationMs)*l.Rate/1000) > float64(p.MaxRequests))) || (l.Model != "open" && l.Model != "closed") {
		return errors.New("invalid load")
	}
	if in.ProbePhase != "" && ((in.ProbePhase != "preflight" && in.ProbePhase != "postcheck") || l.Model != "closed" || l.Concurrency != 1) {
		return errors.New("invalid functional probe")
	}
	if in.Case.ExpectedStatus < 100 || in.Case.ExpectedStatus > 599 || len(in.Case.Checks) < 1 || len(in.Case.Checks) > 32 || !json.Valid(in.Case.Body) {
		return errors.New("invalid case")
	}
	for _, c := range in.Case.Checks {
		if _, e := compat.Parse(c.Pointer); e != nil {
			return errors.New("invalid JSON pointer string")
		}
		var s *string
		if json.Unmarshal(c.Pointer, &s) != nil || s == nil || (*s != "" && !strings.HasPrefix(*s, "/")) {
			return errors.New("invalid JSON pointer")
		}
		if (c.CanonicalSHA256 != "") == (len(c.Equals) > 0) {
			return errors.New("each check needs exactly one expected value or digest")
		}
		if c.CanonicalSHA256 != "" {
			b, e := hex.DecodeString(c.CanonicalSHA256)
			if e != nil || len(b) != 32 || strings.ToLower(c.CanonicalSHA256) != c.CanonicalSHA256 {
				return errors.New("invalid check digest")
			}
		} else if _, e := compat.Parse(c.Equals); e != nil {
			return errors.New("invalid check value")
		}
	}
	if p.AuthorizationEnv != nil {
		for i, c := range *p.AuthorizationEnv {
			if !(c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9' || c == '_') {
				return errors.New("invalid authorization env name")
			}
		}
		if *p.AuthorizationEnv == "" {
			return errors.New("empty authorization env name")
		}
	}
	return validateGraphPool(in)
}

// Transport is explicit: no proxy, redirects, HTTP/2, transparent decompression or replayable POST body.
func newTransport(maxInFlight int) *http.Transport {
	return &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: false, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}, MaxIdleConns: maxInFlight, MaxIdleConnsPerHost: maxInFlight, MaxConnsPerHost: maxInFlight, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ExpectContinueTimeout: time.Second, DisableCompression: true, MaxResponseHeaderBytes: 65536}
}
func expectedChecks(c Case) ([]CheckDigest, error) {
	out := make([]CheckDigest, 0, len(c.Checks))
	for _, r := range c.Checks {
		d := r.CanonicalSHA256
		if d == "" {
			v, e := compat.Parse(r.Equals)
			if e != nil {
				return nil, e
			}
			d = v.Digest()
		}
		out = append(out, CheckDigest{true, &d})
	}
	return out, nil
}
func actualChecks(raw []byte, c Case) ([]CheckDigest, error) {
	raw = bytes.TrimPrefix(raw, []byte{0xef, 0xbb, 0xbf})
	v, e := compat.Parse(raw)
	if e != nil {
		return nil, e
	}
	out := make([]CheckDigest, 0, len(c.Checks))
	for _, rule := range c.Checks {
		q, ok := v.PointerJSON(rule.Pointer)
		if !ok {
			out = append(out, CheckDigest{})
		} else {
			d := q.Digest()
			out = append(out, CheckDigest{true, &d})
		}
	}
	return out, nil
}
func equalChecks(a, b []CheckDigest) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Present != b[i].Present || a[i].SHA256 == nil || b[i].SHA256 == nil || *a[i].SHA256 != *b[i].SHA256 {
			return false
		}
	}
	return true
}
func contextOutcome(phase, request context.Context) string {
	if errors.Is(context.Cause(phase), errDrain) {
		return "drain-timeout"
	}
	if errors.Is(context.Cause(phase), errEvidence) {
		return "evidence-failure"
	}
	if phase.Err() != nil {
		return "cancelled"
	}
	if request.Err() != nil {
		return "timeout"
	}
	return "transport-error"
}

func perform(ctx context.Context, client *http.Client, in Input, body []byte, auth string, start time.Time, sequence int, at float64, expected []CheckDigest) (r Record) {
	p := in.Profile
	method, endpoint := "POST", p.Endpoint
	var graph *GraphRequest
	if len(in.Case.RequestPool) > 0 {
		index := sequence % len(in.Case.RequestPool)
		graph = &in.Case.RequestPool[index]
		method, endpoint, body = graph.Method, graph.URL, graph.Body
	}
	r = Record{SchemaVersion: RawVersion, Engine: Engine, Sequence: sequence, ScheduledMs: at}
	if graph != nil {
		r.ParameterIndex = pointer(sequence % len(in.Case.RequestPool))
	}
	r.SentMs = pointer(float64(time.Since(start)) / 1e6)
	r.Late = in.Load.Model == "open" && *r.SentMs-at > in.Load.MaxStartDelayMs
	requestCtx, cancel := context.WithTimeout(ctx, ms(p.RequestTimeoutMs))
	defer cancel()
	var traceMu sync.Mutex
	var socketMs *float64
	var reusedSocket *bool
	connections := 0
	defer func() {
		traceMu.Lock()
		defer traceMu.Unlock()
		r.SocketMs = socketMs
		r.ReusedSocket = reusedSocket
		if r.ReusedSocket == nil {
			r.ReusedSocket = pointer(false)
		}
	}()
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		traceMu.Lock()
		defer traceMu.Unlock()
		connections++
		if connections > 1 {
			cancel()
		} // Abort any transparent GET retry before sending again.
		socketMs = pointer(float64(time.Since(start)) / 1e6)
		reusedSocket = pointer(info.Reused)
	}}
	req, e := http.NewRequestWithContext(httptrace.WithClientTrace(requestCtx, trace), method, endpoint, io.NopCloser(bytes.NewReader(body)))
	if e != nil {
		r.Outcome = "transport-error"
		r.EndMs = pointer(float64(time.Since(start)) / 1e6)
		return r
	}
	if method == "GET" {
		req.Body = nil
		body = nil
	}
	req.ContentLength = int64(len(body))
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", p.ResponseEncoding)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	response, e := client.Do(req)
	if e != nil {
		r.Outcome = contextOutcome(ctx, requestCtx)
		r.EndMs = pointer(float64(time.Since(start)) / 1e6)
		return r
	}
	r.Status = pointer(response.StatusCode)
	encoding := strings.ToLower(response.Header.Get("Content-Encoding"))
	if encoding == "" {
		encoding = "identity"
	}
	r.ResponseEncoding = &encoding
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, p.MaxResponseBytes+1))
	r.EndMs = pointer(float64(time.Since(start)) / 1e6)
	r.Bytes = len(raw)
	response.Body.Close()
	if int64(len(raw)) > p.MaxResponseBytes {
		r.Outcome = "response-too-large"
		return r
	}
	if readErr != nil {
		r.Outcome = contextOutcome(ctx, requestCtx)
		if r.Outcome == "transport-error" {
			r.Outcome = "response-error"
		}
		return r
	}
	r.CompleteResponse = true
	validationStart := time.Now()
	sum := sha256.Sum256(raw)
	r.BodySHA256 = pointer(hex.EncodeToString(sum[:]))
	r.Outcome = "ok"
	if response.ProtoMajor != 1 || response.ProtoMinor != 1 {
		r.Outcome = "protocol-error"
	} else if response.StatusCode != func() int {
		if graph != nil {
			return graph.ExpectedStatus
		}
		return in.Case.ExpectedStatus
	}() {
		r.Outcome = "http-status"
	} else if encoding != "identity" && !(p.ResponseEncoding == "gzip" && encoding == "gzip") {
		r.Outcome = "unexpected-encoding"
	}
	if r.Outcome == "ok" && encoding == "gzip" {
		decodeStart := time.Now()
		gz, e := gzip.NewReader(bytes.NewReader(raw))
		if e != nil {
			r.Outcome = "invalid-gzip"
		} else {
			raw, e = io.ReadAll(io.LimitReader(gz, p.MaxResponseBytes+1))
			closeErr := gz.Close()
			if int64(len(raw)) > p.MaxResponseBytes {
				r.Outcome = "decoded-response-too-large"
			} else if e != nil || closeErr != nil {
				r.Outcome = "invalid-gzip"
			}
		}
		r.DecodeMs = pointer(float64(time.Since(decodeStart)) / 1e6)
	}
	if r.Outcome == "ok" && graph != nil {
		r.DecodedBytes = pointer(len(raw))
		r.GraphValue, e = graphProjection(bytes.TrimSpace(raw), graph.Rule)
		r.CheckDigests = []CheckDigest{}
		if e != nil {
			r.Outcome = "invalid-json"
		} else if !validGraphValue(r.GraphValue, graph.Rule) {
			r.Outcome = "wrong-answer"
		}
	} else if r.Outcome == "ok" {
		r.DecodedBytes = pointer(len(raw))
		r.CheckDigests, e = actualChecks(raw, in.Case)
		if e != nil {
			r.Outcome = "invalid-json"
		} else if !equalChecks(r.CheckDigests, expected) {
			r.Outcome = "wrong-answer"
		}
	}
	r.ValidationMs = pointer(float64(time.Since(validationStart)) / 1e6)
	return r
}

func waitUntil(ctx context.Context, at time.Time) bool {
	for {
		d := time.Until(at)
		if d <= 0 {
			return ctx.Err() == nil
		}
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
			return ctx.Err() == nil
		case <-ctx.Done():
			timer.Stop()
			return false
		}
	}
}
func phase(parent context.Context, c *clock, w *TraceWriter, client *http.Client, in Input, body []byte, auth, name string, duration float64, expected []CheckDigest) Phase {
	startResources := resources()
	boundary, start := c.boundary()
	end := start.Add(ms(duration))
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	drain := time.AfterFunc(ms(duration+in.Profile.DrainMs), func() { cancel(errDrain) })
	defer drain.Stop()
	p := Phase{Name: name, DurationMs: duration, Counts: map[string]int{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	finish := func(r Record) {
		r.Phase = name
		mu.Lock()
		p.Counts[r.Outcome]++
		if r.SentMs == nil {
			p.Dropped++
		} else {
			p.Sent++
			p.Completed++
			if r.Outcome != "ok" {
				p.Failed++
			}
		}
		if r.Late {
			p.Late++
		}
		mu.Unlock()
		if e := w.Append(r); e != nil {
			cancel(errEvidence)
		}
	}
	execute := func(sequence int, at float64) {
		r := perform(ctx, client, in, body, auth, start, sequence, at, expected)
		finish(r)
	}
	if in.ProbePhase != "" {
		p.Scheduled = 1
		execute(0, 0)
	} else if in.Load.Model == "open" {
		tokens := make(chan struct{}, in.Load.MaxInFlight)
		total := int(math.Ceil(duration * in.Load.Rate / 1000))
		p.Scheduled = total
		for sequence := 0; sequence < total; sequence++ {
			at := float64(sequence) * 1000 / in.Load.Rate
			waitUntil(ctx, start.Add(ms(at)))
			now := float64(time.Since(start)) / 1e6
			outcome := ""
			if ctx.Err() != nil {
				outcome = "cancelled-before-send"
			} else if now-at > in.Load.MaxStartDelayMs || !time.Now().Before(end) {
				outcome = "scheduler-late"
			} else {
				select {
				case tokens <- struct{}{}:
					wg.Add(1)
					go func(seq int, scheduled float64) {
						defer wg.Done()
						defer func() { <-tokens }()
						execute(seq, scheduled)
					}(sequence, at)
				default:
					outcome = "concurrency-limit"
				}
			}
			if outcome != "" {
				finish(Record{SchemaVersion: RawVersion, Engine: Engine, Sequence: sequence, ScheduledMs: at, ObservedMs: &now, Outcome: outcome, Late: outcome == "scheduler-late"})
			}
		}
		wg.Wait()
		waitUntil(ctx, end)
	} else {
		for i := 0; i < in.Load.Concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ctx.Err() == nil && time.Now().Before(end) {
					mu.Lock()
					if p.Scheduled >= in.Profile.MaxRequests {
						p.BudgetExhausted = true
						mu.Unlock()
						return
					}
					seq := p.Scheduled
					p.Scheduled++
					mu.Unlock()
					execute(seq, float64(time.Since(start))/1e6)
				}
			}()
		}
		wg.Wait()
		if !p.BudgetExhausted {
			waitUntil(ctx, end)
		}
	}
	boundaryEnd, finished := c.boundary()
	p.ElapsedMs = float64(finished.Sub(start)) / 1e6
	p.Cancelled = parent.Err() != nil || errors.Is(context.Cause(ctx), errEvidence)
	p.Clock = PhaseClock{Contract: c.identity.Contract, Start: boundary, End: boundaryEnd}
	p.Resources, p.AverageCPUCores, p.MaxRSSKiB = resourceDelta(startResources, resources(), p.ElapsedMs, in.Profile.Executor)
	return p
}

func Run(ctx context.Context, in Input, tracePath string) (Result, error) {
	return RunObserved(ctx, in, tracePath, nil)
}

// RunObserved allows the controller to attest this live process outside phase windows.
// A failed observation prevents a successful run receipt and is never treated as calibration.
func RunObserved(ctx context.Context, in Input, tracePath string, observe func(context.Context, string, ClockIdentity) error) (Result, error) {
	if e := in.Validate(); e != nil {
		return Result{}, e
	}
	runtime.GOMAXPROCS(in.Profile.Executor.GOMAXPROCS)
	debug.SetGCPercent(100)
	debug.SetMemoryLimit(math.MaxInt64)
	var compact bytes.Buffer
	if e := json.Compact(&compact, in.Case.Body); e != nil {
		return Result{}, e
	}
	body := compact.Bytes()
	auth := ""
	if in.Profile.AuthorizationEnv != nil {
		auth = os.Getenv(*in.Profile.AuthorizationEnv)
		if auth == "" || strings.ContainsAny(auth, "\r\n") {
			return Result{}, errors.New("missing or invalid authorization environment value")
		}
	}
	expected, e := expectedChecks(in.Case)
	if e != nil {
		return Result{}, e
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	w, e := OpenWriter(tracePath, min(in.Profile.MaxTraceBytes, in.RemainingTraceBytes), in.Profile.Executor.MaxPendingBytes, func(error) { cancel(errEvidence) })
	if e != nil {
		return Result{}, e
	}
	maxInFlight := in.Load.MaxInFlight
	if in.Load.Model == "closed" {
		maxInFlight = in.Load.Concurrency
	}
	transport := newTransport(maxInFlight)
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	c := newClock()
	kind := "http-run"
	if in.ProbePhase != "" {
		kind = "http-functional-probe"
	}
	result := Result{SchemaVersion: RawVersion, Kind: kind, Engine: Engine, ID: in.ID, CaseID: in.Case.ID, LoadID: in.Load.ID, Repeat: in.Repeat, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), PhaseClock: c.identity}
	if observe != nil {
		if e := observe(ctx, "ready", c.identity); e != nil {
			result.TraceWriter, _ = w.Finish()
			result.TraceBytes = result.TraceWriter.PersistedBytes
			result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
			return result, fmt.Errorf("live process observation before run: %w", e)
		}
	}
	if in.ProbePhase != "" {
		result.Phases = append(result.Phases, phase(runCtx, c, w, client, in, body, auth, in.ProbePhase, in.Profile.RequestTimeoutMs, expected))
	} else {
		if in.Profile.WarmupMs > 0 {
			result.Phases = append(result.Phases, phase(runCtx, c, w, client, in, body, auth, "warmup", in.Profile.WarmupMs, expected))
		}
		if runCtx.Err() == nil {
			result.Phases = append(result.Phases, phase(runCtx, c, w, client, in, body, auth, "measurement", in.Profile.DurationMs, expected))
		}
	}
	receipt, writerErr := finishEvidence(ctx, w, result.Phases)
	result.TraceWriter = receipt
	result.TraceBytes = receipt.PersistedBytes
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	result.Client = map[string]any{"engine": Engine, "goVersion": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "http": "1.1", "keepAlive": true, "maxSockets": maxInFlight, "retries": 0, "redirects": 0, "compression": in.Profile.ResponseEncoding, "proxy": "none", "tlsVerification": "default-enabled", "authorization": map[bool]string{true: "environment-supplied", false: "none"}[auth != ""], "postReplay": "nonempty-body-getbody-nil", "nodeEluApplicable": false, "gcPercent": 100, "goMemoryLimit": "off"}
	if writerErr != nil {
		return result, fmt.Errorf("trace persistence: %w", writerErr)
	}
	if observe != nil {
		after := c.identity
		after.Process = processIdentity()
		result.ProcessIdentityAfter = after.Process
		if e := observe(ctx, "finished", after); e != nil {
			return result, fmt.Errorf("live process observation after run: %w", e)
		}
	}
	return result, nil
}

func finishEvidence(ctx context.Context, w *TraceWriter, phases []Phase) (WriterReceipt, error) {
	receipt, e := w.Finish()
	recorded := false
	for _, p := range phases {
		recorded = recorded || p.Cancelled
	}
	if e == nil && ctx.Err() != nil && !recorded {
		e = errors.New("cancelled during evidence finalization")
	}
	return receipt, e
}
