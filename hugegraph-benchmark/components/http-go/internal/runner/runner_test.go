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
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func input(endpoint string) Input {
	return Input{SchemaVersion: 2, ID: "r0001", Profile: Profile{SchemaVersion: 2, Kind: "http-benchmark-profile", Name: "test", Endpoint: endpoint, ResponseEncoding: "identity", Executor: Executor{Engine: Engine, GOMAXPROCS: 2, MaxPendingBytes: 1048576, CPUQuotaCores: 2, MaxRSSBytes: 1073741824}, DurationMs: 100, RequestTimeoutMs: 100, DrainMs: 100, MaxRequests: 1000, MaxResponseBytes: 4194304, MaxTraceBytes: 1048576}, Case: Case{ID: "answer", Body: json.RawMessage(`{"gremlin":"1+1"}`), ExpectedStatus: 200, Checks: []Check{{Pointer: json.RawMessage(`"/result/data"`), Equals: json.RawMessage(`[2]`)}}}, Load: Load{ID: "open", Model: "open", Rate: 30, MaxInFlight: 2, MaxStartDelayMs: 50}, RemainingTraceBytes: 1048576}
}
func runTest(t *testing.T, in Input, ctx context.Context) (Result, []Record) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "trace.ndjson")
	r, e := Run(ctx, in, p)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	rows := []Record{}
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var v Record
		if e = json.Unmarshal(line, &v); e != nil {
			t.Fatal(e)
		}
		rows = append(rows, v)
	}
	if int64(len(raw)) != r.TraceBytes || r.TraceWriter.Records != len(rows) || !r.TraceWriter.Complete {
		t.Fatal("writer accounting")
	}
	return r, rows
}
func handler(w http.ResponseWriter, r *http.Request) {
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, `{"result":{"data":[2]}}`)
}
func TestNormalAndKeepAlive(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Proto != "HTTP/1.1" || r.Method != "POST" || r.Header.Get("Accept-Encoding") != "identity" || r.ContentLength != 17 {
			t.Error("HTTP policy")
		}
		handler(w, r)
	}))
	defer s.Close()
	in := input(s.URL)
	in.Load.MaxInFlight = 1
	r, rows := runTest(t, in, context.Background())
	if len(rows) != 3 || r.Phases[0].Counts["ok"] != 3 {
		t.Fatal(r, rows)
	}
	reused := false
	for _, v := range rows {
		if v.ReusedSocket != nil && *v.ReusedSocket {
			reused = true
		}
		if !v.CompleteResponse || v.ValidationMs == nil || v.EndMs == nil || v.SocketMs == nil {
			t.Fatal(v)
		}
	}
	if !reused {
		t.Fatal("keepalive not observed")
	}
	if r.Phases[0].EventLoopUtilization != nil || r.Phases[0].Resources.NodeELUApplicable {
		t.Fatal("invented Node ELU")
	}
}
func TestGzipAndAnswerValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, encoding, outcome string
		cap                           int64
	}{{"good", `{"result":{"data":[2]}}`, "gzip", "ok", 4194304}, {"wrong", `{"result":{"data":[3]}}`, "identity", "wrong-answer", 4194304}, {"missing", `{"result":{}}`, "identity", "wrong-answer", 4194304}, {"bom", "\xef\xbb\xbf" + `{"result":{"data":[2]}}`, "identity", "ok", 4194304},
		{"bad-json", `{"result":`, "identity", "invalid-json", 4194304}, {"bad-gzip", "bad", "bad-gzip", "invalid-gzip", 4194304}, {"zip-bomb", strings.Repeat("x", 4000), "gzip", "decoded-response-too-large", 256}, {"wire-bound", strings.Repeat("x", 4000), "identity", "response-too-large", 256}, {"encoding", "ok", "br", "unexpected-encoding", 4194304}, {"utf8", string([]byte{'"', 0xff, '"'}), "identity", "invalid-json", 4194304}} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				encoding := tc.encoding
				if encoding == "bad-gzip" {
					encoding = "gzip"
				}
				w.Header().Set("Content-Encoding", encoding)
				if tc.encoding == "gzip" {
					z := gzip.NewWriter(w)
					io.WriteString(z, tc.body)
					z.Close()
				} else {
					io.WriteString(w, tc.body)
				}
			}))
			defer s.Close()
			in := input(s.URL)
			in.Profile.ResponseEncoding = "gzip"
			in.Profile.MaxResponseBytes = tc.cap
			in.ProbePhase = "preflight"
			in.Load = Load{ID: "probe", Model: "closed", Concurrency: 1}
			_, rows := runTest(t, in, context.Background())
			if len(rows) != 1 || rows[0].Outcome != tc.outcome {
				t.Fatalf("want %s: %+v", tc.outcome, rows)
			}
		})
	}
}
func TestNoRedirectOrProxy(t *testing.T) {
	var target atomic.Int32
	end := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { target.Add(1); handler(w, r) }))
	defer end.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Location", end.URL)
		w.WriteHeader(307)
		io.WriteString(w, `{}`)
	}))
	defer s.Close()
	t.Setenv("HTTP_PROXY", end.URL)
	t.Setenv("HTTPS_PROXY", end.URL)
	in := input(s.URL)
	r, rows := runTest(t, in, context.Background())
	if target.Load() != 0 || r.Phases[0].Counts["http-status"] != len(rows) {
		t.Fatal("redirect or proxy used")
	}
}
func TestTLSVerification(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(handler))
	defer s.Close()
	in := input(s.URL)
	_, rows := runTest(t, in, context.Background())
	for _, v := range rows {
		if v.Outcome != "transport-error" {
			t.Fatal(v)
		}
	}
}
func TestTimeoutAndNoRetry(t *testing.T) {
	var count atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer s.Close()
	in := input(s.URL)
	in.Profile.RequestTimeoutMs = 10
	_, rows := runTest(t, in, context.Background())
	if int(count.Load()) != len(rows) {
		t.Fatal("retry observed")
	}
	for _, v := range rows {
		if v.Outcome != "timeout" {
			t.Fatal(v)
		}
	}
}
func TestPeerAbortNoReplay(t *testing.T) {
	var count atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		io.Copy(io.Discard, r.Body)
		c, _, e := w.(http.Hijacker).Hijack()
		if e == nil {
			c.Close()
		}
	}))
	defer s.Close()
	in := input(s.URL)
	_, rows := runTest(t, in, context.Background())
	if count.Load() != 3 || len(rows) != 3 {
		t.Fatal("peer abort replay")
	}
	for _, v := range rows {
		if v.Outcome != "transport-error" {
			t.Fatal(v)
		}
	}
}
func TestReusedConnectionFailureNoReplay(t *testing.T) {
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if requests.Add(1) == 1 {
			handler(w, r)
			return
		}
		c, _, e := w.(http.Hijacker).Hijack()
		if e == nil {
			c.Close()
		}
	}))
	defer s.Close()
	in := input(s.URL)
	in.Load.MaxInFlight = 1
	r, rows := runTest(t, in, context.Background())
	if requests.Load() != 3 || r.Phases[0].Sent != 3 {
		t.Fatal("unexpected request/replay count")
	}
	if rows[0].Outcome != "ok" {
		t.Fatal(rows)
	}
	found := false
	for _, v := range rows[1:] {
		if v.ReusedSocket != nil && *v.ReusedSocket && v.Outcome == "transport-error" {
			found = true
		}
	}
	if !found {
		t.Fatal("did not exercise reused connection failure")
	}
}
func TestCancellationKeepsFullSchedule(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	defer s.Close()
	in := input(s.URL)
	in.Profile.DurationMs = 1000
	in.Load.Rate = 100
	in.Load.MaxInFlight = 1
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(25*time.Millisecond, cancel)
	r, rows := runTest(t, in, ctx)
	if r.Phases[0].Scheduled != 100 || len(rows) != 100 || !r.Phases[0].Cancelled {
		t.Fatal("cancel shrank denominator")
	}
	ids := map[int]bool{}
	for _, v := range rows {
		if ids[v.Sequence] {
			t.Fatal("duplicate")
		}
		ids[v.Sequence] = true
		if v.Sequence > 2 && v.Outcome != "cancelled-before-send" {
			t.Fatal(v)
		}
	}
}
func TestConcurrentAdmissionBound(t *testing.T) {
	var active, peak atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		io.Copy(io.Discard, r.Body)
		select {
		case <-time.After(40 * time.Millisecond):
			handler(w, r)
		case <-r.Context().Done():
		}
	}))
	defer s.Close()
	in := input(s.URL)
	in.Load.Rate = 500
	in.Load.MaxInFlight = 2
	r, rows := runTest(t, in, context.Background())
	if peak.Load() > 2 || len(rows) != 50 || r.Phases[0].Dropped == 0 {
		t.Fatal("unbounded admission", peak.Load(), r.Phases[0])
	}
}
func TestDrainTimeout(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	defer s.Close()
	in := input(s.URL)
	in.Load = Load{ID: "closed", Model: "closed", Concurrency: 1}
	in.Profile.RequestTimeoutMs = 1000
	in.Profile.DrainMs = 10
	_, rows := runTest(t, in, context.Background())
	if len(rows) != 1 || rows[0].Outcome != "drain-timeout" {
		t.Fatal(rows)
	}
}
func TestCreateOnlyAndTraceBudget(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()
	in := input(s.URL)
	p := filepath.Join(t.TempDir(), "trace")
	os.WriteFile(p, []byte("keep"), 0600)
	if _, e := Run(context.Background(), in, p); e == nil {
		t.Fatal("overwrote trace")
	}
	b, _ := os.ReadFile(p)
	if string(b) != "keep" {
		t.Fatal("changed existing file")
	}
	in.RemainingTraceBytes = 1
	r, e := Run(context.Background(), in, p+"new")
	if e == nil || r.TraceWriter.Complete {
		t.Fatal("budget failure accepted")
	}
}
func TestInvalidInput(t *testing.T) {
	for _, edit := range []func(*Input){func(i *Input) { i.Profile.SchemaVersion = 1 }, func(i *Input) { i.Profile.Executor.Engine = "node" }, func(i *Input) { i.Profile.Endpoint = "http://u:p@example.com" }, func(i *Input) { i.Load.Rate = 100000 }, func(i *Input) { i.Case.Checks[0].CanonicalSHA256 = strings.Repeat("a", 64) }, func(i *Input) { i.Case.Checks[0].Pointer = json.RawMessage(`null`) }, func(i *Input) { i.Profile.Executor.GOMAXPROCS = 0 }} {
		in := input("http://127.0.0.1")
		edit(&in)
		if in.Validate() == nil {
			t.Fatal("invalid input accepted")
		}
	}
}

type fakeFile struct {
	mu      sync.Mutex
	data    bytes.Buffer
	block   chan struct{}
	entered chan struct{}
	once    sync.Once
	err     error
	short   bool
	syncErr error
}

func (f *fakeFile) Write(b []byte) (int, error) {
	if f.entered != nil {
		f.once.Do(func() { close(f.entered) })
		<-f.block
	}
	if f.err != nil {
		return 0, f.err
	}
	if f.short {
		return 0, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data.Write(b)
}
func (f *fakeFile) Sync() error  { return f.syncErr }
func (f *fakeFile) Close() error { return nil }
func TestWriterByteBoundIncludesActiveWrite(t *testing.T) {
	f := &fakeFile{block: make(chan struct{}), entered: make(chan struct{})}
	r := Record{Engine: Engine, Phase: "measurement"}
	b, _ := json.Marshal(r)
	w := newWriter(f, 100000, 2*(len(b)+1), nil)
	if e := w.Append(r); e != nil {
		t.Fatal(e)
	}
	<-f.entered
	if e := w.Append(r); e != nil {
		t.Fatal(e)
	}
	if e := w.Append(r); e == nil {
		t.Fatal("inflight write excluded from pending bound")
	}
	close(f.block)
	receipt, e := w.Finish()
	if e == nil || receipt.Complete || receipt.PeakPendingBytes > receipt.MaximumPendingBytes {
		t.Fatal(receipt, e)
	}
}
func TestWriterFailures(t *testing.T) {
	for _, f := range []*fakeFile{{err: errors.New("disk full")}, {short: true}, {syncErr: errors.New("sync failed")}} {
		w := newWriter(f, 10000, 10000, nil)
		w.Append(Record{Engine: Engine})
		r, e := w.Finish()
		if e == nil || r.Complete {
			t.Fatal("disk failure accepted")
		}
	}
}
func TestClockBrackets(t *testing.T) {
	c := newClock()
	a, start := c.boundary()
	time.Sleep(time.Millisecond)
	b, end := c.boundary()
	if b.SinceOriginMs <= a.SinceOriginMs || end.Sub(start) <= 0 {
		t.Fatal("monotonic ordering")
	}
	if a.BeforeNs != nil {
		if *a.BeforeNs > *a.AfterNs || *b.BeforeNs > *b.AfterNs || *b.BeforeNs < *a.BeforeNs {
			t.Fatal("invalid brackets")
		}
	}
}

// Assert net/http retry classification itself: a nonempty body with GetBody=nil is not replayable.
func TestNonReplayableBody(t *testing.T) {
	req, e := http.NewRequest("POST", "http://127.0.0.1", io.NopCloser(strings.NewReader("{}")))
	if e != nil {
		t.Fatal(e)
	}
	req.ContentLength = 2
	if req.GetBody != nil {
		t.Fatal("replayable body")
	}
	if newTransport(1).Proxy != nil || !newTransport(1).DisableCompression {
		t.Fatal("transport policy")
	}
}

type failNextWriteConn struct {
	net.Conn
	fail atomic.Bool
}

func (c *failNextWriteConn) Write(b []byte) (int, error) {
	if c.fail.CompareAndSwap(true, false) {
		return 0, errors.New("injected failure before writing reused POST")
	}
	return c.Conn.Write(b)
}
func TestReusedConnectionFailureBeforeWriteNoReplay(t *testing.T) {
	var requests, dials atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); handler(w, r) }))
	defer s.Close()
	tr := newTransport(1)
	defer tr.CloseIdleConnections()
	var conn *failNextWriteConn
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		conn = &failNextWriteConn{Conn: c}
		return conn, nil
	}
	client := &http.Client{Transport: tr}
	in := input(s.URL)
	expected, _ := expectedChecks(in.Case)
	start := time.Now()
	first := perform(context.Background(), client, in, []byte(`{"gremlin":"1+1"}`), "", start, 0, 0, expected)
	if first.Outcome != "ok" || conn == nil {
		t.Fatal("first request failed")
	}
	conn.fail.Store(true)
	second := perform(context.Background(), client, in, []byte(`{"gremlin":"1+1"}`), "", start, 1, float64(time.Since(start))/1e6, expected)
	if second.Outcome != "transport-error" || second.ReusedSocket == nil || !*second.ReusedSocket || requests.Load() != 1 || dials.Load() != 1 {
		t.Fatalf("nonreplayable POST retried: requests=%d dials=%d record=%+v", requests.Load(), dials.Load(), second)
	}
}

func TestCancelDuringFinalFlushCannotReturnUncancelledSuccess(t *testing.T) {
	f := &fakeFile{block: make(chan struct{}), entered: make(chan struct{})}
	w := newWriter(f, 10000, 10000, nil)
	w.Append(Record{Engine: Engine})
	<-f.entered
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := finishEvidence(ctx, w, []Phase{{Name: "measurement"}}); done <- e }()
	cancel()
	close(f.block)
	if e := <-done; e == nil || !strings.Contains(e.Error(), "cancelled during") {
		t.Fatal("uncancelled success after finalization signal", e)
	}
}

func TestClockProbeBoundedNonceAndIdentityProtocol(t *testing.T) {
	var input, output bytes.Buffer
	for i := 0; i < 5; i++ {
		kind := "sample"
		if i == 4 {
			kind = "finish"
		}
		json.NewEncoder(&input).Encode(map[string]any{"schemaVersion": 1, "kind": kind, "sequence": i, "nonce": strings.Repeat("a", 32)})
	}
	if e := ClockProbe(context.Background(), &input, &output, map[string]any{"binarySha256": strings.Repeat("b", 64)}); e != nil {
		t.Fatal(e)
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	if len(lines) != 6 {
		t.Fatal("missing ready/sample/finish", len(lines))
	}
	var ready map[string]any
	json.Unmarshal(lines[0], &ready)
	if ready["kind"] != "http-go-clock-ready" {
		t.Fatal(ready)
	}
	for _, bad := range []string{strings.Repeat("x", 4097), `{"schemaVersion":1,"kind":"sample","sequence":1,"nonce":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`} {
		var out bytes.Buffer
		if e := ClockProbe(context.Background(), strings.NewReader(bad+"\n"), &out, nil); e == nil {
			t.Fatal("invalid clock exchange accepted")
		}
	}
}

func TestRunObservedHoldsAdmissionAndClosesWriterOnObservationFailure(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1); handler(w, r) }))
	defer server.Close()
	in := input(server.URL)
	failure := errors.New("host attestation rejected")
	path := filepath.Join(t.TempDir(), "trace.ndjson")
	result, e := RunObserved(context.Background(), in, path, func(ctx context.Context, stage string, c ClockIdentity) error {
		if stage != "ready" || count.Load() != 0 {
			t.Fatal("HTTP admission preceded live observation")
		}
		return failure
	})
	if !errors.Is(e, failure) || count.Load() != 0 || len(result.Phases) != 0 || !result.TraceWriter.Complete || result.TraceBytes != 0 {
		t.Fatalf("failed observation did not stop the run: %v %+v", e, result)
	}
	stages := []string{}
	result, e = RunObserved(context.Background(), in, filepath.Join(t.TempDir(), "success.ndjson"), func(ctx context.Context, stage string, c ClockIdentity) error {
		stages = append(stages, stage)
		if stage == "ready" && count.Load() != 0 {
			t.Fatal("premature HTTP request")
		}
		if stage == "finished" && count.Load() != 3 {
			t.Fatal("finished observation preceded request drain")
		}
		return nil
	})
	if e != nil || len(stages) != 2 || stages[0] != "ready" || stages[1] != "finished" || result.ProcessIdentityAfter == nil || result.Phases[0].Counts["ok"] != 3 {
		t.Fatalf("two-sided observation failed: %v %+v", e, result)
	}
}

func TestHighThroughputBudgetBounds(t *testing.T) {
	in := input("http://127.0.0.1:1/gremlin")
	in.Profile.MaxRequests = 1000000
	in.Profile.MaxTraceBytes = 1073741824
	if err := in.Validate(); err != nil {
		t.Fatal(err)
	}
	in.Profile.MaxRequests++
	if in.Validate() == nil {
		t.Fatal("unbounded request count accepted")
	}
	in.Profile.MaxRequests--
	in.Profile.MaxTraceBytes++
	if in.Validate() == nil {
		t.Fatal("unbounded trace budget accepted")
	}
}
