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

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

var sourceSHA256 string // Set by the pinned, offline build command.
var shaPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var aliasPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
var envPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func fmtCountInt(n int) string { return strconv.Itoa(n) }

type options struct {
	root, node, job, prepared, preparedSHA, lease, endpoint, alias, auth, scope, admission, prior, priorSHA, out string
	deadline, timeout                                                                                            float64
	pageSize, maxPages                                                                                           int
	ledgerBudget, summaryBudget, responseBytes                                                                   int64
	identity                                                                                                     bool
}

func parseOptions(args []string) (options, error) {
	var o options
	f := flag.NewFlagSet("hgbench-ldbc-scan-go", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Fprint(f.Output(), "Full read-only SF100 content verifier. Exit 0: selected scope matched; 4: unknown/mismatch.\nRequires an externally attested exclusive lease and stopped/drained writers. No retries, redirects, writes, resume, or performance acceptance.\nCreates a NEW bounded evidence directory. Source, target, schema and counts must all match.\nDefault: vertices gate, then full with the accepted prior proof; both share page/ledger budgets and the original absolute deadline.\nCPU/memory/I/O quotas remain the task controller's responsibility.\nExample: --root PACKAGE --job job.json --prepared prepare-manifest.json --prepared-sha256 SHA --quiescence lease.json --endpoint http://HOST:8080/gremlin --alias ldbc --scope vertices --deadline-unix EPOCH --out NEW\n")
		f.PrintDefaults()
	}
	f.StringVar(&o.root, "root", "", "Installed HG-Bench package root (scripts and fixed context validator)")
	f.StringVar(&o.node, "node", "node", "Node executable for the existing frozen-job/context validator")
	f.StringVar(&o.job, "job", "", "Frozen Loader job in the same target epoch")
	f.StringVar(&o.prepared, "prepared", "", "Full prepared/prepare-manifest.json")
	f.StringVar(&o.preparedSHA, "prepared-sha256", "", "Frozen prepared manifest SHA-256")
	f.StringVar(&o.lease, "quiescence", "", "Immutable external exclusive-access attestation")
	f.StringVar(&o.endpoint, "endpoint", "", "Exact frozen target origin plus /gremlin")
	f.StringVar(&o.alias, "alias", "", "Exact target traversal-source alias")
	f.StringVar(&o.auth, "authorization-env", "", "Environment name containing Authorization; value is never logged")
	f.StringVar(&o.scope, "scope", "", "vertices or full; vertices never qualifies the complete dataset")
	f.StringVar(&o.admission, "vertex-admission", "content", "content (default) or acknowledged-writes (full only)")
	f.StringVar(&o.prior, "prior-scan", "", "Accepted vertices/scan.json for the default full scan")
	f.StringVar(&o.priorSHA, "prior-scan-sha256", "", "Frozen SHA-256 of --prior-scan")
	f.Float64Var(&o.deadline, "deadline-unix", 0, "Original absolute whole-task deadline; never reset")
	f.Float64Var(&o.timeout, "timeout-seconds", 210, "Whole HTTP response timeout, further bounded by remaining task time")
	f.IntVar(&o.pageSize, "page-size", 4096, "Backend page size, 1..100000")
	f.IntVar(&o.maxPages, "max-pages", 600000, "Shared page request cap including schema calls in both scans")
	f.Int64Var(&o.ledgerBudget, "ledger-budget-bytes", 1<<30, "Shared page ledger byte cap across both scans")
	f.Int64Var(&o.summaryBudget, "summary-budget-bytes", 4<<20, "Combined metadata/source ledger/digest cap per scan")
	f.Int64Var(&o.responseBytes, "max-response-bytes", 32<<20, "Independent wire and decoded HTTP byte cap")
	f.StringVar(&o.out, "out", "", "NEW immutable evidence directory; existing paths are refused")
	f.BoolVar(&o.identity, "identity", false, "Print executor/source/build identity without reading data or connecting")
	err := f.Parse(args)
	if err == nil && f.NArg() != 0 {
		err = fmt.Errorf("unexpected positional arguments")
	}
	return o, err
}
func (o options) validate() {
	need(o.root != "" && o.job != "" && o.prepared != "" && o.lease != "" && o.endpoint != "" && o.out != "", "Required path/target flags are missing")
	need(shaPattern.MatchString(o.preparedSHA) && aliasPattern.MatchString(o.alias), "Invalid frozen identity or alias")
	need(!math.IsNaN(o.deadline) && !math.IsInf(o.deadline, 0) && o.deadline > float64(time.Now().UnixNano())/1e9 && o.timeout > 0 && o.timeout <= 3600 && !math.IsNaN(o.timeout), "Invalid absolute deadline or timeout")
	need(o.pageSize >= 1 && o.pageSize <= 100000 && o.maxPages >= 1 && o.maxPages <= 1000000 && o.responseBytes >= 1 && o.responseBytes <= 64<<20 && o.ledgerBudget >= 1 && o.ledgerBudget <= 2<<30 && o.summaryBudget >= 65536 && o.summaryBudget <= 16<<20, "Invalid scan resource budget")
	need((o.prior == "") == (o.priorSHA == "") && (o.priorSHA == "" || shaPattern.MatchString(o.priorSHA)), "Prior proof path/digest must be provided together")
	need((o.scope == "vertices" && o.admission == "content" && o.prior == "") || (o.scope == "full" && ((o.admission == "content" && o.prior != "") || (o.admission == "acknowledged-writes" && o.prior == ""))), "content requires vertices then full/prior; acknowledged-writes requires full without prior")
	need(runtime.GOOS == "linux" || runtime.GOOS == "darwin", "Unsupported file-identity platform")
	need(shaPattern.MatchString(sourceSHA256), "Build is missing the frozen Go source identity")
}
func executorIdentity() object {
	return object{"kind": "hgbench-ldbc-scan-go", "schemaVersion": 1, "sourceSha256": sourceSHA256, "goVersion": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "protocol": protocol}
}

type boundedOutput struct {
	bytes.Buffer
	limit int
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > b.limit {
		return 0, fmt.Errorf("context output budget exceeded")
	}
	return b.Buffer.Write(p)
}
func frozenContext(ctx context.Context, o options) object {
	timeout, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	job, e := filepath.Abs(o.job)
	checked(e, "Cannot resolve job")
	prepared, e := filepath.Abs(o.prepared)
	checked(e, "Cannot resolve manifest")
	cmd := exec.CommandContext(timeout, o.node, filepath.Join(o.root, "tools/ldbc-reconcile-context.mjs"), job, prepared)
	stdout, stderr := &boundedOutput{limit: 32 << 20}, &boundedOutput{limit: 65536}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	checked(cmd.Run(), "Frozen job/context validation failed")
	return obj(decode(stdout.Bytes()))
}
func leaseCheck(lease, context object) (string, string) {
	job := obj(context["job"])
	request := obj(job["request"])
	graph := str(obj(obj(job["config"])["target"])["graph"])
	epoch := str(obj(request["target"])["epoch"])
	marker := "hgb_epoch_" + digest([]byte(epoch))[:32]
	need(shape(lease, "schemaVersion", "kind", "requestDigest", "target", "graphName", "epochMarker", "validUntil", "writersStopped", "inflightWritesDrained", "exclusiveAccess", "attestor", "basis") && intValue(lease["schemaVersion"]) == 1 && lease["kind"] == "sf100-writer-quiescence", "Invalid quiescence contract")
	need(lease["requestDigest"] == context["requestDigest"] && same(lease["target"], request["target"]) && lease["graphName"] == graph && lease["epochMarker"] == marker, "Quiescence target/attempt/epoch differs")
	for _, k := range []string{"writersStopped", "inflightWritesDrained", "exclusiveAccess"} {
		need(lease[k] == true, "All writers must be stopped, drained and excluded")
	}
	for _, k := range []string{"attestor", "basis"} {
		need(len(bytes.TrimSpace([]byte(str(lease[k])))) > 0, "Attestor and external basis required")
	}
	expiry, e := time.Parse(time.RFC3339Nano, str(lease["validUntil"]))
	need(e == nil && expiry.After(time.Now()), "Quiescence lease expired or lacks timezone")
	return graph, marker
}
func guardedError(action func()) (failure object) {
	defer func() {
		if caught := recover(); caught != nil {
			failure = recoverError(caught)
		}
	}()
	action()
	return nil
}
func runScan(ctx context.Context, o options) int {
	o.validate()
	start := time.Now()
	remaining := o.deadline - float64(start.UnixNano())/1e9
	end := start.Add(time.Duration(remaining * 1e9))
	ctx, cancel := context.WithDeadline(ctx, end)
	defer cancel()
	timeGuard := func() {
		if ctx.Err() != nil {
			panic(verificationError{"InterruptedError", "Cancelled or whole-task deadline reached; no retry or resume"})
		}
		need(time.Now().Before(end) && float64(time.Now().UnixNano())/1e9 < o.deadline, "Whole-task absolute deadline reached")
	}
	manifestIdentity := identify(o.prepared)
	prepared, raw := readJSON(o.prepared, 32<<20)
	need(digest(raw) == o.preparedSHA, "Prepared manifest differs from frozen plan")
	base := filepath.Dir(o.prepared)
	expected := loadExpected(base, prepared)
	context := frozenContext(ctx, o)
	root, e := filepath.Abs(base)
	checked(e, "Cannot resolve input root")
	input, e := filepath.Abs(str(obj(context["job"])["inputRoot"]))
	checked(e, "Cannot resolve frozen input root")
	need(root == input, "Input root differs")
	leaseIdentity := identify(o.lease)
	lease, leaseRaw := readJSON(o.lease, 65536)
	graph, marker := leaseCheck(lease, context)
	validateEndpoint(o.endpoint, obj(obj(obj(context["job"])["config"])["target"]))
	self, e := os.Executable()
	checked(e, "Cannot identify executable")
	component := fileFact(self, timeGuard)["sha256"].(string)
	dependencies := object{}
	for _, name := range []string{"tools/ldbc-reconcile.py", "tools/ldbc-inventory.py", "tools/ldbc-reconcile-context.mjs", "adapters/ldbc-gremlin/scan-page.groovy", "adapters/ldbc-gremlin/inventory-page.groovy"} {
		dependencies[name] = fileFact(filepath.Join(o.root, name), timeGuard)["sha256"]
	}
	job := obj(context["job"])
	binding := object{"preparedSha256": o.preparedSHA, "target": obj(job["request"])["target"], "graphName": graph, "epochMarker": marker, "mappingDigest": obj(job["config"])["mappingDigest"], "dependencies": dependencies}
	budgets := object{"maxPages": o.maxPages, "ledgerBytes": o.ledgerBudget, "summaryBytes": o.summaryBudget, "pageSize": o.pageSize, "responseBytes": o.responseBytes, "deadlineUnix": o.deadline, "timeoutSeconds": o.timeout}
	usedPages, usedBytes, priorChallenge := 0, int64(0), ""
	if o.prior != "" {
		usedPages, usedBytes, priorChallenge = priorUsage(o.prior, o.priorSHA, binding, component, budgets, timeGuard)
	}
	transport := newTransport(ctx, o.endpoint, o.auth, o.responseBytes, time.Duration(o.timeout*1e9))
	guard := func() {
		timeGuard()
		need(leaseIdentity.equal(identify(o.lease)), "Exclusive-access lease changed during scan")
		need(manifestIdentity.equal(identify(o.prepared)), "Prepared manifest changed during scan")
		leaseCheck(lease, context)
	}
	guard()
	challenge := make([]byte, 32)
	_, e = rand.Read(challenge)
	checked(e, "Cannot generate challenge")
	commitment := digest(challenge)
	need(commitment != priorChallenge, "Full scan must use a fresh challenge")
	labels := map[string][]string{}
	for _, cat := range []string{"vertices", "edges"} {
		labels[cat] = []string{}
		for label := range obj(obj(expected["counts"])[cat]) {
			labels[cat] = append(labels[cat], label)
		}
	}
	wanted, e := newFP(labels, challenge)
	checked(e, "Invalid expected labels")
	actual, e := newFP(labels, challenge)
	checked(e, "Invalid expected labels")
	checked(os.Mkdir(o.out, 0700), "Evidence directory must be new")
	evidence := openEvidence(o.out, o.maxPages, o.ledgerBudget, o.summaryBudget, usedPages, usedBytes)
	defer evidence.ledger.Close()
	evidence.save("challenge.json", object{"protocol": protocol, "challengeHex": hex.EncodeToString(challenge), "commitmentSha256": commitment, "createdAfterLeaseCheckAt": time.Now().UTC().Format(time.RFC3339Nano), "purpose": "multiset verification, not authentication"})
	evidence.save("quiescence.json", leaseRaw)
	categories := []string{"vertices"}
	if o.scope == "full" {
		categories = append(categories, "edges")
	}
	outcome := "unknown"
	var contentMatch, schemaMatch, countsMatch bool
	var sourceCounts, counts, observed object
	failure := guardedError(func() {
		ledger, e := os.OpenFile(filepath.Join(o.out, "inputs.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		checked(e, "Cannot reserve source ledger")
		defer ledger.Close()
		sourceCounts = sourceFingerprint(prepared, base, categories, wanted, guard, func(row object) {
			raw := append(encoded(row), '\n')
			need(evidence.sizeExcept("pages.jsonl")+int64(len(raw))+65536 <= o.summaryBudget, "Source evidence byte budget exceeded")
			n, e := ledger.Write(raw)
			checked(e, "Cannot write source receipt")
			need(n == len(raw), "Incomplete source receipt")
		})
		checked(ledger.Close(), "Cannot close source ledger")
		evidence.save("expected.json", wanted.snapshot())
		observed = observe(o.root, graph, marker, o.alias, categories, actual, transport, guard, evidence, o.pageSize)
		evidence.save("actual.json", actual.snapshot())
		contentMatch = same(wanted.snapshot(), actual.snapshot())
		schemaMatch = same(expected["schema"], observed["schema"])
		counts = object{}
		for _, cat := range categories {
			group := object{}
			for _, label := range labels[cat] {
				var n uint64
				for k, v := range actual.values {
					if k.category == cat && k.label == label {
						need(^uint64(0)-n >= v.count, "Observed count overflow")
						n += v.count
					}
				}
				group[label] = bytesOf(n)
			}
			counts[cat] = group
		}
		matched := true
		for _, cat := range categories {
			matched = matched && same(counts[cat], obj(expected["counts"])[cat])
		}
		countsMatch = matched
		guard()
		need(fileFact(self, timeGuard)["sha256"] == component, "Verifier executable changed during scan")
		for name, sha := range dependencies {
			need(fileFact(filepath.Join(o.root, name), timeGuard)["sha256"] == sha, "Verification implementation changed during scan")
		}
		outcome = "mismatch"
		if contentMatch == true && schemaMatch == true && countsMatch == true {
			outcome = "matched"
		}
	})
	checked(evidence.ledger.Close(), "Cannot close page ledger")
	facts := []object{}
	if finalError := guardedError(func() {
		for _, name := range []string{"expected.json", "actual.json", "inputs.jsonl", "pages.jsonl", "quiescence.json", "challenge.json"} {
			p := filepath.Join(o.out, name)
			if _, e := os.Lstat(p); e == nil {
				facts = append(facts, fileFact(p, timeGuard))
			}
		}
		timeGuard()
	}); finalError != nil {
		outcome = "unknown"
		failure = finalError
	}
	var pages, hidden, prior any
	if observed != nil {
		pages = observed["pages"]
		hidden = observed["hiddenRecords"]
	}
	if o.prior != "" {
		prior = object{"sha256": o.priorSHA}
	}
	report := object{"schemaVersion": 1, "kind": "sf100-sequential-content-observation", "protocol": protocol, "scope": o.scope, "vertexAdmission": o.admission, "outcome": outcome, "contentMatches": contentMatch, "schemaMatches": schemaMatch, "countsMatch": countsMatch, "binding": binding, "anchorRequestDigest": context["requestDigest"], "componentSha256": component, "executor": executorIdentity(), "challengeCommitment": commitment, "sourceCounts": sourceCounts, "counts": counts, "pages": pages, "hiddenRecords": hidden, "requests": evidence.requests, "ledgerBytes": evidence.ledgerBytes, "wholeTaskPages": usedPages + evidence.requests, "wholeTaskLedgerBytes": usedBytes + evidence.ledgerBytes, "priorScan": prior, "budgets": budgets, "error": failure, "evidence": facts, "datasetVerified": o.scope == "full" && outcome == "matched", "performanceAccepted": false, "trust": "External lease must prove exclusive access and drained writers; digest equality assumes no collision"}
	evidence.save("scan.json", report)
	if o.scope == "full" && observed != nil {
		scanRaw, e := os.ReadFile(filepath.Join(o.out, "scan.json"))
		checked(e, "Cannot read completed report")
		evidence.save("inventory.json", object{"schemaVersion": 1, "kind": "sf100-inventory-observation", "outcome": outcome, "matches": outcome == "matched", "schemaMatch": schemaMatch, "countsMatch": countsMatch, "contentMatches": contentMatch, "method": protocol, "scanSha256": digest(scanRaw), "actual": object{"schemaVersion": 1, "scope": expected["scope"], "schema": observed["schema"], "counts": counts}})
	}
	checked(json.NewEncoder(os.Stdout).Encode(object{"report": filepath.Join(o.out, "scan.json"), "outcome": outcome, "datasetVerified": report["datasetVerified"], "requests": evidence.requests, "wholeTaskPages": report["wholeTaskPages"], "error": failure}), "Cannot emit summary")
	if outcome == "matched" {
		return 0
	}
	return 4
}
func mainExit() (code int) {
	defer func() {
		if caught := recover(); caught != nil {
			_ = json.NewEncoder(os.Stderr).Encode(object{"error": recoverError(caught), "datasetVerified": false, "performanceAccepted": false})
			code = 4
		}
	}()
	o, e := parseOptions(os.Args[1:])
	if e == flag.ErrHelp {
		return 0
	}
	if e != nil {
		return 2
	}
	if o.identity {
		_ = json.NewEncoder(os.Stdout).Encode(executorIdentity())
		return 0
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return runScan(ctx, o)
}
func main() { os.Exit(mainExit()) }
