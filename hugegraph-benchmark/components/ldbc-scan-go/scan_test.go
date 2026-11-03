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
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func panics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected refusal")
		}
	}()
	fn()
}
func parsed(v any) object { return obj(decode(encoded(v))) }
func put(t *testing.T, path string, raw []byte) {
	t.Helper()
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
}
func compress(raw []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, _ = w.Write(raw)
	_ = w.Close()
	return b.Bytes()
}
func TestProtocolVectors(t *testing.T) {
	raw, e := os.ReadFile("testdata/protocol-v1.json")
	if e != nil {
		t.Fatal(e)
	}
	v := obj(decode(raw))
	labels := map[string][]string{}
	for cat, a := range obj(v["labels"]) {
		for _, label := range array(a) {
			labels[cat] = append(labels[cat], str(label))
		}
	}
	challenge, e := hex.DecodeString(str(v["challengeHex"]))
	if e != nil {
		t.Fatal(e)
	}
	aggregate, e := newFP(labels, challenge)
	if e != nil {
		t.Fatal(e)
	}
	valid := 0
	for _, entry := range array(v["cases"]) {
		c := obj(entry)
		t.Run(str(c["id"]), func(t *testing.T) {
			raw, e := base64.StdEncoding.DecodeString(str(c["rawBase64"]))
			if e != nil {
				t.Fatal(e)
			}
			value, e := strict(raw)
			if e == nil && c["kind"] == "source" {
				value, e = fromSource(str(c["category"]), str(c["label"]), value)
			}
			var trace *trace
			if e == nil {
				trace, e = aggregate.add(str(c["category"]), value, true)
			}
			if (e == nil) != (c["valid"] == true) {
				t.Fatalf("acceptance differs: %v", e)
			}
			if e == nil {
				valid++
				if !same(trace, c["expected"]) {
					t.Fatal("canonical bytes, bucket, or HMAC differs")
				}
			}
		})
	}
	if len(array(v["cases"])) != 305 || valid != 270 || !same(aggregate.snapshot(), v["expectedAggregate"]) {
		t.Fatal("frozen aggregate or vector coverage differs")
	}
}

type fixture struct {
	options                            options
	base                               string
	prepared, expected, context, lease object
	vertex, edge                       object
	calls                              int
	mutate                             func(object, int)
	encoding                           string
	status                             int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{base: t.TempDir(), encoding: "identity", status: 200}
	root, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	f.vertex = parsed(object{"id": "person:9007199254740993>甲", "label": "person", "properties": object{"ldbcId": json.Number("9007199254740993"), "firstName": " \t甲\n ", "emails": []string{"b", "a"}}})
	f.edge = parsed(object{"sourceId": f.vertex["id"], "targetId": "person:2", "label": "knows", "properties": object{"edgeKey": json.Number("9223372036854775807"), "stamp": "甲>"}})
	f.expected = parsed(object{"schemaVersion": 1, "scope": "public-ldbc-initial-graph", "schema": object{"propertyKeys": object{}, "vertexLabels": object{"person": object{}}, "edgeLabels": object{"knows": object{}}, "indexLabels": object{}}, "counts": object{"vertices": object{"person": "1"}, "edges": object{"knows": "1"}}})
	prepared := object{"schemaVersion": 2, "vertices": 1, "edges": 1, "counts": object{"person": 1, "knows": 1}, "files": object{}, "shards": []object{}}
	shards := []object{}
	for _, cat := range []string{"vertices", "edges"} {
		row, label := object{}, "person"
		if cat == "vertices" {
			row["vertexId"] = f.vertex["id"]
			for k, v := range obj(f.vertex["properties"]) {
				row[k] = v
			}
		} else {
			label = "knows"
			row["sourceId"] = f.edge["sourceId"]
			row["targetId"] = f.edge["targetId"]
			for k, v := range obj(f.edge["properties"]) {
				row[k] = v
			}
		}
		raw := append(encoded(row), '\n')
		stored := compress(raw)
		name := cat + ".jsonl.gz"
		put(t, filepath.Join(f.base, name), stored)
		obj(prepared["files"])[name] = object{"bytes": bytesOf(uint64(len(stored))), "sha256": digest(stored)}
		shards = append(shards, object{"id": cat + "-0", "category": cat, "label": label, "path": name, "bytes": bytesOf(uint64(len(stored))), "sha256": digest(stored), "records": "1", "encoding": object{"compression": "gzip", "uncompressedBytes": bytesOf(uint64(len(raw))), "uncompressedSha256": digest(raw)}})
	}
	prepared["shards"] = shards
	raw := encoded(f.expected)
	put(t, filepath.Join(f.base, "inventory.expected.json"), raw)
	obj(prepared["files"])["inventory.expected.json"] = object{"bytes": len(raw), "sha256": digest(raw)}
	f.prepared = parsed(prepared)
	put(t, filepath.Join(f.base, "prepare-manifest.json"), encoded(prepared))
	epoch := "unit-epoch"
	marker := "hgb_epoch_" + digest([]byte(epoch))[:32]
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls++
		raw, _ := io.ReadAll(r.Body)
		b := obj(obj(decode(raw))["bindings"])
		cat := str(b["category"])
		row := object{"graphName": "unit", "epochMarker": marker, "kind": "schema", "schema": f.expected["schema"]}
		if cat != "schema" {
			element := f.vertex
			if cat == "edges" {
				element = f.edge
			}
			row = object{"graphName": "unit", "epochMarker": marker, "kind": "scan-page", "category": cat, "page": b["page"], "nextPage": nil, "scanned": "1", "hiddenRecords": "0", "rows": []object{element}}
		}
		if f.mutate != nil {
			f.mutate(row, f.calls)
		}
		raw = encoded(object{"status": object{"code": 200}, "result": object{"data": []object{row}}})
		if f.encoding == "gzip" {
			raw = compress(raw)
			w.Header().Set("Content-Encoding", "gzip")
		}
		w.WriteHeader(f.status)
		_, _ = w.Write(raw)
	}))
	t.Cleanup(server.Close)
	u, _ := url.Parse(server.URL)
	port, _ := strconv.Atoi(u.Port())
	f.context = parsed(object{"requestDigest": strings.Repeat("b", 64), "job": object{"request": object{"target": object{"targetDigest": strings.Repeat("a", 64), "epoch": epoch}}, "inputRoot": f.base, "config": object{"mappingDigest": strings.Repeat("c", 64), "target": object{"graph": "unit", "host": "http://127.0.0.1", "port": port}}}})
	f.lease = parsed(object{"schemaVersion": 1, "kind": "sf100-writer-quiescence", "requestDigest": f.context["requestDigest"], "target": obj(obj(f.context["job"])["request"])["target"], "graphName": "unit", "epochMarker": marker, "validUntil": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), "writersStopped": true, "inflightWritesDrained": true, "exclusiveAccess": true, "attestor": "unit", "basis": "fixed-response component fixture; no actual database"})
	put(t, filepath.Join(f.base, "lease.json"), encoded(f.lease))
	put(t, filepath.Join(f.base, "context.json"), encoded(f.context))
	node := filepath.Join(f.base, "node-fixture")
	put(t, node, []byte("#!/bin/sh\nexec /bin/cat \"$HGBENCH_TEST_CONTEXT\"\n"))
	_ = os.Chmod(node, 0700)
	t.Setenv("HGBENCH_TEST_CONTEXT", filepath.Join(f.base, "context.json"))
	sourceSHA256 = strings.Repeat("d", 64)
	f.options = options{root: root, node: node, job: filepath.Join(f.base, "job.json"), prepared: filepath.Join(f.base, "prepare-manifest.json"), preparedSHA: digest(encoded(prepared)), lease: filepath.Join(f.base, "lease.json"), endpoint: server.URL + "/gremlin", alias: "g", scope: "vertices", admission: "content", deadline: float64(time.Now().Add(time.Minute).UnixNano()) / 1e9, timeout: 2, pageSize: 4096, maxPages: 20, ledgerBudget: 1 << 20, summaryBudget: 4 << 20, responseBytes: 1 << 20}
	return f
}
func (f *fixture) run(t *testing.T, name string, change func(*options)) (int, object) {
	t.Helper()
	o := f.options
	o.out = filepath.Join(f.base, name)
	if change != nil {
		change(&o)
	}
	code := runScan(context.Background(), o)
	report, _ := readJSON(filepath.Join(o.out, "scan.json"), 4<<20)
	return code, report
}

func TestSourceAndCompleteScopes(t *testing.T) {
	f := newFixture(t)
	f.encoding = "gzip"
	code, v := f.run(t, "vertices", nil)
	if code != 0 || v["datasetVerified"] != false || intValue(v["requests"]) != 3 {
		t.Fatal(v)
	}
	prior := filepath.Join(f.base, "vertices/scan.json")
	raw, _ := os.ReadFile(prior)
	code, full := f.run(t, "full", func(o *options) { o.scope = "full"; o.prior = prior; o.priorSHA = digest(raw) })
	if code != 0 || full["datasetVerified"] != true || intValue(full["requests"]) != 4 || intValue(full["wholeTaskPages"]) != 7 || full["challengeCommitment"] == v["challengeCommitment"] || full["performanceAccepted"] != false {
		t.Fatal(full)
	}
	if _, e := os.Stat(filepath.Join(f.base, "full/inventory.json")); e != nil {
		t.Fatal(e)
	}
}
func TestMismatchAndFailedPages(t *testing.T) {
	for _, kind := range []string{"content", "schema", "epoch", "cursor", "count", "logical", "http", "lease"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			f.mutate = func(row object, n int) {
				if n != 2 {
					return
				}
				switch kind {
				case "content":
					obj(obj(array(parsed(row)["rows"])[0])["properties"])["firstName"] = "changed"
					obj(f.vertex["properties"])["firstName"] = "changed"
				case "schema":
					obj(f.expected["schema"])["indexLabels"] = object{"unexpected": object{}}
				case "epoch":
					row["epochMarker"] = "wrong"
				case "cursor":
					row["nextPage"] = ""
				case "count":
					row["scanned"] = "2"
				case "logical":
					obj(f.vertex["properties"])["ldbcId"] = true
				case "http":
					f.status = 500
				case "lease":
					put(t, filepath.Join(f.base, "lease.json"), append(encoded(f.lease), '\n'))
				}
			}
			code, r := f.run(t, "run", nil)
			if code != 4 || r["datasetVerified"] != false || r["performanceAccepted"] != false || r["outcome"] == "matched" {
				t.Fatal(r)
			}
			if kind != "content" && r["outcome"] != "unknown" {
				t.Fatal("invalid page/lease must be unknown")
			}
		})
	}
}
func TestSourceCorruptionAndFileIdentity(t *testing.T) {
	for _, kind := range []string{"stored-sha", "expanded-sha", "gzip-crc", "records", "unterminated", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			s := obj(array(f.prepared["shards"])[0])
			p := filepath.Join(f.base, str(s["path"]))
			switch kind {
			case "stored-sha":
				s["sha256"] = strings.Repeat("0", 64)
				obj(obj(f.prepared["files"])[s["path"].(string)])["sha256"] = s["sha256"]
			case "expanded-sha":
				obj(s["encoding"])["uncompressedSha256"] = strings.Repeat("0", 64)
			case "gzip-crc":
				raw, _ := os.ReadFile(p)
				raw[len(raw)-8] ^= 1
				put(t, p, raw)
				s["sha256"] = digest(raw)
				obj(obj(f.prepared["files"])[s["path"].(string)])["sha256"] = s["sha256"]
			case "records":
				s["records"] = "2"
			case "unterminated":
				raw := []byte(`{"vertexId":"person:1","ldbcId":1}`)
				stored := compress(raw)
				put(t, p, stored)
				s["sha256"] = digest(stored)
				s["bytes"] = bytesOf(uint64(len(stored)))
				obj(f.prepared["files"])[s["path"].(string)] = object{"sha256": s["sha256"], "bytes": s["bytes"]}
				s["encoding"] = parsed(object{"compression": "gzip", "uncompressedBytes": bytesOf(uint64(len(raw))), "uncompressedSha256": digest(raw)})
			case "symlink":
				raw, _ := os.ReadFile(p)
				_ = os.Remove(p)
				other := p + ".original"
				put(t, other, raw)
				_ = os.Symlink(other, p)
			}
			fp, _ := newFP(map[string][]string{"vertices": {"person"}, "edges": {"knows"}}, make([]byte, 32))
			panics(t, func() { sourceFingerprint(f.prepared, f.base, []string{"vertices"}, fp, func() {}, func(object) {}) })
		})
	}
	t.Run("atime-is-not-mutation", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "input")
		put(t, p, []byte("a"))
		before := identify(p)
		_, _ = os.ReadFile(p)
		if !before.equal(identify(p)) {
			t.Fatal("read changed identity")
		}
		put(t, p, []byte("b"))
		_ = os.Chtimes(p, before.info.ModTime(), before.info.ModTime())
		if before.equal(identify(p)) {
			t.Fatal("ctime mutation was missed")
		}
	})
}
func TestBudgetsPriorProofAndNoResume(t *testing.T) {
	for _, kind := range []string{"pages", "ledger", "summary"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			if kind == "summary" {
				o := f.options
				o.out = filepath.Join(f.base, "budget")
				o.summaryBudget = 65536
				panics(t, func() { runScan(context.Background(), o) })
				if f.calls != 0 {
					t.Fatal("metadata budget failure reached target")
				}
				return
			}
			code, r := f.run(t, "budget", func(o *options) {
				switch kind {
				case "pages":
					o.maxPages = 2
				case "ledger":
					o.ledgerBudget = 1
				case "summary":
					o.summaryBudget = 65536
				}
			})
			if code != 4 || r["outcome"] != "unknown" || r["datasetVerified"] != false {
				t.Fatal(r)
			}
			if f.calls > 2 {
				t.Fatal("ran beyond budget")
			}
			panics(t, func() { f.run(t, "budget", nil) })
		})
	}
	for _, kind := range []string{"component", "budget", "evidence", "chain", "proof-sha"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			_, r := f.run(t, "v", nil)
			prior := filepath.Join(f.base, "v/scan.json")
			switch kind {
			case "component":
				r["componentSha256"] = strings.Repeat("e", 64)
			case "budget":
				obj(r["budgets"])["maxPages"] = json.Number("19")
			case "evidence":
				p := filepath.Join(f.base, "v/expected.json")
				raw, _ := os.ReadFile(p)
				put(t, p, append(raw, ' '))
			case "chain":
				p := filepath.Join(f.base, "v/pages.jsonl")
				raw, _ := os.ReadFile(p)
				put(t, p, bytes.Replace(raw, []byte(`"sequence":1`), []byte(`"sequence":2`), 1))
			}
			put(t, prior, append(encoded(r), '\n'))
			raw, _ := os.ReadFile(prior)
			o := f.options
			o.out = filepath.Join(f.base, "full")
			o.scope = "full"
			o.prior = prior
			o.priorSHA = digest(raw)
			if kind == "proof-sha" {
				o.priorSHA = strings.Repeat("0", 64)
			}
			before := f.calls
			panics(t, func() { runScan(context.Background(), o) })
			if f.calls != before {
				t.Fatal("invalid prior proof reached target")
			}
		})
	}
}
func TestHTTPBoundsRedirectDeadlineAndExactStatus(t *testing.T) {
	for _, kind := range []string{"redirect", "wire", "decoded", "encoding", "gzip-crc", "duplicate", "status-float", "slow", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				_, _ = io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()
				switch kind {
				case "redirect":
					w.Header().Set("Location", "/other")
					w.WriteHeader(307)
				case "wire":
					_, _ = w.Write(bytes.Repeat([]byte("x"), 257))
				case "decoded":
					w.Header().Set("Content-Encoding", "gzip")
					_, _ = w.Write(compress(bytes.Repeat([]byte("x"), 257)))
				case "encoding":
					w.Header().Set("Content-Encoding", "br")
				case "gzip-crc":
					raw := compress([]byte(`{"status":{"code":200},"result":{"data":[]}}`))
					raw[len(raw)-8] ^= 1
					w.Header().Set("Content-Encoding", "gzip")
					_, _ = w.Write(raw)
				case "duplicate":
					_, _ = w.Write([]byte(`{"status":{"code":200,"code":200},"result":{"data":[]}}`))
				case "status-float":
					_, _ = w.Write([]byte(`{"status":{"code":200.0},"result":{"data":[]}}`))
				case "slow", "cancel":
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
					}
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "cancel" {
				time.AfterFunc(10*time.Millisecond, cancel)
			}
			tr := newTransport(ctx, server.URL, "", 256, 40*time.Millisecond)
			started := time.Now()
			panics(t, func() { tr.post(object{}) })
			if calls != 1 || time.Since(started) > time.Second {
				t.Fatal("redirect/retry/deadline escaped")
			}
		})
	}
}
func TestLeaseAndEndpointContracts(t *testing.T) {
	for _, kind := range []string{"writers", "drained", "exclusive", "expired", "timezone", "attempt", "epoch", "extra"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			l := parsed(f.lease)
			switch kind {
			case "writers":
				l["writersStopped"] = false
			case "drained":
				l["inflightWritesDrained"] = false
			case "exclusive":
				l["exclusiveAccess"] = false
			case "expired":
				l["validUntil"] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
			case "timezone":
				l["validUntil"] = "2099-01-01T00:00:00"
			case "attempt":
				l["requestDigest"] = "other"
			case "epoch":
				l["epochMarker"] = "other"
			case "extra":
				l["unexpected"] = true
			}
			panics(t, func() { leaseCheck(l, f.context) })
		})
	}
	f := newFixture(t)
	target := obj(obj(obj(f.context["job"])["config"])["target"])
	for _, endpoint := range []string{f.options.endpoint + "?x=1", strings.Replace(f.options.endpoint, "http://", "http://user:secret@", 1), f.options.endpoint + "#fragment", strings.Replace(f.options.endpoint, "/gremlin", "/%67remlin", 1)} {
		panics(t, func() { validateEndpoint(endpoint, target) })
	}
}
