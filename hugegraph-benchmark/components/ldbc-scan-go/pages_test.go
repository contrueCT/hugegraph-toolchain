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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeRocksDBPages(t *testing.T) {
	raw, e := os.ReadFile("testdata/native-rocksdb-pages.jsonl")
	if e != nil {
		t.Fatal(e)
	}
	need(digest(raw) == "37e36a0c921725c3ba7d5b5362cf1136ac768241d67d488f98825233dade0037", "Native page fixture changed")
	labels := map[string][]string{"vertices": {"person"}, "edges": {"knows"}}
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}
	expected, _ := newFP(labels, seed)
	actual, _ := newFP(labels, seed)
	a, b := "person:9007199254740993>甲", "person:2"
	for _, v := range []object{{"id": a, "label": "person", "properties": object{"ldbcId": json.Number("9007199254740993"), "firstName": " \t甲\n ", "emails": []any{"a", "b"}}}, {"id": b, "label": "person", "properties": object{"ldbcId": json.Number("2"), "firstName": "B", "emails": []any{"c"}}}} {
		_, e := expected.add("vertices", v, false)
		if e != nil {
			t.Fatal(e)
		}
	}
	for _, key := range []string{"0", "-1", "9007199254740993", "9223372036854775807"} {
		_, e := expected.add("edges", object{"sourceId": a, "targetId": b, "label": "knows", "properties": object{"edgeKey": json.Number(key), "stamp": "甲>" + key}}, false)
		if e != nil {
			t.Fatal(e)
		}
	}
	_, e = expected.add("edges", object{"sourceId": b, "targetId": a, "label": "knows", "properties": object{"edgeKey": json.Number("7"), "stamp": "reverse"}}, false)
	if e != nil {
		t.Fatal(e)
	}
	for _, category := range []string{"vertices", "edges"} {
		fold := newFold(category, "scan_page_check", "epoch_marker", actual, 1, 12)
		for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
			page := obj(decode(line))
			if page["category"] == category {
				fold.accept(*fold.next, page)
			}
		}
		if !fold.complete() {
			t.Fatal("actual native pages did not terminate")
		}
	}
	if !same(expected.snapshot(), actual.snapshot()) {
		t.Fatal("native pages differ from independent logical records")
	}
}
func TestPageIdentityCountsCyclesAndFailurePoisoning(t *testing.T) {
	for _, kind := range []string{"graph", "epoch", "category", "page", "shape", "count", "decimal", "hidden", "cursor", "label", "property", "type"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			fp, _ := newFP(map[string][]string{"vertices": {"person"}, "edges": {"knows"}}, make([]byte, 32))
			fold := newFold("vertices", "unit", str(f.lease["epochMarker"]), fp, 1, 4)
			page := parsed(object{"graphName": "unit", "epochMarker": f.lease["epochMarker"], "kind": "scan-page", "category": "vertices", "page": "", "nextPage": nil, "scanned": "1", "hiddenRecords": "0", "rows": []object{f.vertex}})
			switch kind {
			case "graph":
				page["graphName"] = "wrong"
			case "epoch":
				page["epochMarker"] = "wrong"
			case "category":
				page["category"] = "edges"
			case "page":
				page["page"] = "wrong"
			case "shape":
				page["extra"] = true
			case "count":
				page["scanned"] = "2"
			case "decimal":
				page["scanned"] = "01"
			case "hidden":
				page["hiddenRecords"] = "2"
			case "cursor":
				page["nextPage"] = ""
			case "label":
				obj(array(page["rows"])[0])["label"] = "unknown"
			case "property":
				obj(obj(array(page["rows"])[0])["properties"])["extra"] = false
			case "type":
				obj(obj(array(page["rows"])[0])["properties"])["ldbcId"] = json.Number("1.0")
			}
			panics(t, func() { fold.accept("", page) })
			if !fold.failed || fold.complete() {
				t.Fatal("failed page was reusable")
			}
			panics(t, func() { fold.accept("", page) })
		})
	}
	t.Run("valid-hidden-and-cycle", func(t *testing.T) {
		f := newFixture(t)
		fp, _ := newFP(map[string][]string{"vertices": {"person"}, "edges": {"knows"}}, make([]byte, 32))
		fold := newFold("vertices", "unit", str(f.lease["epochMarker"]), fp, 1, 4)
		page := parsed(object{"graphName": "unit", "epochMarker": f.lease["epochMarker"], "kind": "scan-page", "category": "vertices", "page": "", "nextPage": "next", "scanned": "1", "hiddenRecords": "1", "rows": []any{}})
		fold.accept("", page)
		page["page"] = "next"
		page["nextPage"] = "next"
		panics(t, func() { fold.accept("next", page) })
	})
}
func TestHTTPErrorsAreBoundedAndRedacted(t *testing.T) {
	secret := "must-never-appear-in-error-evidence"
	raw := encoded(object{"status": object{"code": 500}, "exception": "org.example.TimeoutException", "message": secret})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(500)
		_, _ = w.Write(compress(raw))
	}))
	defer server.Close()
	failure := guardedError(func() { newTransport(context.Background(), server.URL, "", 65536, time.Second).post(object{}) })
	if failure == nil || strings.Contains(string(encoded(failure)), secret) {
		t.Fatal("HTTP error leaked body")
	}
	details := obj(failure["http"])
	if details["providerException"] != "TimeoutException" || details["status"] != 500 || details["bodyState"] != "complete-json" || details["wirePrefixSha256"] != digest(compress(raw)) {
		t.Fatal(failure)
	}
}
func TestDeadlineAndManifestMutationKeepUnknown(t *testing.T) {
	for _, kind := range []string{"deadline", "manifest", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.mutate = func(object, int) {
				switch kind {
				case "deadline":
					time.Sleep(3 * time.Second)
				case "manifest":
					p := f.options.prepared
					raw, _ := os.ReadFile(p)
					put(t, p, append(raw, ' '))
				case "cancel":
					cancel()
				}
			}
			o := f.options
			o.out = filepath.Join(f.base, "run")
			o.timeout = 5
			if kind == "deadline" {
				o.deadline = float64(time.Now().Add(2*time.Second).UnixNano()) / 1e9
			}
			code := runScan(ctx, o)
			report, _ := readJSON(filepath.Join(o.out, "scan.json"), 4<<20)
			if code != 4 || report["outcome"] != "unknown" || report["datasetVerified"] != false || f.calls > 1 {
				t.Fatal(report)
			}
		})
	}
}

func TestDeclaredEmptyLabelMatchesPythonInventory(t *testing.T) {
	f := newFixture(t)
	obj(obj(f.expected["schema"])["vertexLabels"])["empty"] = object{}
	obj(obj(f.expected["counts"])["vertices"])["empty"] = "0"
	raw := encoded(f.expected)
	put(t, filepath.Join(f.base, "inventory.expected.json"), raw)
	obj(f.prepared["files"])["inventory.expected.json"] = object{"bytes": len(raw), "sha256": digest(raw)}
	raw = encoded(f.prepared)
	put(t, f.options.prepared, raw)
	f.options.preparedSHA = digest(raw)
	code, report := f.run(t, "empty-label", nil)
	if code != 0 || obj(obj(report["counts"])["vertices"])["empty"] != "0" {
		t.Fatal(report)
	}
}
