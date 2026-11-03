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
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type transport struct {
	endpoint, authorization string
	maxBytes                int64
	timeout                 time.Duration
	client                  *http.Client
	ctx                     context.Context
}

func newTransport(ctx context.Context, endpoint, auth string, maxBytes int64, timeout time.Duration) *transport {
	authorization := ""
	if auth != "" {
		need(envPattern.MatchString(auth), "Invalid authorization environment name")
		authorization = os.Getenv(auth)
		need(authorization != "" && !strings.ContainsAny(authorization, "\r\n"), "Invalid authorization value")
	}
	return &transport{endpoint, authorization, maxBytes, timeout, &http.Client{Transport: &http.Transport{Proxy: nil, DisableCompression: true, DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, ctx}
}
func limitedBody(reader io.Reader, max int64) []byte {
	raw, e := io.ReadAll(io.LimitReader(reader, max+1))
	checked(e, "Incomplete HTTP response")
	need(int64(len(raw)) <= max, "Response byte budget exceeded")
	return raw
}
func decodedBody(raw []byte, encoding string, max int64) []byte {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return raw
	case "gzip":
		gz, e := gzip.NewReader(bytes.NewReader(raw))
		checked(e, "Invalid gzip response")
		defer gz.Close()
		return limitedBody(gz, max)
	default:
		panic(verificationError{"ValueError", "Unsupported response encoding"})
	}
}
func (t *transport) post(body object) ([]any, string) {
	ctx, cancel := context.WithTimeout(t.ctx, t.timeout)
	defer cancel()
	request, e := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(encoded(body)))
	checked(e, "Cannot construct HTTP request")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "gzip")
	if t.authorization != "" {
		request.Header.Set("Authorization", t.authorization)
	}
	response, e := t.client.Do(request)
	checked(e, "HTTP request failed or reached its deadline")
	defer response.Body.Close()
	if response.StatusCode != 200 {
		panic(safeHTTPFailure(response, t.maxBytes))
	}
	raw := limitedBody(response.Body, t.maxBytes)
	decoded := decodedBody(raw, response.Header.Get("Content-Encoding"), t.maxBytes)
	need(ctx.Err() == nil, "Whole response deadline reached")
	result := obj(decode(decoded))
	status := obj(result["status"])
	need(intValue(status["code"]) == 200, "Invalid Gremlin response status")
	data := array(obj(result["result"])["data"])
	need(ctx.Err() == nil, "Whole response deadline reached during decoding")
	return data, digest(raw)
}

type pageFold struct {
	category, graph, epoch       string
	fp                           *fingerprint
	pageSize, maxPages, requests int
	next                         *string
	seen                         map[string]bool
	failed                       bool
}

func newFold(category, graph, epoch string, fp *fingerprint, pageSize, maxPages int) *pageFold {
	need((category == "vertices" || category == "edges") && graph != "" && epoch != "" && pageSize >= 1 && pageSize <= 100000 && maxPages >= 1 && maxPages <= 1000000, "Invalid scan budget")
	start := ""
	return &pageFold{category: category, graph: graph, epoch: epoch, fp: fp, pageSize: pageSize, maxPages: maxPages, next: &start, seen: map[string]bool{}}
}
func (f *pageFold) accept(requested string, response object) {
	need(!f.failed, "Failed scan cannot continue")
	success := false
	defer func() {
		if !success {
			f.failed = true
		}
	}()
	need(f.next != nil && *f.next == requested && len(requested) <= 32768, "Unexpected or oversized request cursor")
	need(f.requests < f.maxPages, "Page budget exhausted")
	marker := digest([]byte(requested))
	need(!f.seen[marker], "Repeated page request")
	f.seen[marker] = true
	need(shape(response, "graphName", "epochMarker", "kind", "category", "page", "nextPage", "scanned", "hiddenRecords", "rows"), "Unexpected page response shape")
	need(response["graphName"] == f.graph && response["epochMarker"] == f.epoch && response["category"] == f.category && response["kind"] == "scan-page" && response["page"] == requested, "Wrong graph/epoch/page binding")
	rows := array(response["rows"])
	scanned, hidden := decimal(response["scanned"]), decimal(response["hiddenRecords"])
	need(hidden <= scanned && scanned <= uint64(f.pageSize) && uint64(len(rows)) == scanned-hidden, "Page record count mismatch")
	var next *string
	if response["nextPage"] != nil {
		n := str(response["nextPage"])
		need(len(n) <= 32768 && !f.seen[digest([]byte(n))], "Repeated or oversized next cursor")
		next = &n
	}
	for _, row := range rows {
		_, e := f.fp.add(f.category, row, false)
		checked(e, "Invalid target logical record")
	}
	f.requests++
	f.next = next
	success = true
}
func (f *pageFold) complete() bool { return !f.failed && f.requests > 0 && f.next == nil }

type evidence struct {
	out                                                 string
	maxPages, usedPages, requests                       int
	ledgerBudget, summaryBudget, usedBytes, ledgerBytes int64
	previous                                            string
	ledger                                              *os.File
}

func openEvidence(out string, maxPages int, ledgerBudget, summaryBudget int64, usedPages int, usedBytes int64) *evidence {
	e := &evidence{out: out, maxPages: maxPages, ledgerBudget: ledgerBudget, summaryBudget: summaryBudget, usedPages: usedPages, usedBytes: usedBytes, previous: strings.Repeat("0", 64)}
	var err error
	e.ledger, err = os.OpenFile(filepath.Join(out, "pages.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	checked(err, "Cannot reserve page evidence")
	return e
}
func (e *evidence) sizeExcept(names ...string) int64 {
	entries, err := os.ReadDir(e.out)
	checked(err, "Cannot inspect evidence budget")
	var n int64
	for _, entry := range entries {
		skip := false
		for _, name := range names {
			if entry.Name() == name {
				skip = true
			}
		}
		if skip {
			continue
		}
		info, err := entry.Info()
		checked(err, "Cannot inspect evidence file")
		if info.Mode().IsRegular() {
			n += info.Size()
		}
	}
	return n
}
func writeNew(path string, raw []byte) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	checked(e, "Evidence already exists or cannot be created")
	defer f.Close()
	n, e := f.Write(raw)
	checked(e, "Cannot write evidence")
	need(n == len(raw), "Incomplete evidence write")
}
func (e *evidence) save(name string, value any) {
	raw, ok := value.([]byte)
	if !ok {
		raw = append(encoded(value), '\n')
	}
	reserve := int64(65536)
	if name == "scan.json" || name == "inventory.json" {
		reserve = 0
	}
	need(e.sizeExcept("pages.jsonl", name)+int64(len(raw))+reserve <= e.summaryBudget, "Combined summary byte budget exceeded")
	writeNew(filepath.Join(e.out, name), raw)
}
func (e *evidence) page(row object) {
	need(e.usedPages+e.requests < e.maxPages, "Whole-task page budget exhausted")
	entry := object{"sequence": e.requests + 1, "previousEntrySha256": e.previous}
	for k, v := range row {
		entry[k] = v
	}
	raw := append(encoded(entry), '\n')
	need(e.usedBytes+e.ledgerBytes+int64(len(raw)) <= e.ledgerBudget, "Whole-task page-ledger byte budget exceeded")
	n, err := e.ledger.Write(raw)
	checked(err, "Cannot write page receipt")
	need(n == len(raw), "Incomplete page receipt")
	e.previous = digest(raw)
	e.requests++
	e.ledgerBytes += int64(len(raw))
}
func observe(root, graph, epoch, alias string, categories []string, fp *fingerprint, t *transport, guard func(), e *evidence, pageSize int) object {
	script := func(name string) string {
		raw, err := os.ReadFile(filepath.Join(root, "adapters/ldbc-gremlin", name))
		checked(err, "Cannot read fixed scan script")
		return string(raw)
	}
	schemaScript, contentScript := script("inventory-page.groovy"), script("scan-page.groovy")
	call := func(category, page string) object {
		guard()
		need(e.usedPages+e.requests < e.maxPages, "Whole-task page budget exhausted")
		gremlin := contentScript
		if category == "schema" {
			gremlin = schemaScript
		}
		body := object{"gremlin": gremlin, "language": "gremlin-groovy", "aliases": object{"g": alias}, "bindings": object{"graphName": graph, "epochMarker": epoch, "category": category, "page": page, "pageSize": pageSize}}
		receipt := object{"category": category, "pageSha256": digest([]byte(page)), "requestSha256": digest(encoded(body)), "responseSha256": nil, "errorType": nil}
		var data []any
		func() {
			defer func() {
				if caught := recover(); caught != nil {
					receipt["errorType"] = recoverError(caught)["type"]
					e.page(receipt)
					panic(caught)
				}
			}()
			var hash string
			data, hash = t.post(body)
			receipt["responseSha256"] = hash
		}()
		e.page(receipt)
		guard()
		need(len(data) == 1, "Invalid scan response envelope")
		row := obj(data[0])
		need(row["graphName"] == graph && row["epochMarker"] == epoch, "Target graph or epoch mismatch")
		return row
	}
	schema := func() any {
		row := call("schema", "")
		need(shape(row, "graphName", "epochMarker", "kind", "schema") && row["kind"] == "schema", "Invalid schema response")
		return row["schema"]
	}
	before := schema()
	pages, hidden := object{}, object{}
	for _, category := range categories {
		fold := newFold(category, graph, epoch, fp, pageSize, e.maxPages)
		var count uint64
		for !fold.complete() {
			page := *fold.next
			row := call(category, page)
			fold.accept(page, row)
			n := decimal(row["hiddenRecords"])
			need(^uint64(0)-count >= n, "Hidden record count overflow")
			count += n
		}
		pages[category] = fold.requests
		hidden[category] = count
	}
	after := schema()
	need(same(before, after), "Schema changed during exclusive content scan")
	return object{"schema": before, "pages": pages, "hiddenRecords": hidden}
}
