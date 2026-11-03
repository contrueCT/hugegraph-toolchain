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
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func validateEndpoint(endpoint string, target object) {
	parsed, e := url.Parse(endpoint)
	need(e == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Hostname() != "" && parsed.User == nil && parsed.Fragment == "" && parsed.Opaque == "", "Explicit HTTP(S) endpoint required without credentials or fragment")
	origin, e := url.Parse(str(target["host"]))
	checked(e, "Invalid frozen target origin")
	port := parsed.Port()
	if port == "" {
		port = "80"
		if parsed.Scheme == "https" {
			port = "443"
		}
	}
	n, e := strconv.Atoi(port)
	checked(e, "Invalid target port")
	need(parsed.Scheme == origin.Scheme && strings.EqualFold(parsed.Hostname(), origin.Hostname()) && int64(n) == intValue(target["port"]) && parsed.EscapedPath() == "/gremlin" && parsed.RawQuery == "" && !parsed.ForceQuery, "Scan endpoint differs from frozen Loader target")
}
func pageLedger(path string, requests int, guard func()) object {
	guard()
	before := identify(path)
	need(before.info.Size() <= 1<<30, "Prior page ledger exceeds byte budget")
	file, e := os.Open(path)
	checked(e, "Cannot open prior page ledger")
	defer file.Close()
	reader := bufio.NewReaderSize(file, 8192)
	hash := sha256.New()
	previous := strings.Repeat("0", 64)
	n, vertices := 0, 0
	schemas := []int{}
	for {
		raw, e := reader.ReadBytes('\n')
		if e == io.EOF && len(raw) == 0 {
			break
		}
		checked(e, "Invalid prior page ledger")
		need(len(raw) <= 8192 && raw[len(raw)-1] == '\n', "Invalid prior page-ledger line")
		row := obj(decode(raw))
		n++
		if n%64 == 0 {
			guard()
		}
		category := row["category"]
		need(intValue(row["sequence"]) == int64(n) && row["previousEntrySha256"] == previous && row["errorType"] == nil && (category == "schema" || category == "vertices") && shaPattern.MatchString(str(row["responseSha256"])), "Prior page-ledger chain or response differs")
		if category == "vertices" {
			vertices++
		} else {
			schemas = append(schemas, n)
		}
		hash.Write(raw)
		previous = digest(raw)
	}
	need(before.equal(identify(path)) && n == requests && len(schemas) == 2 && schemas[0] == 1 && schemas[1] == n && vertices == n-2, "Prior page-ledger count/scope differs")
	return object{"path": filepath.Base(path), "bytes": strconv.FormatInt(before.info.Size(), 10), "sha256": hex.EncodeToString(hash.Sum(nil))}
}
func priorUsage(path, expectedSHA string, binding object, component string, budgets object, guard func()) (int, int64, string) {
	guard()
	report, raw := readJSON(path, 4<<20)
	need(digest(raw) == expectedSHA && report["kind"] == "sf100-sequential-content-observation" && report["outcome"] == "matched" && report["scope"] == "vertices" && report["contentMatches"] == true && report["schemaMatches"] == true && report["countsMatch"] == true && same(report["binding"], binding) && report["error"] == nil && report["protocol"] == protocol && report["vertexAdmission"] == "content" && report["datasetVerified"] == false && report["priorScan"] == nil && report["componentSha256"] == component, "Prior vertex-content proof identity differs")
	need(same(report["budgets"], budgets) && intValue(report["wholeTaskPages"]) == intValue(report["requests"]) && intValue(report["wholeTaskLedgerBytes"]) == intValue(report["ledgerBytes"]), "Prior scan budget differs")
	requests, usedBytes := intValue(report["requests"]), intValue(report["ledgerBytes"])
	need(requests > 0 && requests < int64(budgets["maxPages"].(int)) && usedBytes >= 0 && usedBytes < budgets["ledgerBytes"].(int64), "Invalid prior scan budget usage")
	required := map[string]bool{"expected.json": true, "actual.json": true, "inputs.jsonl": true, "pages.jsonl": true, "quiescence.json": true, "challenge.json": true}
	facts := array(report["evidence"])
	need(len(facts) == len(required), "Incomplete prior scan evidence")
	for _, v := range facts {
		fact := obj(v)
		name := str(fact["path"])
		need(required[name], "Duplicate or unexpected prior scan evidence")
		delete(required, name)
		var actual object
		if name == "pages.jsonl" {
			actual = pageLedger(filepath.Join(filepath.Dir(path), name), int(requests), guard)
			need(decimal(fact["bytes"]) == uint64(usedBytes), "Prior ledger usage differs")
		} else {
			actual = fileFact(filepath.Join(filepath.Dir(path), name), guard)
		}
		need(same(actual, fact), "Prior scan evidence digest differs")
	}
	wanted, _ := readJSON(filepath.Join(filepath.Dir(path), "expected.json"), 4<<20)
	actual, _ := readJSON(filepath.Join(filepath.Dir(path), "actual.json"), 4<<20)
	need(same(wanted, actual) && wanted["protocol"] == protocol && wanted["challengeCommitment"] == report["challengeCommitment"], "Prior digest comparison differs")
	return int(requests), usedBytes, str(report["challengeCommitment"])
}
