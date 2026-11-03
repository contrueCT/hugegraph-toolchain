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
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type hashedInput struct {
	reader       io.Reader
	hash         hash.Hash
	bytes, limit uint64
}

func (h *hashedInput) Read(p []byte) (int, error) {
	n, e := h.reader.Read(p)
	h.bytes += uint64(n)
	need(h.bytes <= h.limit, "Stored source byte budget exceeded")
	h.hash.Write(p[:n])
	return n, e
}
func line(reader *bufio.Reader, limit int) []byte {
	raw := []byte{}
	for {
		part, e := reader.ReadSlice('\n')
		need(len(raw)+len(part) <= limit, "Oversized source record")
		raw = append(raw, part...)
		if e == bufio.ErrBufferFull {
			continue
		}
		if e == io.EOF {
			need(len(raw) == 0, "Unterminated source record")
			return nil
		}
		checked(e, "Source stream or gzip checksum failed")
		return raw
	}
}
func loadExpected(base string, prepared object) object {
	fact := obj(obj(prepared["files"])["inventory.expected.json"])
	path := filepath.Join(base, "inventory.expected.json")
	expected, raw := readJSON(path, 4<<20)
	need(digest(raw) == fact["sha256"] && bytesOf(uint64(len(raw))) == fmtCount(fact["bytes"]), "Expected inventory identity mismatch")
	need(shape(expected, "schemaVersion", "scope", "schema", "counts") && intValue(expected["schemaVersion"]) == 1 && expected["scope"] == "public-ldbc-initial-graph", "Invalid initial inventory contract")
	schema, counts := obj(expected["schema"]), obj(expected["counts"])
	need(shape(schema, "propertyKeys", "vertexLabels", "edgeLabels", "indexLabels") && shape(counts, "vertices", "edges"), "Invalid schema/count inventory")
	totals := map[string]map[string]uint64{"vertices": {}, "edges": {}}
	for _, v := range array(prepared["shards"]) {
		s := obj(v)
		cat, label := str(s["category"]), str(s["label"])
		group, ok := totals[cat]
		need(ok, "Invalid source category")
		n := decimal(s["records"])
		need(^uint64(0)-group[label] >= n, "Source count overflow")
		group[label] += n
	}
	for _, cat := range []string{"vertices", "edges"} {
		labels := "vertexLabels"
		if cat == "edges" {
			labels = "edgeLabels"
		}
		group := obj(counts[cat])
		declared := obj(schema[labels])
		need(len(group) == len(declared), "Schema/count label inventory mismatch")
		var total uint64
		for label, n := range group {
			_, ok := declared[label]
			need(ok, "Schema/count label inventory mismatch")
			count := decimal(n)
			var declaredCount uint64
			if value, exists := obj(prepared["counts"])[label]; exists {
				declaredCount = nonnegative(value)
			}
			need(count == totals[cat][label] && count == declaredCount, "Inventory count differs from source")
			need(^uint64(0)-total >= count, "Source count overflow")
			total += count
		}
		for label := range totals[cat] {
			_, ok := group[label]
			need(ok, "Undeclared prepared label")
		}
		need(total == nonnegative(prepared[cat]), "Prepared total disagrees with per-label counts")
	}
	return expected
}
func fmtCount(v any) string {
	if n, ok := v.(string); ok {
		return n
	}
	return strconv.FormatInt(intValue(v), 10)
}
func sourceFingerprint(prepared object, base string, categories []string, fp *fingerprint, guard func(), record func(object)) object {
	totals := object{"vertices": "0", "edges": "0"}
	ids, paths := map[string]bool{}, map[string]bool{}
	selected := map[string]bool{}
	for _, cat := range categories {
		selected[cat] = true
	}
	for _, v := range array(prepared["shards"]) {
		guard()
		s := obj(v)
		cat, id, relative := str(s["category"]), str(s["id"]), str(s["path"])
		need((cat == "vertices" || cat == "edges") && !ids[id] && !paths[relative], "Duplicate or invalid shard identity")
		ids[id] = true
		paths[relative] = true
		artifact := obj(obj(prepared["files"])[relative])
		need(artifact["sha256"] == s["sha256"] && fmtCount(artifact["bytes"]) == s["bytes"], "Shard/file identity mismatch")
		path := safePath(base, relative)
		before := identify(path)
		storedBytes := decimal(s["bytes"])
		need(uint64(before.info.Size()) == storedBytes, "Stored shard byte count differs")
		if !selected[cat] {
			continue
		}
		expandedBytes, expandedDigest := storedBytes, str(s["sha256"])
		compressed := s["encoding"] != nil
		if compressed {
			encoding := obj(s["encoding"])
			need(shape(encoding, "compression", "uncompressedBytes", "uncompressedSha256") && encoding["compression"] == "gzip", "Invalid expanded source identity")
			expandedBytes = decimal(encoding["uncompressedBytes"])
			expandedDigest = str(encoding["uncompressedSha256"])
		}
		need(shaPattern.MatchString(expandedDigest) && shaPattern.MatchString(str(s["sha256"])), "Invalid source digest")
		expectedRecords := decimal(s["records"])
		var bytesRead, records uint64
		expanded := sha256.New()
		func() {
			f, e := os.Open(path)
			checked(e, "Cannot open source")
			defer f.Close()
			stored := &hashedInput{reader: f, hash: sha256.New(), limit: storedBytes}
			var reader io.Reader = stored
			if compressed {
				gz, e := gzip.NewReader(stored)
				checked(e, "Invalid gzip source")
				defer gz.Close()
				reader = gz
			}
			buffer := bufio.NewReaderSize(reader, 128<<10)
			for {
				maximum := uint64(16<<20) + 1
				if expandedBytes-bytesRead+1 < maximum {
					maximum = expandedBytes - bytesRead + 1
				}
				raw := line(buffer, int(maximum))
				if raw == nil {
					break
				}
				need(len(raw) <= 16<<20, "Oversized source record")
				bytesRead += uint64(len(raw))
				need(bytesRead <= expandedBytes && records < expectedRecords, "Source exceeds frozen byte or record count")
				expanded.Write(raw)
				row, e := fromSource(cat, str(s["label"]), decode(raw))
				checked(e, "Invalid source record")
				_, e = fp.add(cat, row, false)
				checked(e, "Invalid source logical record")
				records++
				if records%1024 == 0 {
					guard()
				}
			}
			need(stored.bytes == storedBytes && hex.EncodeToString(stored.hash.Sum(nil)) == s["sha256"], "Stored source digest or byte count differs")
		}()
		need(before.equal(identify(path)), "Source changed while scanning")
		actualDigest := hex.EncodeToString(expanded.Sum(nil))
		need(bytesRead == expandedBytes && actualDigest == expandedDigest && records == expectedRecords, "Expanded source digest or record count differs")
		guard()
		old := decimal(totals[cat])
		need(^uint64(0)-old >= records, "Source count overflow")
		totals[cat] = bytesOf(old + records)
		record(object{"shardId": id, "category": cat, "records": bytesOf(records), "storedBytes": s["bytes"], "storedSha256": s["sha256"], "expandedBytes": bytesOf(bytesRead), "expandedSha256": actualDigest})
	}
	result := object{}
	for _, cat := range []string{"vertices", "edges"} {
		n := decimal(totals[cat])
		result[cat] = n
		if selected[cat] {
			need(n == nonnegative(prepared[cat]), "Source category total differs from frozen inventory")
		}
	}
	return result
}
