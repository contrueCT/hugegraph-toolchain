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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const protocol = "hgbench-keyed-logical-multiset-v1"

var bad = errors.New("invalid strict logical record")

type key struct {
	category, label string
	bucket          int
}
type sums struct {
	count uint64
	a, b  [4]uint64
}
type fingerprint struct {
	challenge []byte
	labels    map[string][]string
	values    map[key]*sums
}
type trace struct {
	IdentityHex  string `json:"identityHex"`
	RecordHex    string `json:"recordHex"`
	BucketTagHex string `json:"bucketTagHex"`
	SumAHex      string `json:"sumAHex"`
	SumBHex      string `json:"sumBHex"`
	Bucket       int    `json:"bucket"`
}

func newFP(labels map[string][]string, challenge []byte) (*fingerprint, error) {
	if len(challenge) != 32 || len(labels) != 2 {
		return nil, bad
	}
	copied := map[string][]string{}
	for _, cat := range []string{"vertices", "edges"} {
		list, ok := labels[cat]
		if !ok || len(list) > 128 {
			return nil, bad
		}
		list = append([]string(nil), list...)
		sort.Strings(list)
		for i, v := range list {
			if v == "" || !utf8.ValidString(v) || strings.HasPrefix(v, "~") || (i > 0 && list[i-1] == v) {
				return nil, bad
			}
		}
		copied[cat] = list
	}
	return &fingerprint{append([]byte(nil), challenge...), copied, map[key]*sums{}}, nil
}
func text(v any) (string, error) {
	s, ok := v.(string)
	if !ok || s == "" || !utf8.ValidString(s) {
		return "", bad
	}
	return s, nil
}
func number(v any) (int64, error) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, bad
	}
	x, e := strconv.ParseInt(string(n), 10, 64)
	if e != nil {
		return 0, bad
	}
	return x, nil
}
func normalize(category string, value any) (map[string]any, error) {
	row, ok := value.(map[string]any)
	if !ok {
		return nil, bad
	}
	fields := []string{"id", "label", "properties"}
	if category == "edges" {
		fields = []string{"sourceId", "targetId", "label", "properties"}
	} else if category != "vertices" {
		return nil, bad
	}
	if len(row) != len(fields) {
		return nil, bad
	}
	out := map[string]any{}
	for _, field := range fields {
		if field != "properties" {
			v, e := text(row[field])
			if e != nil {
				return nil, e
			}
			out[field] = v
		}
	}
	props, ok := row["properties"].(map[string]any)
	if !ok {
		return nil, bad
	}
	p := map[string]any{}
	for name, v := range props {
		if _, e := text(name); e != nil {
			return nil, e
		}
		if name == "emails" || name == "languages" {
			list, ok := v.([]any)
			if !ok {
				return nil, bad
			}
			a := make([]string, len(list))
			for i, x := range list {
				s, ok := x.(string)
				if !ok || !utf8.ValidString(s) {
					return nil, bad
				}
				a[i] = s
			}
			sort.Strings(a)
			for i := 1; i < len(a); i++ {
				if a[i] == a[i-1] {
					return nil, bad
				}
			}
			p[name] = a
		} else if s, ok := v.(string); ok {
			if !utf8.ValidString(s) {
				return nil, bad
			}
			p[name] = s
		} else {
			n, e := number(v)
			if e != nil {
				return nil, e
			}
			p[name] = n
		}
	}
	required := "ldbcId"
	if category == "edges" {
		required = "edgeKey"
	}
	if _, ok := p[required].(int64); !ok {
		return nil, bad
	}
	out["properties"] = p
	return out, nil
}
func fromSource(category, label string, value any) (map[string]any, error) {
	source, ok := value.(map[string]any)
	if !ok {
		return nil, bad
	}
	props := map[string]any{}
	row := map[string]any{"label": label, "properties": props}
	if category == "vertices" {
		row["id"] = source["vertexId"]
		for k, v := range source {
			if k != "vertexId" {
				props[k] = v
			}
		}
	} else if category == "edges" {
		row["sourceId"] = source["sourceId"]
		row["targetId"] = source["targetId"]
		for k, v := range source {
			if k != "sourceId" && k != "targetId" {
				props[k] = v
			}
		}
	} else {
		return nil, bad
	}
	return row, nil
}
func quote(out []byte, s string) []byte {
	const digits = "0123456789abcdef"
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"', '\\':
			out = append(out, '\\', c)
		case '\b':
			out = append(out, '\\', 'b')
		case '\f':
			out = append(out, '\\', 'f')
		case '\n':
			out = append(out, '\\', 'n')
		case '\r':
			out = append(out, '\\', 'r')
		case '\t':
			out = append(out, '\\', 't')
		default:
			if c < 0x20 {
				out = append(out, '\\', 'u', '0', '0', digits[c>>4], digits[c&15])
			} else {
				out = append(out, c)
			}
		}
	}
	return append(out, '"')
}
func exact(out []byte, v any) []byte {
	switch x := v.(type) {
	case string:
		out = append(out, `["str",`...)
		out = quote(out, x)
		return append(out, ']')
	case int64:
		out = append(out, `["int",`...)
		out = strconv.AppendInt(out, x, 10)
		return append(out, ']')
	case []string:
		out = append(out, `["array",[`...)
		for i, s := range x {
			if i > 0 {
				out = append(out, ',')
			}
			out = exact(out, s)
		}
		return append(out, ']', ']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out = append(out, `["object",[`...)
		for i, k := range keys {
			if i > 0 {
				out = append(out, ',')
			}
			out = append(out, '[')
			out = quote(out, k)
			out = append(out, ',')
			out = exact(out, x[k])
			out = append(out, ']')
		}
		return append(out, ']', ']')
	default:
		panic("normalized value escaped its exact type contract")
	}
}
func (f *fingerprint) tag(purpose string, raw []byte) [32]byte {
	h := hmac.New(sha256.New, f.challenge)
	h.Write([]byte(protocol + "\x00"))
	h.Write([]byte(purpose))
	h.Write([]byte{0})
	h.Write(raw)
	var out [32]byte
	h.Sum(out[:0])
	return out
}
func add256(sum *[4]uint64, raw [32]byte) {
	var carry uint64
	for i := 3; i >= 0; i-- {
		sum[i], carry = bits.Add64(sum[i], binary.BigEndian.Uint64(raw[8*i:8*i+8]), carry)
	}
}
func (f *fingerprint) add(category string, value any, wantTrace bool) (*trace, error) {
	row, e := normalize(category, value)
	if e != nil {
		return nil, e
	}
	label := row["label"].(string)
	at := sort.SearchStrings(f.labels[category], label)
	if at == len(f.labels[category]) || f.labels[category][at] != label {
		return nil, bad
	}
	identity := quote([]byte{'['}, category)
	identity = append(identity, ',')
	identity = quote(identity, label)
	identity = append(identity, ',')
	if category == "vertices" {
		identity = quote(identity, row["id"].(string))
	} else {
		identity = quote(identity, row["sourceId"].(string))
		identity = append(identity, ',')
		identity = quote(identity, row["targetId"].(string))
		identity = append(identity, ',')
		identity = strconv.AppendInt(identity, row["properties"].(map[string]any)["edgeKey"].(int64), 10)
	}
	identity = append(identity, ']')
	raw := quote([]byte{'['}, category)
	raw = append(raw, ',')
	raw = exact(raw, row)
	raw = append(raw, ']')
	bucketTag, a, b := f.tag("bucket", identity), f.tag("a", raw), f.tag("b", raw)
	bucket := int(binary.BigEndian.Uint16(bucketTag[:2])) & 63
	k := key{category, label, bucket}
	sum := f.values[k]
	if sum == nil {
		sum = &sums{}
		f.values[k] = sum
	}
	if sum.count == ^uint64(0) {
		return nil, bad
	}
	sum.count++
	add256(&sum.a, a)
	add256(&sum.b, b)
	if !wantTrace {
		return nil, nil
	}
	return &trace{hex.EncodeToString(identity), hex.EncodeToString(raw), hex.EncodeToString(bucketTag[:]), hex.EncodeToString(a[:]), hex.EncodeToString(b[:]), bucket}, nil
}
func sumHex(v [4]uint64) string {
	var raw [32]byte
	for i, n := range v {
		binary.BigEndian.PutUint64(raw[i*8:i*8+8], n)
	}
	return hex.EncodeToString(raw[:])
}
func (f *fingerprint) snapshot() map[string]any {
	rows := []map[string]any{}
	for _, cat := range []string{"edges", "vertices"} {
		for _, label := range f.labels[cat] {
			for bucket := 0; bucket < 64; bucket++ {
				s := f.values[key{cat, label, bucket}]
				if s == nil {
					s = &sums{}
				}
				rows = append(rows, map[string]any{"category": cat, "label": label, "bucket": bucket, "records": strconv.FormatUint(s.count, 10), "sumA": sumHex(s.a), "sumB": sumHex(s.b)})
			}
		}
	}
	challenge := sha256.Sum256(f.challenge)
	return map[string]any{"protocol": protocol, "buckets": 64, "rows": rows, "challengeCommitment": hex.EncodeToString(challenge[:])}
}
func hex4(raw []byte) (int, error) {
	if len(raw) != 4 {
		return 0, bad
	}
	n := 0
	for _, c := range raw {
		n <<= 4
		switch {
		case c >= '0' && c <= '9':
			n += int(c - '0')
		case c >= 'a' && c <= 'f':
			n += int(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			n += int(c - 'A' + 10)
		default:
			return 0, bad
		}
	}
	return n, nil
}
func stringsValid(raw []byte) error {
	if !utf8.Valid(raw) {
		return bad
	}
	in := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if !in {
			if c == '"' {
				in = true
			}
			continue
		}
		if c == '"' {
			in = false
			continue
		}
		if c != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return bad
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return bad
		}
		n, e := hex4(raw[i+1 : i+5])
		if e != nil {
			return e
		}
		i += 4
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return bad
			}
			low, e := hex4(raw[i+3 : i+7])
			if e != nil || low < 0xdc00 || low > 0xdfff {
				return bad
			}
			i += 6
		} else if n >= 0xdc00 && n <= 0xdfff {
			return bad
		}
	}
	return nil
}
func decodeValue(d *json.Decoder, depth int) (any, error) {
	if depth > 128 {
		return nil, bad
	}
	token, e := d.Token()
	if e != nil {
		return nil, e
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delim {
	case '{':
		out := map[string]any{}
		for d.More() {
			name, e := d.Token()
			if e != nil {
				return nil, e
			}
			k, ok := name.(string)
			if !ok {
				return nil, bad
			}
			if _, exists := out[k]; exists {
				return nil, bad
			}
			v, e := decodeValue(d, depth+1)
			if e != nil {
				return nil, e
			}
			out[k] = v
		}
		end, e := d.Token()
		if e != nil || end != json.Delim('}') {
			return nil, bad
		}
		return out, nil
	case '[':
		out := []any{}
		for d.More() {
			v, e := decodeValue(d, depth+1)
			if e != nil {
				return nil, e
			}
			out = append(out, v)
		}
		end, e := d.Token()
		if e != nil || end != json.Delim(']') {
			return nil, bad
		}
		return out, nil
	default:
		return nil, bad
	}
}
func strict(raw []byte) (any, error) {
	raw = bytes.TrimPrefix(raw, []byte{0xef, 0xbb, 0xbf})
	if len(raw) > 64*1024*1024 {
		return nil, bad
	}
	if e := stringsValid(raw); e != nil {
		return nil, e
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	v, e := decodeValue(d, 0)
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, bad
	}
	return v, nil
}
