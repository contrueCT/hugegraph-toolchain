// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package compat

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestASCIIStringBufferBoundaries(t *testing.T) {
	suffixes := [][2]string{
		{"", ""},
		{`\"\\\/\b\f\n\r\t\u0000`, `\"\\/\b\f\n\r\t\u0000`},
		{`汉🙂\ud800`, `汉🙂\ud800`},
		{`\u0061\u2028\u2029`, "a\u2028\u2029"},
	}
	for _, length := range []int{0, 1, 15, 16, 63, 64, 65, 16383, 16384, 16385, 32768, 1048576} {
		for index, suffix := range suffixes {
			t.Run(fmt.Sprintf("%d-%d", length, index), func(t *testing.T) {
				prefix := strings.Repeat("x", length)
				input := []byte(`"` + prefix + suffix[0] + `"`)
				v, err := Parse(input)
				if err != nil {
					t.Fatal(err)
				}
				// The parsed value owns its UTF-16 contents, independent of later source mutation.
				for i := range input {
					input[i] = 0
				}
				var out bytes.Buffer
				if err = v.WriteCanonical(&out); err != nil {
					t.Fatal(err)
				}
				want := `"` + prefix + suffix[1] + `"`
				if out.String() != want {
					t.Fatalf("canonical mismatch at %d/%d", length, index)
				}
			})
		}
	}
}
func TestASCIIInvalidSuffixesRemainRejected(t *testing.T) {
	prefix := `"` + strings.Repeat("x", 32768)
	for _, suffix := range []string{"", "\x01\"", `\uZZZZ"`, string([]byte{0xff, '"'})} {
		if _, err := Parse([]byte(prefix + suffix)); err == nil {
			t.Fatal("invalid suffix accepted")
		}
	}
}

var benchmarkDigest string

func BenchmarkCompatPayload(b *testing.B) {
	var rows strings.Builder
	rows.WriteString(`{"status":{"code":200},"result":{"data":[`)
	for i := 0; i < 4096; i++ {
		if i > 0 {
			rows.WriteByte(',')
		}
		fmt.Fprintf(&rows, `{"id":%d,"name":"hgbench","value":%d,"active":%t}`, i, i*3, i%2 == 0)
	}
	rows.WriteString(`]}}`)
	inputs := []struct {
		name string
		raw  []byte
	}{
		{"string-1024k", []byte(`{"status":{"code":200},"result":{"data":["` + strings.Repeat("x", 1048576) + `"]}}`)},
		{"rows-4096", []byte(rows.String())},
	}
	for _, input := range inputs {
		b.Run(input.name+"/Parse", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(input.raw)))
			for i := 0; i < b.N; i++ {
				if _, err := Parse(input.raw); err != nil {
					b.Fatal(err)
				}
			}
		})
		v, err := Parse(input.raw)
		if err != nil {
			b.Fatal(err)
		}
		data, ok := v.Pointer("/result/data")
		if !ok {
			b.Fatal("data missing")
		}
		b.Run(input.name+"/CanonicalDigest", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(input.raw)))
			for i := 0; i < b.N; i++ {
				benchmarkDigest = data.Digest()
			}
		})
	}
}

func TestLongASCIIJSONPointer(t *testing.T) {
	key := strings.Repeat("segment", 128)
	v, err := Parse([]byte(`{"` + key + `":"` + key + `","slash/key":"ok"}`))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := v.PointerJSON([]byte(`"/` + key + `"`))
	if !ok {
		t.Fatal("long ASCII pointer did not resolve")
	}
	var out bytes.Buffer
	if err = got.WriteCanonical(&out); err != nil || out.String() != `"`+key+`"` {
		t.Fatal("wrong pointer result", err)
	}
	if _, ok = v.PointerJSON([]byte(`"/slash~1key"`)); !ok {
		t.Fatal("escaped short pointer did not resolve")
	}
	if _, ok = v.PointerJSON([]byte(`"/` + key + `~2"`)); ok {
		t.Fatal("invalid long pointer accepted")
	}
}

func TestOwnedRepeatedAndOverflowKeys(t *testing.T) {
	var raw strings.Builder
	raw.WriteString(`[{`)
	for i := 0; i < 100; i++ {
		if i > 0 {
			raw.WriteByte(',')
		}
		fmt.Fprintf(&raw, `"k%03d":%d`, i, i)
	}
	raw.WriteString(`},{"same":1,"sa\u006de":2,"0":3,"00":4},{"same":5,"unicode汉":6}]`)
	input := []byte(raw.String())
	v, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	for i := range input {
		input[i] = 0
	}
	for i := 0; i < 100; i++ {
		got, ok := v.Pointer(fmt.Sprintf("/0/k%03d", i))
		if !ok {
			t.Fatal("missing cached/uncached key", i)
		}
		var out bytes.Buffer
		if err = got.WriteCanonical(&out); err != nil || out.String() != fmt.Sprint(i) {
			t.Fatal("key ownership/value", i, err)
		}
	}
	for _, row := range [][2]string{{"/1/same", "2"}, {"/2/same", "5"}, {"/2/unicode汉", "6"}, {"/1/0", "3"}, {"/1/00", "4"}} {
		got, ok := v.Pointer(row[0])
		if !ok {
			t.Fatal("missing pointer", row[0])
		}
		var out bytes.Buffer
		if err = got.WriteCanonical(&out); err != nil || out.String() != row[1] {
			t.Fatal("duplicate/Unicode/key value", row[0], err)
		}
	}
}

func TestEarlyRepeatedPlainAndEscapedKeys(t *testing.T) {
	raw := []byte(`[{"same":1},{"same":2,"sa\u006de":3}]`)
	v, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0
	}
	for _, row := range [][2]string{{"/0/same", "1"}, {"/1/same", "3"}} {
		got, ok := v.Pointer(row[0])
		if !ok {
			t.Fatal("missing repeated key")
		}
		var out bytes.Buffer
		if err = got.WriteCanonical(&out); err != nil || out.String() != row[1] {
			t.Fatal("changed repeated key", row[0], err)
		}
	}
}

func TestAllPlainASCIICharacters(t *testing.T) {
	var chars strings.Builder
	for c := byte(0x20); c < 0x80; c++ {
		if c != '"' && c != '\\' {
			chars.WriteByte(c)
		}
	}
	text := strings.Repeat(chars.String(), 1024)
	input := []byte(`"` + text + `"`)
	v, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	for i := range input {
		input[i] = 0
	}
	var out bytes.Buffer
	if err = v.WriteCanonical(&out); err != nil || out.String() != `"`+text+`"` {
		t.Fatal("plain ASCII ownership/canonical mismatch", err)
	}
}
