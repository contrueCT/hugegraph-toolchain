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
package compat

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestCanonical(t *testing.T) {
	cases := [][2]string{
		{`{"10":10,"2":2,"a":0}`, `{"2":2,"10":10,"a":0}`},
		{`{"\ue000":1,"\ud800\udc00":2}`, "{\"𐀀\":2,\"\ue000\":1}"},
		{`["<>&","\u2028\u2029","\ud800","\udc00",-0,1e400]`, "[\"<>&\",\"\u2028\u2029\",\"\\ud800\",\"\\udc00\",0,null]"},
		{`{"x":1,"x":2}`, `{"x":2}`},
		{`[9007199254740993,1e-7,1e-6,1e20,1e21]`, `[9007199254740992,1e-7,0.000001,100000000000000000000,1e+21]`},
	}
	for _, c := range cases {
		v, e := Parse([]byte(c[0]))
		if e != nil {
			t.Fatal(e)
		}
		var out bytes.Buffer
		if e = v.WriteCanonical(&out); e != nil || out.String() != c[1] {
			t.Fatalf("%s => %q, want %q (%v)", c[0], out.String(), c[1], e)
		}
	}
}
func TestInvalidJSON(t *testing.T) {
	for _, s := range []string{"", `01`, `-`, `[1,]`, `{"x":}`, `truefalse`, `NaN`, `"\x00"`, string([]byte{'"', 0xff, '"'}), strings.Repeat("[", 258) + "0" + strings.Repeat("]", 258)} {
		if _, e := Parse([]byte(s)); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}
func TestPointer(t *testing.T) {
	v, e := Parse([]byte(`{"a/b":{"~key":[null,false,"图🙂"]},"\ud800":42}`))
	if e != nil {
		t.Fatal(e)
	}
	for _, p := range []string{"/a~1b/~0key/2", "/a~1b/~0key/length"} {
		q, ok := v.Pointer(p)
		if !ok || q == nil {
			t.Fatal(p)
		}
	}
	q, ok := v.PointerJSON([]byte(`"/\ud800"`))
	if !ok || q.number != 42 {
		t.Fatal("UTF16 pointer")
	}
	for _, p := range []string{"/a~2b", "/missing", "/a~1b/~0key/02", "/a~1b/~0key/3", "/a~1b/~0key/2/x"} {
		if _, ok := v.Pointer(p); ok {
			t.Fatal(p)
		}
	}
}
func TestGoldenVectors(t *testing.T) {
	path := os.Getenv("HGBENCH_GO_GOLDEN")
	if path == "" {
		t.Skip("cross-language vectors supplied by verification harness")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var rows []struct {
		ID       string `json:"id"`
		JSON     string `json:"json"`
		Expected string `json:"expected"`
	}
	if e = json.Unmarshal(raw, &rows); e != nil || len(rows) < 22 {
		t.Fatal("golden input", e)
	}
	for _, r := range rows {
		v, e := Parse([]byte(r.JSON))
		if e != nil {
			t.Fatalf("%s: %v", r.ID, e)
		}
		if v.Digest() != r.Expected {
			t.Fatalf("%s: %s != %s", r.ID, v.Digest(), r.Expected)
		}
	}
	t.Logf("verified %d JS oracle vectors", len(rows))
}
