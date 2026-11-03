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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGraphSemanticRules(t *testing.T) {
	set := GraphRule{Kind: "id-set", Pointer: "/vertices", Allowed: []string{"person:9007199254740993", "b", "c"}, Count: 2}
	path := GraphRule{Kind: "shortest-path", Pointer: "/path", Source: "a", Target: "d", Distance: 2, Edges: [][2]string{{"a", "b"}, {"b", "d"}, {"a", "c"}, {"c", "d"}}}
	for _, c := range []struct {
		rule  GraphRule
		raw   string
		valid bool
	}{
		{set, `["b","person:9007199254740993"]`, true}, {set, `["c","b"]`, true}, {set, `["b","b"]`, false}, {set, `["b"]`, false}, {set, `["b","wrong"]`, false}, {set, `[9007199254740993,"b"]`, false}, {set, `null`, false},
		{path, `["a","b","d"]`, true}, {path, `["a","c","d"]`, true}, {path, `["a","d"]`, false}, {path, `["a","d","d"]`, false}, {path, `["d","b","a"]`, false},
		{GraphRule{Kind: "shortest-path", Distance: -1}, `[]`, true}, {GraphRule{Kind: "shortest-path", Distance: -1}, `null`, false},
		{GraphRule{Kind: "vertex", ID: "person:9007199254740993", Label: "person"}, `{"id":"person:9007199254740993","label":"person"}`, true},
		{GraphRule{Kind: "vertex", ID: "person:9007199254740993", Label: "person"}, `{"id":9007199254740993,"label":"person"}`, false},
		{GraphRule{Kind: "vertex", Absent: true}, `{"exception":"x.NotFoundException","message":"Vertex missing"}`, true},
		{GraphRule{Kind: "vertex", Absent: true}, `{"exception":"x.Error","message":"Vertex missing"}`, false},
	} {
		if got := validGraphValue(json.RawMessage(c.raw), c.rule); got != c.valid {
			t.Fatalf("%s: got %v", c.raw, got)
		}
	}
	for _, raw := range []string{`{"vertices":["b","c"]}`, ` { "vertices" : ["b","c"] } `} {
		v, e := graphProjection([]byte(raw), set)
		if e != nil || !validGraphValue(v, set) {
			t.Fatal(string(v), e)
		}
	}
	if _, e := graphProjection([]byte(`{"vertices":[1]}`), set); e == nil {
		t.Fatal("numeric graph ID accepted")
	}
}
func TestGraphPoolActualRequestsAndProjection(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/vertex" {
			if r.Method != "GET" {
				t.Error("expected native GET")
			}
			io.WriteString(w, `{"id":"person:9007199254740993","label":"person","properties":{"unexamined":1}}`)
		} else {
			body, _ := io.ReadAll(r.Body)
			if r.Method != "POST" || string(body) != `{"bindings":{"userId":"a"}}` {
				t.Errorf("wrong binding: %s", body)
			}
			io.WriteString(w, `{"result":{"data":["c","b"]}}`)
		}
	}))
	defer s.Close()
	in := input(s.URL)
	in.Profile.WarmupMs = 100
	in.Load = Load{ID: "c2", Model: "closed", Concurrency: 2}
	in.Profile.MaxRequests = 8
	in.Case.RequestPool = []GraphRequest{
		{ID: "p0", Stratum: "hit", Method: "GET", URL: s.URL + "/vertex", Body: json.RawMessage(`null`), ExpectedStatus: 200, Rule: GraphRule{Kind: "vertex", ID: "person:9007199254740993", Label: "person"}},
		{ID: "p1", Stratum: "high", Method: "POST", URL: s.URL + "/gremlin", Body: json.RawMessage(`{"bindings":{"userId":"a"}}`), ExpectedStatus: 200, Rule: GraphRule{Kind: "id-set", Pointer: "/result/data", Allowed: []string{"b", "c"}, Count: 2}},
	}
	r, rows := runTest(t, in, context.Background())
	if len(rows) != 16 || r.Phases[1].Failed != 0 {
		t.Fatal(r, len(rows))
	}
	for _, row := range rows {
		if row.Outcome != "ok" || row.ParameterIndex == nil || *row.ParameterIndex != row.Sequence%2 || len(row.GraphValue) == 0 || len(row.CheckDigests) != 0 {
			t.Fatal(row)
		}
	}
	in.Case.RequestPool[0].URL = "http://example.com/foreign"
	if in.Validate() == nil {
		t.Fatal("cross-origin pool accepted")
	}
}
