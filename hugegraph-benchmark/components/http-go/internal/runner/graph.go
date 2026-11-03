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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

func validateGraphPool(in Input) error {
	root, _ := url.Parse(in.Profile.Endpoint)
	seen := map[string]bool{}
	if len(in.Case.RequestPool) > 100000 {
		return errors.New("graph pool too large")
	}
	for _, r := range in.Case.RequestPool {
		u, e := url.Parse(r.URL)
		if e != nil || u.Scheme != root.Scheme || u.Host != root.Host || u.User != nil || u.Fragment != "" || (r.Method != "GET" && r.Method != "POST") || !json.Valid(r.Body) || r.ID == "" || r.Stratum == "" || seen[r.ID] || r.ExpectedStatus < 100 || r.ExpectedStatus > 599 {
			return errors.New("invalid graph request or origin")
		}
		seen[r.ID] = true
		q := r.Rule
		switch q.Kind {
		case "vertex":
			if q.ID == "" || q.Label == "" || (q.Absent && r.ExpectedStatus != 404) || (!q.Absent && r.ExpectedStatus != 200) {
				return errors.New("invalid vertex rule")
			}
		case "id-set":
			allowed := map[string]bool{}
			for _, id := range q.Allowed {
				if id == "" || allowed[id] {
					return errors.New("duplicate/empty reference ID")
				}
				allowed[id] = true
			}
			if q.Count < 0 || q.Count > len(allowed) || len(allowed) > 100000 || !strings.HasPrefix(q.Pointer, "/") {
				return errors.New("invalid set rule")
			}
		case "shortest-path":
			if q.Source == "" || q.Target == "" || q.Distance < -1 || q.Distance > 3 || len(q.Edges) > 100000 || !strings.HasPrefix(q.Pointer, "/") {
				return errors.New("invalid path rule")
			}
		default:
			return errors.New("unknown graph rule")
		}
	}
	return nil
}

// JSON pointers select only the small semantic projection retained in the trace.
// IDs must be JSON strings; no numeric ID is converted through binary64.
func graphProjection(raw []byte, rule GraphRule) (json.RawMessage, error) {
	if rule.Kind == "vertex" {
		var object map[string]json.RawMessage
		if e := json.Unmarshal(raw, &object); e != nil || object == nil {
			return nil, errors.New("invalid vertex response")
		}
		keys := []string{"id", "label"}
		if rule.Absent {
			keys = []string{"exception", "message"}
		}
		value := map[string]json.RawMessage{}
		for _, k := range keys {
			v, ok := object[k]
			if !ok {
				return nil, fmt.Errorf("missing %s", k)
			}
			value[k] = v
		}
		return json.Marshal(value)
	}
	value := json.RawMessage(raw)
	for _, part := range strings.Split(rule.Pointer[1:], "/") {
		key := strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		if len(value) > 0 && value[0] == '[' {
			var array []json.RawMessage
			n, e := strconv.Atoi(key)
			if e != nil || n < 0 || strconv.Itoa(n) != key || json.Unmarshal(value, &array) != nil || n >= len(array) {
				return nil, errors.New("missing array pointer")
			}
			value = array[n]
		} else {
			var object map[string]json.RawMessage
			if json.Unmarshal(value, &object) != nil || object == nil {
				return nil, errors.New("invalid graph pointer")
			}
			var ok bool
			value, ok = object[key]
			if !ok {
				return nil, errors.New("missing graph pointer")
			}
		}
	}
	var ids []string
	if json.Unmarshal(value, &ids) != nil || string(value) == "null" || len(ids) > 100000 {
		return nil, errors.New("expected string ID array")
	}
	return json.Marshal(ids)
}
func validGraphValue(value json.RawMessage, q GraphRule) bool {
	if q.Kind == "vertex" {
		var v map[string]string
		if json.Unmarshal(value, &v) != nil {
			return false
		}
		if q.Absent {
			return strings.HasSuffix(v["exception"], "NotFoundException") && strings.Contains(strings.ToLower(v["message"]), "vertex")
		}
		return v["id"] == q.ID && v["label"] == q.Label
	}
	var ids []string
	if json.Unmarshal(value, &ids) != nil || string(value) == "null" {
		return false
	}
	if q.Kind == "id-set" {
		if len(ids) != q.Count {
			return false
		}
		allowed, seen := map[string]bool{}, map[string]bool{}
		for _, s := range q.Allowed {
			allowed[s] = true
		}
		for _, s := range ids {
			if !allowed[s] || seen[s] {
				return false
			}
			seen[s] = true
		}
		return true
	}
	if q.Distance == -1 {
		return len(ids) == 0
	}
	if len(ids) != q.Distance+1 || ids[0] != q.Source || ids[len(ids)-1] != q.Target {
		return false
	}
	edges, seen := map[[2]string]bool{}, map[string]bool{}
	for _, e := range q.Edges {
		edges[e] = true
	}
	for i, id := range ids {
		if seen[id] || (i > 0 && !edges[[2]string{ids[i-1], id}]) {
			return false
		}
		seen[id] = true
	}
	return true
}
