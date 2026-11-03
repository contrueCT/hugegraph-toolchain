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
	"io"
	"net/http"
	"strings"
)

type httpFailure struct {
	status  int
	details object
}

func safeHTTPFailure(response *http.Response, maxBytes int64) httpFailure {
	limit := min(maxBytes, int64(65536))
	details := object{"status": response.StatusCode, "bodyState": "incomplete", "wirePrefixBytes": 0, "wirePrefixSha256": digest(nil)}
	failure := httpFailure{response.StatusCode, details}
	var raw []byte
	func() {
		defer func() { _ = recover() }()
		var e error
		raw, e = io.ReadAll(io.LimitReader(response.Body, limit+1))
		if e != nil {
			return
		}
		if int64(len(raw)) > limit {
			details["bodyState"] = "wire-limit"
			return
		}
		details["bodyState"] = "invalid-encoding"
		decoded := decodedBody(raw, response.Header.Get("Content-Encoding"), limit)
		details["bodyState"] = "invalid-json"
		value := obj(decode(decoded))
		details["bodyState"] = "complete-json"
		if status, ok := value["status"].(map[string]any); ok {
			if n, e := number(status["code"]); e == nil && n >= 100 && n <= 599 {
				details["providerCode"] = n
			}
		}
		if exception, ok := value["exception"].(string); ok && len(exception) <= 256 {
			parts := strings.Split(exception, ".")
			name := parts[len(parts)-1]
			for _, known := range []string{"TimeoutException", "RejectedExecutionException", "IllegalArgumentException", "IllegalStateException", "NotFoundException", "ServerGremlinException", "BackendException", "SecurityException", "NoSuchElementException", "UnsupportedOperationException", "IOException"} {
				if name == known {
					details["providerException"] = name
				}
			}
		}
	}()
	if int64(len(raw)) > limit {
		raw = raw[:limit]
	}
	details["wirePrefixBytes"], details["wirePrefixSha256"] = len(raw), digest(raw)
	return failure
}
