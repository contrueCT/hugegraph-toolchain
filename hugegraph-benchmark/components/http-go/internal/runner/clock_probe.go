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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"runtime/debug"
)

// ClockProbe exposes the same clock implementation as Run, with bounded exchanges.
// It sends no HTTP requests. The host independently observes the live container PID.
func ClockProbe(ctx context.Context, input io.Reader, output io.Writer, binary map[string]any) error {
	runtime.GOMAXPROCS(2)
	debug.SetGCPercent(100)
	debug.SetMemoryLimit(1<<63 - 1)
	c := newClock()
	encoder := json.NewEncoder(output)
	if e := encoder.Encode(map[string]any{"schemaVersion": 1, "kind": "http-go-clock-ready", "engine": Engine, "binary": binary, "clock": c.identity}); e != nil {
		return e
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 4096)
	for sequence := 0; sequence < 5; sequence++ {
		if !scanner.Scan() {
			if e := scanner.Err(); e != nil {
				return e
			}
			return errors.New("clock probe input ended before finish")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var request struct {
			SchemaVersion int    `json:"schemaVersion"`
			Kind          string `json:"kind"`
			Sequence      int    `json:"sequence"`
			Nonce         string `json:"nonce"`
		}
		if e := json.Unmarshal(scanner.Bytes(), &request); e != nil || request.SchemaVersion != 1 || request.Sequence != sequence || len(request.Nonce) != 32 {
			return errors.New("invalid clock probe exchange")
		}
		for _, r := range request.Nonce {
			if !(r >= 'a' && r <= 'f' || r >= '0' && r <= '9') {
				return errors.New("invalid clock probe nonce")
			}
		}
		expected := "sample"
		kind := "http-go-clock-sample"
		if sequence == 4 {
			expected = "finish"
			kind = "http-go-clock-finished"
		}
		if request.Kind != expected {
			return errors.New("invalid clock probe request kind")
		}
		boundary, _ := c.boundary()
		response := map[string]any{"schemaVersion": 1, "kind": kind, "engine": Engine, "sequence": sequence, "nonce": request.Nonce, "boundary": boundary}
		if sequence == 4 {
			response["processIdentityAfter"] = processIdentity()
		}
		if e := encoder.Encode(response); e != nil {
			return e
		}
	}
	return nil
}
