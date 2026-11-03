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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/apache/hugegraph-toolchain/hugegraph-benchmark/components/http-go/internal/runner"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func waitMarker(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("marker was not published")
		case <-tick.C:
			raw, e := os.ReadFile(path)
			if errors.Is(e, os.ErrNotExist) {
				continue
			}
			if e != nil {
				t.Fatal(e)
			}
			var v map[string]any
			if json.Unmarshal(raw, &v) != nil {
				t.Fatal("partially visible marker")
			}
			return raw
		}
	}
}
func releaseMarker(t *testing.T, directory, stage string, raw []byte, wrong bool) {
	t.Helper()
	var m map[string]any
	if e := json.Unmarshal(raw, &m); e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(raw)
	nonce := m["nonce"]
	if wrong {
		nonce = "replayed"
	}
	v := map[string]any{"schemaVersion": 1, "kind": "http-go-live-process-release", "id": m["id"], "stage": stage, "nonce": nonce, "markerSha256": hex.EncodeToString(sum[:])}
	if e := marker(filepath.Join(directory, stage+"-release.json"), v); e != nil {
		t.Fatal(e)
	}
}
func TestIdentityGateExactTwoStageRelease(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "gate")
	observe, e := identityGate(directory, "r0001", map[string]any{"binarySha256": "fixture"})
	if e != nil {
		t.Fatal(e)
	}
	for _, stage := range []string{"ready", "finished"} {
		done := make(chan error, 1)
		go func() { done <- observe(context.Background(), stage, runner.ClockIdentity{PID: 123}) }()
		raw := waitMarker(t, filepath.Join(directory, stage+".json"))
		select {
		case <-done:
			t.Fatal("phase was released without host acknowledgement")
		default:
		}
		releaseMarker(t, directory, stage, raw, false)
		select {
		case e := <-done:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("valid release did not unblock")
		}
	}
	if _, e := identityGate(directory, "r0001", nil); e == nil {
		t.Fatal("existing gate was reused")
	}
}
func TestIdentityGateCancellationAndReplay(t *testing.T) {
	for _, cancelled := range []bool{true, false} {
		directory := filepath.Join(t.TempDir(), "gate")
		observe, e := identityGate(directory, "r0001", nil)
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- observe(ctx, "ready", runner.ClockIdentity{}) }()
		raw := waitMarker(t, filepath.Join(directory, "ready.json"))
		if cancelled {
			cancel()
		} else {
			releaseMarker(t, directory, "ready", raw, true)
		}
		select {
		case e := <-done:
			if e == nil {
				t.Fatal("invalid release/cancellation accepted")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("gate did not terminate")
		}
	}
}
