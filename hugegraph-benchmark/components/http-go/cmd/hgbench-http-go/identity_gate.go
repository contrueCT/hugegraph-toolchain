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
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/apache/hugegraph-toolchain/hugegraph-benchmark/components/http-go/internal/runner"
)

// Each marker becomes visible only after its complete bytes have been synced.
func marker(path string, value any) error {
	temporary := path + ".tmp"
	if e := writeJSON(temporary, value); e != nil {
		return e
	}
	defer os.Remove(temporary)
	return os.Link(temporary, path)
}

func identityGate(directory, id string, binary map[string]any) (func(context.Context, string, runner.ClockIdentity) error, error) {
	if e := os.Mkdir(directory, 0700); e != nil {
		return nil, e
	}
	directory, e := filepath.Abs(directory)
	if e != nil {
		return nil, e
	}
	return func(ctx context.Context, stage string, clock runner.ClockIdentity) error {
		if stage != "ready" && stage != "finished" {
			return errors.New("invalid identity gate stage")
		}
		nonce := make([]byte, 16)
		if _, e := rand.Read(nonce); e != nil {
			return e
		}
		token := hex.EncodeToString(nonce)
		path := filepath.Join(directory, stage+".json")
		value := map[string]any{"schemaVersion": 1, "kind": "http-go-live-process-marker", "stage": stage, "id": id, "nonce": token, "binary": binary, "clock": clock, "performanceAccepted": false}
		if e := marker(path, value); e != nil {
			return e
		}
		raw, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		sum := sha256.Sum256(raw)
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		release := filepath.Join(directory, stage+"-release.json")
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline.C:
				return errors.New("live process attestation deadline expired")
			case <-tick.C:
				info, e := os.Lstat(release)
				if errors.Is(e, os.ErrNotExist) {
					continue
				}
				if e != nil {
					return e
				}
				if !info.Mode().IsRegular() || info.Size() > 4096 {
					return errors.New("identity release must be a regular file <=4096 bytes")
				}
				f, e := os.OpenFile(release, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
				if e != nil {
					return e
				}
				actual, e := f.Stat()
				if e != nil || !actual.Mode().IsRegular() {
					f.Close()
					return errors.New("identity release changed file type")
				}
				raw, e := io.ReadAll(io.LimitReader(f, 4097))
				f.Close()
				if e != nil {
					return e
				}
				if len(raw) > 4096 {
					return errors.New("identity release exceeds bound")
				}
				var ack struct {
					SchemaVersion int    `json:"schemaVersion"`
					Kind          string `json:"kind"`
					ID            string `json:"id"`
					Stage         string `json:"stage"`
					Nonce         string `json:"nonce"`
					MarkerSHA256  string `json:"markerSha256"`
				}
				decoder := json.NewDecoder(bytes.NewReader(raw))
				decoder.DisallowUnknownFields()
				if e := decoder.Decode(&ack); e != nil {
					return e
				}
				if e := decoder.Decode(new(any)); e != io.EOF {
					return errors.New("identity release has trailing data")
				}
				if ack.SchemaVersion != 1 || ack.Kind != "http-go-live-process-release" || ack.ID != id || ack.Stage != stage || ack.Nonce != token || ack.MarkerSHA256 != hex.EncodeToString(sum[:]) {
					return fmt.Errorf("identity release does not match %s marker", stage)
				}
				return nil
			}
		}
	}, nil
}
