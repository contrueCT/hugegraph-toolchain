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
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/apache/hugegraph-toolchain/hugegraph-benchmark/components/http-go/internal/runner"
)

var sourceSHA256 = "unregistered"

func identity() (map[string]any, error) {
	p, e := os.Executable()
	if e != nil {
		return nil, e
	}
	f, e := os.Open(p)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || info.Size() > 32*1024*1024 {
		return nil, errors.New("binary identity bound exceeded")
	}
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return nil, e
	}
	settings := map[string]string{}
	if b, ok := debug.ReadBuildInfo(); ok {
		for _, s := range b.Settings {
			settings[s.Key] = s.Value
		}
	}
	return map[string]any{"schemaVersion": 1, "kind": "hgbench-http-go-identity", "engine": runner.Engine, "sourceSha256": sourceSHA256, "requestSchemaVersion": 2, "rawSchemaVersion": 2, "goVersion": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "binarySha256": hex.EncodeToString(h.Sum(nil)), "bytes": info.Size(), "buildSettings": settings}, nil
}
func writeJSON(path string, v any) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	encoder := json.NewEncoder(f)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	e = encoder.Encode(v)
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e == nil {
		e = closeErr
	}
	return e
}
func execute() error {
	if len(os.Args) == 1 || os.Args[1] == "--help" || os.Args[1] == "help" {
		fmt.Println(`HG-Bench Go HTTP execution component (go-http-v1)

Usage:
  hgbench-http-go version                 Print JSON binary, build and protocol identity
  hgbench-http-go clock-probe             Bounded stdin/stdout clock exchange; no HTTP
  hgbench-http-go run --request FILE --trace FILE --output FILE [--identity-gate NEW-DIRECTORY]

The TS hgbench CLI freezes plans and manages task ownership. This component
executes one frozen request; it never downloads, builds, retries or discovers a target.
Request protocol: 2. Output/raw protocol: 2. Output and trace paths must be new.
SIGINT/SIGTERM cancel admission and active HTTP, preserve planned arrivals,
then finish the byte-bounded trace writer. Failure exits nonzero and retains partial files.
Environment: authorizationEnv supplies credentials; GODEBUG must be empty.
Optional identity gate waits for exact host releases before/after phases (10s each).
GC percent=100 and Go memory limit=off; external CPU/RSS limits require proof.
Reports are descriptive until the new engine passes its independent calibration.`)
		return nil
	}
	meta, e := identity()
	if e != nil {
		return e
	}
	if os.Args[1] == "version" {
		return json.NewEncoder(os.Stdout).Encode(meta)
	}
	if os.Args[1] == "clock-probe" {
		if len(os.Args) != 2 || os.Getenv("GODEBUG") != "" {
			return errors.New("clock-probe takes no arguments and requires empty GODEBUG")
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		// File.Close can wait behind a blocking stdin Read on some platforms.
		// This command owns the process, so return on cancellation independently.
		timer := time.AfterFunc(30*time.Second, stop)
		defer timer.Stop()
		completed := make(chan error, 1)
		go func() { completed <- runner.ClockProbe(ctx, os.Stdin, os.Stdout, meta) }()
		select {
		case e := <-completed:
			return e
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if os.Args[1] != "run" {
		return errors.New("unknown command; use --help")
	}
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	request := flags.String("request", "", "frozen request JSON (max 4 MiB)")
	trace := flags.String("trace", "", "create-only NDJSON trace path")
	output := flags.String("output", "", "create-only JSON result path")
	gate := flags.String("identity-gate", "", "new live-process marker directory; host releases required within 10s per stage")
	if e = flags.Parse(os.Args[2:]); e != nil {
		return e
	}
	if flags.NArg() != 0 || *request == "" || *trace == "" || *output == "" || *trace == *output {
		return errors.New("run requires distinct --request, --trace and --output paths")
	}
	for _, p := range []string{*trace, *output} {
		if _, e = os.Lstat(p); !errors.Is(e, os.ErrNotExist) {
			return errors.New("output paths must be new")
		}
	}
	f, e := os.Open(*request)
	if e != nil {
		return e
	}
	raw, e := io.ReadAll(io.LimitReader(f, 4*1024*1024+1))
	f.Close()
	if e != nil || len(raw) > 4*1024*1024 {
		return errors.New("request exceeds 4 MiB bound")
	}
	var in runner.Input
	if e = json.Unmarshal(raw, &in); e != nil {
		return errors.New("invalid request JSON")
	}
	if in.Profile.Executor.Binary.SourceSHA256 != sourceSHA256 || in.Profile.Executor.Binary.SHA256 != meta["binarySha256"] || in.Profile.Executor.Binary.GoVersion != runtime.Version() || in.Profile.Executor.Binary.GOOS != runtime.GOOS || in.Profile.Executor.Binary.GOARCH != runtime.GOARCH {
		return errors.New("running binary identity differs from frozen executor")
	}
	if os.Getenv("GODEBUG") != "" {
		return errors.New("GODEBUG must be unset/empty for frozen HTTP policy")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var observe func(context.Context, string, runner.ClockIdentity) error
	if *gate != "" {
		observe, e = identityGate(*gate, in.ID, meta)
		if e != nil {
			return e
		}
	}
	result, runErr := runner.RunObserved(ctx, in, *trace, observe)
	if result.Client != nil {
		result.Client["binarySha256"] = meta["binarySha256"]
		result.Client["buildSettings"] = meta["buildSettings"]
	}
	if runErr != nil {
		failure := map[string]any{"schemaVersion": 2, "kind": "http-go-run-failure", "engine": runner.Engine, "sourceSha256": sourceSHA256, "id": in.ID, "partial": true, "error": runErr.Error(), "result": result, "performanceAccepted": false}
		if e = writeJSON(*output, failure); e != nil {
			return fmt.Errorf("run failed and failure receipt could not persist: %w", e)
		}
		return runErr
	}
	if e = writeJSON(*output, result); e != nil {
		return e
	}
	if ctx.Err() != nil {
		return errors.New("cancelled during output finalization")
	}
	return nil
}
func main() {
	if e := execute(); e != nil {
		fmt.Fprintln(os.Stderr, "HGBENCH_GO_ERROR:", e)
		os.Exit(1)
	}
}
