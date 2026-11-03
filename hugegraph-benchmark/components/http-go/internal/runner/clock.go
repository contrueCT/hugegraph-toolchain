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
	"os"
	"runtime"
	"time"
)

type Boundary struct {
	SinceOriginMs float64 `json:"sinceOriginMs"`
	BeforeNs      *int64  `json:"beforeNs,string"`
	AfterNs       *int64  `json:"afterNs,string"`
	Error         string  `json:"error,omitempty"`
}
type PhaseClock struct {
	Contract string   `json:"contract"`
	Start    Boundary `json:"start"`
	End      Boundary `json:"end"`
}
type ClockIdentity struct {
	Contract                    string         `json:"contract"`
	GOOS                        string         `json:"goos"`
	GOARCH                      string         `json:"goarch"`
	GoVersion                   string         `json:"goVersion"`
	PID                         int            `json:"pid"`
	Process                     map[string]any `json:"process"`
	Origin                      Boundary       `json:"origin"`
	ExternalCorrelationAccepted bool           `json:"externalCorrelationAccepted"`
}
type clock struct {
	origin   time.Time
	identity ClockIdentity
}

func newClock() *clock {
	before, e1 := monotonicNs()
	now := time.Now()
	after, e2 := monotonicNs()
	c := &clock{origin: now}
	b := Boundary{}
	if e1 == nil && e2 == nil {
		b.BeforeNs = &before
		b.AfterNs = &after
	} else {
		b.Error = "raw Linux CLOCK_MONOTONIC unavailable"
	}
	c.identity = ClockIdentity{Contract: "go-monotonic-bracket-v1", GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(), PID: os.Getpid(), Process: processIdentity(), Origin: b}
	return c
}
func (c *clock) boundary() (Boundary, time.Time) {
	before, e1 := monotonicNs()
	now := time.Now()
	after, e2 := monotonicNs()
	b := Boundary{SinceOriginMs: float64(now.Sub(c.origin)) / 1e6}
	if e1 == nil && e2 == nil {
		b.BeforeNs = &before
		b.AfterNs = &after
	} else {
		b.Error = "raw Linux CLOCK_MONOTONIC unavailable"
	}
	return b, now
}
