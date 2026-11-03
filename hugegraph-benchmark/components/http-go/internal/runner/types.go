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

import "encoding/json"

const Engine = "go-http-v1"
const RawVersion = 2

type Check struct {
	Pointer         json.RawMessage `json:"pointer"`
	Equals          json.RawMessage `json:"equals,omitempty"`
	CanonicalSHA256 string          `json:"canonicalSha256,omitempty"`
}
type GraphRule struct {
	Kind     string      `json:"kind"`
	Pointer  string      `json:"pointer"`
	ID       string      `json:"id"`
	Label    string      `json:"label"`
	Absent   bool        `json:"absent"`
	Allowed  []string    `json:"allowed"`
	Count    int         `json:"count"`
	Source   string      `json:"source"`
	Target   string      `json:"target"`
	Distance int         `json:"distance"`
	Edges    [][2]string `json:"edges"`
}
type GraphRequest struct {
	ID             string          `json:"id"`
	Stratum        string          `json:"stratum"`
	Method         string          `json:"method"`
	URL            string          `json:"url"`
	Body           json.RawMessage `json:"body"`
	ExpectedStatus int             `json:"expectedStatus"`
	Rule           GraphRule       `json:"rule"`
}
type Case struct {
	RequestPool    []GraphRequest  `json:"requestPool,omitempty"`
	ID             string          `json:"id"`
	Body           json.RawMessage `json:"body"`
	ExpectedStatus int             `json:"expectedStatus"`
	Checks         []Check         `json:"checks"`
}
type Load struct {
	ID              string  `json:"id"`
	Model           string  `json:"model"`
	Concurrency     int     `json:"concurrency,omitempty"`
	Rate            float64 `json:"ratePerSecond,omitempty"`
	MaxInFlight     int     `json:"maxInFlight,omitempty"`
	MaxStartDelayMs float64 `json:"maxStartDelayMs,omitempty"`
}
type Binary struct {
	SourceSHA256 string `json:"sourceSha256"`
	Path         string `json:"path"`
	SHA256       string `json:"sha256"`
	GoVersion    string `json:"goVersion"`
	GOOS         string `json:"goos"`
	GOARCH       string `json:"goarch"`
}
type Executor struct {
	Engine          string  `json:"engine"`
	Binary          Binary  `json:"binary"`
	GOMAXPROCS      int     `json:"gomaxprocs"`
	MaxPendingBytes int     `json:"maxPendingBytes"`
	CPUQuotaCores   float64 `json:"cpuQuotaCores"`
	MaxRSSBytes     int64   `json:"maxRssBytes"`
}
type Profile struct {
	SchemaVersion    int      `json:"schemaVersion"`
	Kind             string   `json:"kind"`
	Name             string   `json:"name"`
	Endpoint         string   `json:"endpoint"`
	ResponseEncoding string   `json:"responseEncoding"`
	AuthorizationEnv *string  `json:"authorizationEnv"`
	Executor         Executor `json:"executor"`
	WarmupMs         float64  `json:"warmupMs"`
	DurationMs       float64  `json:"durationMs"`
	RequestTimeoutMs float64  `json:"requestTimeoutMs"`
	DrainMs          float64  `json:"drainMs"`
	MaxRequests      int      `json:"maxRequestsPerPhase"`
	MaxResponseBytes int64    `json:"maxResponseBytes"`
	MaxTraceBytes    int64    `json:"maxTraceBytes"`
}
type Input struct {
	SchemaVersion       int     `json:"schemaVersion"`
	ID                  string  `json:"id"`
	Profile             Profile `json:"profile"`
	Case                Case    `json:"case"`
	Load                Load    `json:"load"`
	Repeat              int     `json:"repeat"`
	RemainingTraceBytes int64   `json:"remainingTraceBytes"`
	ProbePhase          string  `json:"probePhase,omitempty"`
}
type CheckDigest struct {
	Present bool    `json:"present"`
	SHA256  *string `json:"sha256"`
}
type Record struct {
	ParameterIndex   *int            `json:"parameterIndex,omitempty"`
	GraphValue       json.RawMessage `json:"graphValue,omitempty"`
	SchemaVersion    int             `json:"schemaVersion"`
	Engine           string          `json:"engine"`
	Phase            string          `json:"phase"`
	Sequence         int             `json:"sequence"`
	ScheduledMs      float64         `json:"scheduledMs"`
	SentMs           *float64        `json:"sentMs"`
	SocketMs         *float64        `json:"socketMs"`
	EndMs            *float64        `json:"endMs"`
	ObservedMs       *float64        `json:"observedMs,omitempty"`
	ValidationMs     *float64        `json:"validationMs"`
	DecodeMs         *float64        `json:"decodeMs"`
	DecodedBytes     *int            `json:"decodedBytes"`
	ResponseEncoding *string         `json:"responseEncoding"`
	Bytes            int             `json:"bytes"`
	Status           *int            `json:"status"`
	BodySHA256       *string         `json:"bodySha256"`
	CheckDigests     []CheckDigest   `json:"checkDigests"`
	CompleteResponse bool            `json:"completeResponse"`
	ReusedSocket     *bool           `json:"reusedSocket"`
	Late             bool            `json:"late"`
	Outcome          string          `json:"outcome"`
}
type Phase struct {
	Name                 string         `json:"name"`
	DurationMs           float64        `json:"durationMs"`
	ElapsedMs            float64        `json:"elapsedMs"`
	Scheduled            int            `json:"scheduled"`
	Sent                 int            `json:"sent"`
	Completed            int            `json:"completed"`
	Dropped              int            `json:"dropped"`
	Failed               int            `json:"failed"`
	Late                 int            `json:"late"`
	Counts               map[string]int `json:"counts"`
	BudgetExhausted      bool           `json:"budgetExhausted"`
	Cancelled            bool           `json:"cancelled"`
	AverageCPUCores      *float64       `json:"averageCpuCores"`
	EventLoopUtilization *float64       `json:"eventLoopUtilization"`
	EventLoopDelayMs     any            `json:"eventLoopDelayMs"`
	MaxRSSKiB            *int64         `json:"maxRssKiB"`
	Resources            ResourceDelta  `json:"resources"`
	Clock                PhaseClock     `json:"clock"`
}
type Result struct {
	SchemaVersion        int            `json:"schemaVersion"`
	Kind                 string         `json:"kind"`
	Engine               string         `json:"engine"`
	ID                   string         `json:"id"`
	CaseID               string         `json:"caseId"`
	LoadID               string         `json:"loadId"`
	Repeat               int            `json:"repeat"`
	StartedAt            string         `json:"startedAt"`
	FinishedAt           string         `json:"finishedAt"`
	Phases               []Phase        `json:"phases"`
	TraceBytes           int64          `json:"traceBytes"`
	TraceWriter          WriterReceipt  `json:"traceWriter"`
	PhaseClock           ClockIdentity  `json:"phaseClock"`
	ProcessIdentityAfter map[string]any `json:"processIdentityAfter,omitempty"`
	Client               map[string]any `json:"client"`
	PerformanceAccepted  bool           `json:"performanceAccepted"`
}
