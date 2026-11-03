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

import "runtime"

type ResourceSnapshot struct {
	CPUUsec    float64          `json:"cpuUsec"`
	MaxRSSKiB  int64            `json:"maxRssKiB"`
	Cgroup     map[string]int64 `json:"cgroup"`
	QuotaCores *float64         `json:"quotaCores"`
	CgroupPath string           `json:"cgroupPath"`
	HeapAlloc  uint64           `json:"heapAllocBytes"`
	HeapSys    uint64           `json:"heapSysBytes"`
	NumGC      uint32           `json:"numGc"`
	GCPauseNs  uint64           `json:"gcPauseNs"`
	Goroutines int              `json:"goroutines"`
}
type ResourceDelta struct {
	Contract                   string           `json:"contract"`
	GOMAXPROCS                 int              `json:"gomaxprocs"`
	Start                      ResourceSnapshot `json:"start"`
	End                        ResourceSnapshot `json:"end"`
	CPUFractionOfDeclaredQuota *float64         `json:"cpuFractionOfDeclaredQuota"`
	DeclaredQuotaCores         float64          `json:"declaredQuotaCores"`
	DeclaredMaxRSSBytes        int64            `json:"declaredMaxRssBytes"`
	ThrottledUsec              *int64           `json:"throttledUsec"`
	NumGC                      uint32           `json:"numGc"`
	GCPauseNs                  uint64           `json:"gcPauseNs"`
	NodeELUApplicable          bool             `json:"nodeEluApplicable"`
}

func resources() ResourceSnapshot {
	cpu, rss, stats, quota, path := platformResources()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return ResourceSnapshot{CPUUsec: cpu, MaxRSSKiB: rss, Cgroup: stats, QuotaCores: quota, CgroupPath: path, HeapAlloc: m.HeapAlloc, HeapSys: m.HeapSys, NumGC: m.NumGC, GCPauseNs: m.PauseTotalNs, Goroutines: runtime.NumGoroutine()}
}
func resourceDelta(a, b ResourceSnapshot, elapsed float64, e Executor) (ResourceDelta, *float64, *int64) {
	d := ResourceDelta{Contract: "go-process-cgroup-v1", GOMAXPROCS: runtime.GOMAXPROCS(0), Start: a, End: b, DeclaredQuotaCores: e.CPUQuotaCores, DeclaredMaxRSSBytes: e.MaxRSSBytes, NumGC: b.NumGC - a.NumGC, GCPauseNs: b.GCPauseNs - a.GCPauseNs}
	var cores *float64
	var rss *int64
	if a.CPUUsec >= 0 && b.CPUUsec >= a.CPUUsec && elapsed > 0 {
		c := (b.CPUUsec - a.CPUUsec) / (elapsed * 1000)
		cores = &c
		f := c / e.CPUQuotaCores
		d.CPUFractionOfDeclaredQuota = &f
	}
	if b.MaxRSSKiB >= 0 {
		rss = &b.MaxRSSKiB
	}
	x, xok := a.Cgroup["throttled_usec"]
	y, yok := b.Cgroup["throttled_usec"]
	if xok && yok && y >= x {
		z := y - x
		d.ThrottledUsec = &z
	}
	return d, cores, rss
}
