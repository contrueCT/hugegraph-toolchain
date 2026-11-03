//go:build linux

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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

func monotonicNs() (int64, error) {
	var ts syscall.Timespec
	_, _, e := syscall.Syscall(syscall.SYS_CLOCK_GETTIME, 1, uintptr(unsafe.Pointer(&ts)), 0)
	if e != 0 {
		return 0, e
	}
	return ts.Sec*1e9 + ts.Nsec, nil
}
func processIdentity() map[string]any {
	m := map[string]any{}
	for k, p := range map[string]string{"bootId": "/proc/sys/kernel/random/boot_id", "timeOffsets": "/proc/self/timens_offsets", "cgroup": "/proc/self/cgroup"} {
		if b, e := os.ReadFile(p); e == nil {
			m[k] = strings.TrimSpace(string(b))
		}
	}
	if b, e := os.ReadFile("/proc/self/stat"); e == nil {
		f := strings.Fields(string(b)[strings.LastIndex(string(b), ")")+1:])
		if len(f) > 19 {
			m["startTicks"] = f[19]
		}
	}
	for _, n := range []string{"pid", "mnt", "time", "time_for_children", "cgroup"} {
		if s, e := os.Readlink("/proc/self/ns/" + n); e == nil {
			m[n+"Namespace"] = s
		}
	}
	return m
}
func platformResources() (float64, int64, map[string]int64, *float64, string) {
	var u syscall.Rusage
	cpu := float64(-1)
	rss := int64(-1)
	if syscall.Getrusage(syscall.RUSAGE_SELF, &u) == nil {
		cpu = float64(u.Utime.Sec+u.Stime.Sec)*1e6 + float64(u.Utime.Usec+u.Stime.Usec)
		rss = u.Maxrss
	}
	var quota *float64
	stats := map[string]int64{}
	root := ""
	b, e := os.ReadFile("/proc/self/cgroup")
	if e == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "0::/") {
				relative := strings.TrimPrefix(line, "0::")
				candidate := filepath.Join("/sys/fs/cgroup", relative)
				if _, e = os.Stat(filepath.Join(candidate, "cpu.stat")); e == nil {
					root = candidate
				} else if _, e = os.Stat("/sys/fs/cgroup/cpu.stat"); e == nil {
					root = "/sys/fs/cgroup"
				}
			}
		}
	}
	if root != "" {
		if b, e = os.ReadFile(filepath.Join(root, "cpu.stat")); e == nil {
			for _, line := range strings.Split(string(b), "\n") {
				var k string
				var v int64
				if _, e = fmt.Sscan(line, &k, &v); e == nil {
					stats[k] = v
				}
			}
		}
		if b, e = os.ReadFile(filepath.Join(root, "cpu.max")); e == nil {
			f := strings.Fields(string(b))
			if len(f) == 2 && f[0] != "max" {
				a, e1 := strconv.ParseFloat(f[0], 64)
				z, e2 := strconv.ParseFloat(f[1], 64)
				if e1 == nil && e2 == nil && z > 0 {
					v := a / z
					quota = &v
				}
			}
		}
		if b, e = os.ReadFile(filepath.Join(root, "memory.current")); e == nil {
			if v, e := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); e == nil {
				stats["memory.current"] = v
			}
		}
	}
	return cpu, rss, stats, quota, root
}
