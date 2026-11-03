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
	"io"
	"os"
	"sync"
	"time"
)

type WriterReceipt struct {
	Mode                      string  `json:"mode"`
	MaximumPendingBytes       int64   `json:"maximumPendingBytes"`
	PeakPendingBytes          int64   `json:"peakPendingBytes"`
	EnqueuedBytes             int64   `json:"enqueuedBytes"`
	PersistedBytes            int64   `json:"persistedBytes"`
	Records                   int     `json:"records"`
	WriteCalls                int     `json:"writeCalls"`
	SerializationAndEnqueueMs float64 `json:"serializationAndEnqueueMs"`
	AsynchronousWriteWaitMs   float64 `json:"asynchronousWriteWaitMs"`
	FinalFlushWaitMs          float64 `json:"finalFlushWaitMs"`
	FinalSyncMs               float64 `json:"finalSyncMs"`
	Complete                  bool    `json:"complete"`
}
type syncFile interface {
	io.Writer
	Sync() error
	Close() error
}
type TraceWriter struct {
	mu        sync.Mutex
	cond      *sync.Cond
	file      syncFile
	queue     [][]byte
	pending   int64
	limit     int64
	accepting bool
	err       error
	done      chan struct{}
	receipt   WriterReceipt
	failure   func(error)
}

func OpenWriter(path string, limit int64, pending int, failure func(error)) (*TraceWriter, error) {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, e
	}
	return newWriter(f, limit, pending, failure), nil
}
func newWriter(f syncFile, limit int64, pending int, failure func(error)) *TraceWriter {
	cap := min(int64(pending), limit)
	w := &TraceWriter{file: f, limit: limit, accepting: true, done: make(chan struct{}), failure: failure, receipt: WriterReceipt{Mode: "go-async-byte-bounded-v1", MaximumPendingBytes: cap}}
	w.cond = sync.NewCond(&w.mu)
	go w.drain()
	return w
}
func (w *TraceWriter) failLocked(e error) bool {
	if w.err != nil {
		return false
	}
	w.err = e
	w.accepting = false
	w.queue = nil
	w.cond.Broadcast()
	return true
}
func (w *TraceWriter) fail(e error) {
	w.mu.Lock()
	fresh := w.failLocked(e)
	w.mu.Unlock()
	if fresh && w.failure != nil {
		w.failure(e)
	}
}
func (w *TraceWriter) Append(record Record) error {
	start := time.Now()
	b, e := json.Marshal(record)
	b = append(b, '\n')
	if e != nil {
		w.fail(e)
		return e
	}
	w.mu.Lock()
	fresh := false
	if w.err != nil {
		e = w.err
	} else if !w.accepting {
		e = errors.New("trace writer already finishing")
	} else if len(b) >= 65536 {
		e = errors.New("trace line exceeds bound")
	} else if w.receipt.EnqueuedBytes+int64(len(b)) > w.limit {
		e = errors.New("trace byte budget exhausted")
	} else if w.pending+int64(len(b)) > w.receipt.MaximumPendingBytes {
		e = errors.New("pending trace byte budget exhausted")
	}
	if e != nil {
		fresh = w.failLocked(e)
	} else {
		w.queue = append(w.queue, b)
		w.pending += int64(len(b))
		w.receipt.EnqueuedBytes += int64(len(b))
		w.receipt.Records++
		w.receipt.PeakPendingBytes = max(w.receipt.PeakPendingBytes, w.pending)
		w.cond.Signal()
	}
	w.receipt.SerializationAndEnqueueMs += float64(time.Since(start)) / 1e6
	w.mu.Unlock()
	if fresh && w.failure != nil {
		w.failure(e)
	}
	return e
}
func (w *TraceWriter) drain() {
	defer close(w.done)
	for {
		w.mu.Lock()
		for len(w.queue) == 0 && w.accepting && w.err == nil {
			w.cond.Wait()
		}
		if w.err != nil || len(w.queue) == 0 && !w.accepting {
			w.mu.Unlock()
			return
		}
		b := w.queue[0]
		w.queue[0] = nil
		w.queue = w.queue[1:]
		w.mu.Unlock()
		original := len(b)
		for len(b) > 0 {
			start := time.Now()
			n, e := w.file.Write(b)
			elapsed := float64(time.Since(start)) / 1e6
			if n < 0 || n > len(b) {
				n = 0
				e = io.ErrShortWrite
			}
			if e == nil && n == 0 {
				e = io.ErrShortWrite
			}
			w.mu.Lock()
			w.receipt.WriteCalls++
			w.receipt.PersistedBytes += int64(n)
			w.receipt.AsynchronousWriteWaitMs += elapsed
			w.mu.Unlock()
			if e != nil {
				w.fail(e)
				return
			}
			b = b[n:]
		}
		w.mu.Lock()
		w.pending -= int64(original)
		w.mu.Unlock()
	}
}
func (w *TraceWriter) Finish() (WriterReceipt, error) {
	w.mu.Lock()
	w.accepting = false
	w.cond.Broadcast()
	w.mu.Unlock()
	start := time.Now()
	<-w.done
	w.mu.Lock()
	w.receipt.FinalFlushWaitMs += float64(time.Since(start)) / 1e6
	e := w.err
	w.mu.Unlock()
	if e == nil {
		start = time.Now()
		e = w.file.Sync()
		w.mu.Lock()
		w.receipt.FinalSyncMs = float64(time.Since(start)) / 1e6
		w.mu.Unlock()
	}
	closeErr := w.file.Close()
	if e == nil {
		e = closeErr
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if e == nil && w.pending == 0 && w.receipt.EnqueuedBytes == w.receipt.PersistedBytes {
		w.receipt.Complete = true
	} else if e == nil {
		e = errors.New("trace byte conservation failed")
	}
	return w.receipt, e
}
