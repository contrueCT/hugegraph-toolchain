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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type object = map[string]any
type verificationError struct{ Kind, Message string }

func (e verificationError) Error() string { return e.Message }
func need(ok bool, message string) {
	if !ok {
		panic(verificationError{"ValueError", message})
	}
}
func checked(err error, operation string) {
	if err != nil {
		panic(verificationError{"IOError", operation})
	}
}
func obj(v any) object  { x, ok := v.(map[string]any); need(ok, "Expected object"); return x }
func array(v any) []any { x, ok := v.([]any); need(ok, "Expected array"); return x }
func str(v any) string  { x, e := text(v); checked(e, "Expected nonempty UTF-8 text"); return x }
func decimal(v any) uint64 {
	s, ok := v.(string)
	need(ok && s != "", "Expected canonical decimal count")
	n, e := strconv.ParseUint(s, 10, 64)
	need(e == nil && strconv.FormatUint(n, 10) == s, "Expected canonical decimal count")
	return n
}
func intValue(v any) int64 { n, e := number(v); checked(e, "Expected exact signed long"); return n }
func nonnegative(v any) uint64 {
	n := intValue(v)
	need(n >= 0, "Expected nonnegative inventory count")
	return uint64(n)
}
func digest(raw []byte) string { x := sha256.Sum256(raw); return hex.EncodeToString(x[:]) }
func decode(raw []byte) any    { v, e := strict(raw); checked(e, "Invalid strict JSON"); return v }
func shape(v object, fields ...string) bool {
	if len(v) != len(fields) {
		return false
	}
	for _, k := range fields {
		if _, ok := v[k]; !ok {
			return false
		}
	}
	return true
}
func canonical(out []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(out, "null"...)
	case bool:
		return strconv.AppendBool(out, x)
	case string:
		return quote(out, x)
	case json.Number:
		return append(out, string(x)...)
	case []any:
		out = append(out, '[')
		for i, e := range x {
			if i > 0 {
				out = append(out, ',')
			}
			out = canonical(out, e)
		}
		return append(out, ']')
	case map[string]any:
		names := make([]string, 0, len(x))
		for k := range x {
			names = append(names, k)
		}
		sort.Strings(names)
		out = append(out, '{')
		for i, k := range names {
			if i > 0 {
				out = append(out, ',')
			}
			out = quote(out, k)
			out = append(out, ':')
			out = canonical(out, x[k])
		}
		return append(out, '}')
	default:
		panic(verificationError{"ValueError", "Unsupported JSON value"})
	}
}
func encoded(v any) []byte {
	raw, e := json.Marshal(v)
	checked(e, "Cannot encode JSON")
	return canonical(nil, decode(raw))
}
func same(a, b any) bool { return string(encoded(a)) == string(encoded(b)) }

// SameFile covers device/inode; size, mode, modification and platform stat cover
// replacements and in-place changes. Symlinks are refused before every read.
type identity struct{ info os.FileInfo }

func identify(path string) identity {
	s, e := os.Lstat(path)
	checked(e, "Cannot stat immutable file")
	need(s.Mode().IsRegular(), "Expected a regular immutable file")
	return identity{s}
}
func (a identity) equal(b identity) bool {
	return os.SameFile(a.info, b.info) && a.info.Size() == b.info.Size() && a.info.Mode() == b.info.Mode() &&
		a.info.ModTime() == b.info.ModTime() && reflect.DeepEqual(changeTime(a.info), changeTime(b.info))
}
func changeTime(info os.FileInfo) any {
	v := reflect.ValueOf(info.Sys())
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	for _, name := range []string{"Ctim", "Ctimespec"} {
		x := v.FieldByName(name)
		if x.IsValid() {
			return x.Interface()
		}
	}
	panic(verificationError{"ValueError", "File change time unavailable on this platform"})
}
func readJSON(path string, limit int64) (object, []byte) {
	before := identify(path)
	need(before.info.Size() <= limit, "Metadata byte budget exceeded")
	raw, e := os.ReadFile(path)
	checked(e, "Cannot read metadata")
	need(int64(len(raw)) == before.info.Size() && before.equal(identify(path)), "Metadata changed while reading")
	return obj(decode(raw)), raw
}
func fileFact(path string, guard func()) object {
	guard()
	before := identify(path)
	f, e := os.Open(path)
	checked(e, "Cannot open evidence")
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for {
		guard()
		n, e := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if e == io.EOF {
			break
		}
		checked(e, "Cannot hash evidence")
	}
	need(before.equal(identify(path)), "Evidence changed while hashing")
	return object{"path": filepath.Base(path), "bytes": strconv.FormatInt(before.info.Size(), 10), "sha256": hex.EncodeToString(h.Sum(nil))}
}
func bytesOf(v uint64) string { return strconv.FormatUint(v, 10) }
func safePath(base, relative string) string {
	need(relative != "" && !filepath.IsAbs(relative), "Unsafe shard path")
	for _, part := range strings.Split(filepath.ToSlash(relative), "/") {
		need(part != "..", "Unsafe shard path")
	}
	p := filepath.Join(base, relative)
	resolved, e := filepath.EvalSymlinks(p)
	checked(e, "Cannot resolve source")
	root, e := filepath.EvalSymlinks(base)
	checked(e, "Cannot resolve source root")
	rel, e := filepath.Rel(root, resolved)
	need(e == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)), "Shard escaped prepared input root")
	return p
}
func recoverError(caught any) object {
	if e, ok := caught.(httpFailure); ok {
		return object{"type": "ValueError", "message": "HTTP status " + fmtCountInt(e.status), "http": e.details}
	}
	if e, ok := caught.(verificationError); ok {
		return object{"type": e.Kind, "message": e.Message}
	}
	// Do not serialize panic values, response bodies, paths, or authorization.
	return object{"type": "InternalError", "message": fmt.Sprintf("Unexpected verifier failure (%T)", caught)}
}
