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

// Package compat implements the existing JS valueDigest contract. It deliberately
// preserves UTF-16 code units, duplicate-key last wins and binary64 numbers.
package compat

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

const MaxDepth = 256

type Value struct {
	kind   byte
	text   []uint16
	ascii  string
	number float64
	array  []*Value
	object map[string]*member
}
type member struct {
	key   []uint16
	value *Value
}
type parser struct {
	raw  []byte
	at   int
	keys map[string]internedKey
}

// Repeated short plain-ASCII object keys share owned, immutable decoded units.
// The per-parse cache is capped; uncached/escaped/Unicode keys use the decoder.
type internedKey struct {
	units []uint16
	id    string
}

func (p *parser) keyValue() ([]uint16, string, error) {
	start := p.at
	if start < len(p.raw) && p.raw[start] == '"' {
		for end := start + 1; end < len(p.raw) && end-start <= 129; end++ {
			c := p.raw[end]
			if c == '"' {
				literal := p.raw[start+1 : end]
				if key, ok := p.keys[string(literal)]; ok {
					p.at = end + 1
					return key.units, key.id, nil
				}
				key, _, err := p.textValue(false)
				if err != nil {
					return nil, "", err
				}
				id := keyID(key)
				if len(p.keys) < 64 {
					if p.keys == nil {
						p.keys = make(map[string]internedKey)
					}
					p.keys[string(literal)] = internedKey{units: key, id: id}
				}
				return key, id, nil
			}
			if c < 0x20 || c >= 0x80 || c == '\\' {
				break
			}
		}
	}
	key, _, err := p.textValue(false)
	if err != nil {
		return nil, "", err
	}
	return key, keyID(key), nil
}

var invalid = errors.New("invalid JSON or nesting exceeds 256")

func Parse(raw []byte) (*Value, error) {
	if !utf8.Valid(raw) {
		return nil, invalid
	}
	p := parser{raw: raw}
	v, e := p.value(0)
	p.space()
	if e != nil || p.at != len(raw) {
		return nil, invalid
	}
	return v, nil
}
func (p *parser) space() {
	for p.at < len(p.raw) && strings.ContainsRune(" \t\r\n", rune(p.raw[p.at])) {
		p.at++
	}
}
func (p *parser) take(c byte) bool {
	if p.at < len(p.raw) && p.raw[p.at] == c {
		p.at++
		return true
	}
	return false
}
func keyID(v []uint16) string {
	b := make([]byte, len(v)*2)
	for i, u := range v {
		b[2*i] = byte(u >> 8)
		b[2*i+1] = byte(u)
	}
	return string(b)
}
func (p *parser) textValue(allowASCII bool) ([]uint16, string, error) {
	if !p.take('"') {
		return nil, "", invalid
	}
	// Copy an ordinary ASCII prefix in one allocation. The existing decoder
	// handles escapes, Unicode and invalid suffixes without changing code units.
	start := p.at
	for p.at < len(p.raw) {
		c := p.raw[p.at]
		if c < 0x20 || c >= 0x80 || c == '"' || c == '\\' {
			break
		}
		p.at++
	}
	// Plain ASCII needs no escaping or UTF-16 transformation when emitted.
	// Keep object keys in UTF-16; only complete long string values use this
	// owned copy. Unicode, escapes and invalid suffixes use the decoder below.
	if allowASCII && p.at-start >= 64 && p.at < len(p.raw) && p.raw[p.at] == '"' {
		ascii := string(p.raw[start:p.at])
		p.at++
		return nil, ascii, nil
	}
	capacity := p.at - start
	if capacity < 16 {
		capacity = 16
	}
	out := make([]uint16, p.at-start, capacity)
	for i, c := range p.raw[start:p.at] {
		out[i] = uint16(c)
	}
	for p.at < len(p.raw) {
		c := p.raw[p.at]
		p.at++
		if c == '"' {
			return out, "", nil
		}
		if c < 0x20 {
			return nil, "", invalid
		}
		if c == '\\' {
			if p.at >= len(p.raw) {
				return nil, "", invalid
			}
			e := p.raw[p.at]
			p.at++
			switch e {
			case '"', '\\', '/':
				out = append(out, uint16(e))
			case 'b':
				out = append(out, 8)
			case 'f':
				out = append(out, 12)
			case 'n':
				out = append(out, 10)
			case 'r':
				out = append(out, 13)
			case 't':
				out = append(out, 9)
			case 'u':
				if p.at+4 > len(p.raw) {
					return nil, "", invalid
				}
				u, err := strconv.ParseUint(string(p.raw[p.at:p.at+4]), 16, 16)
				if err != nil {
					return nil, "", invalid
				}
				out = append(out, uint16(u))
				p.at += 4
			default:
				return nil, "", invalid
			}
			continue
		}
		if c < 0x80 {
			out = append(out, uint16(c))
			continue
		}
		p.at--
		r, n := utf8.DecodeRune(p.raw[p.at:])
		p.at += n
		if r <= 0xffff {
			out = append(out, uint16(r))
		} else {
			a, b := utf16.EncodeRune(r)
			out = append(out, uint16(a), uint16(b))
		}
	}
	return nil, "", invalid
}
func (p *parser) value(depth int) (*Value, error) {
	p.space()
	if depth > MaxDepth || p.at >= len(p.raw) {
		return nil, invalid
	}
	c := p.raw[p.at]
	v := &Value{kind: c}
	switch c {
	case '"':
		s, ascii, e := p.textValue(true)
		v.text, v.ascii = s, ascii
		return v, e
	case '[':
		p.at++
		p.space()
		v.array = make([]*Value, 0)
		if p.take(']') {
			return v, nil
		}
		for {
			q, e := p.value(depth + 1)
			if e != nil {
				return nil, e
			}
			v.array = append(v.array, q)
			p.space()
			if p.take(']') {
				return v, nil
			}
			if !p.take(',') {
				return nil, invalid
			}
		}
	case '{':
		p.at++
		p.space()
		v.object = make(map[string]*member)
		if p.take('}') {
			return v, nil
		}
		for {
			p.space()
			key, id, e := p.keyValue()
			if e != nil {
				return nil, e
			}
			p.space()
			if !p.take(':') {
				return nil, invalid
			}
			q, e := p.value(depth + 1)
			if e != nil {
				return nil, e
			}
			v.object[id] = &member{key, q}
			p.space()
			if p.take('}') {
				return v, nil
			}
			if !p.take(',') {
				return nil, invalid
			}
		}
	case 't', 'f', 'n':
		word := map[byte]string{'t': "true", 'f': "false", 'n': "null"}[c]
		if p.at+len(word) > len(p.raw) || string(p.raw[p.at:p.at+len(word)]) != word {
			return nil, invalid
		}
		p.at += len(word)
		return v, nil
	default:
		start := p.at
		p.take('-')
		if !p.take('0') {
			if p.at >= len(p.raw) || p.raw[p.at] < '1' || p.raw[p.at] > '9' {
				return nil, invalid
			}
			for p.at < len(p.raw) && p.raw[p.at] >= '0' && p.raw[p.at] <= '9' {
				p.at++
			}
		}
		if p.take('.') {
			begin := p.at
			for p.at < len(p.raw) && p.raw[p.at] >= '0' && p.raw[p.at] <= '9' {
				p.at++
			}
			if begin == p.at {
				return nil, invalid
			}
		}
		if p.take('e') || p.take('E') {
			if !p.take('+') {
				p.take('-')
			}
			begin := p.at
			for p.at < len(p.raw) && p.raw[p.at] >= '0' && p.raw[p.at] <= '9' {
				p.at++
			}
			if begin == p.at {
				return nil, invalid
			}
		}
		f, e := strconv.ParseFloat(string(p.raw[start:p.at]), 64)
		if e != nil && !errors.Is(e, strconv.ErrRange) {
			return nil, invalid
		}
		v.kind = '0'
		v.number = f
		return v, nil
	}
}

type output struct {
	w      io.Writer
	err    error
	buffer []byte
}

func (o *output) flush() {
	if o.err == nil && len(o.buffer) > 0 {
		var n int
		n, o.err = o.w.Write(o.buffer)
		if o.err == nil && n != len(o.buffer) {
			o.err = io.ErrShortWrite
		}
	}
	o.buffer = o.buffer[:0]
}
func (o *output) write(s string) {
	for len(s) > 0 && o.err == nil {
		n := 16384 - len(o.buffer)
		if n > len(s) {
			n = len(s)
		}
		o.buffer = append(o.buffer, s[:n]...)
		s = s[n:]
		if len(o.buffer) == 16384 {
			o.flush()
		}
	}
}
func (o *output) one(b byte) {
	if len(o.buffer) == 16384 {
		o.flush()
	}
	o.buffer = append(o.buffer, b)
}
func (o *output) text(s []uint16) {
	o.write("\"")
	for i := 0; i < len(s); i++ {
		u := s[i]
		switch u {
		case '"':
			o.write("\\\"")
		case '\\':
			o.write("\\\\")
		case 8:
			o.write("\\b")
		case 9:
			o.write("\\t")
		case 10:
			o.write("\\n")
		case 12:
			o.write("\\f")
		case 13:
			o.write("\\r")
		default:
			if u < 32 {
				o.write(fmt.Sprintf("\\u%04x", u))
			} else if u < 0x80 {
				o.one(byte(u))
			} else if u >= 0xd800 && u <= 0xdbff && i+1 < len(s) && s[i+1] >= 0xdc00 && s[i+1] <= 0xdfff {
				o.write(string(utf16.DecodeRune(rune(u), rune(s[i+1]))))
				i++
			} else if u >= 0xd800 && u <= 0xdfff {
				o.write(fmt.Sprintf("\\u%04x", u))
			} else {
				o.write(string(rune(u)))
			}
		}
	}
	o.write("\"")
}
func indexKey(s []uint16) (uint64, bool) {
	if len(s) == 0 || len(s) > 10 || len(s) > 1 && s[0] == '0' {
		return 0, false
	}
	n := uint64(0)
	for _, u := range s {
		if u < '0' || u > '9' {
			return 0, false
		}
		n = n*10 + uint64(u-'0')
	}
	return n, n < 4294967295
}
func lessKey(a, b []uint16) bool {
	ai, ax := indexKey(a)
	bi, bx := indexKey(b)
	if ax != bx {
		return ax
	}
	if ax {
		return ai < bi
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}
func number(f float64) string {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return "null"
	}
	if f == 0 {
		return "0"
	}
	a := math.Abs(f)
	if a >= 1e-6 && a < 1e21 {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	s := strconv.FormatFloat(f, 'e', -1, 64)
	at := strings.LastIndexByte(s, 'e')
	exponent, _ := strconv.Atoi(s[at+1:])
	sign := ""
	if exponent >= 0 {
		sign = "+"
	}
	return s[:at] + "e" + sign + strconv.Itoa(exponent)
}
func (o *output) value(v *Value) {
	switch v.kind {
	case '"':
		if v.ascii != "" {
			o.write("\"")
			o.write(v.ascii)
			o.write("\"")
		} else {
			o.text(v.text)
		}
	case '0':
		o.write(number(v.number))
	case 't':
		o.write("true")
	case 'f':
		o.write("false")
	case 'n':
		o.write("null")
	case '[':
		o.write("[")
		for i, q := range v.array {
			if i > 0 {
				o.write(",")
			}
			o.value(q)
		}
		o.write("]")
	case '{':
		keys := make([]*member, 0, len(v.object))
		for _, m := range v.object {
			keys = append(keys, m)
		}
		sort.Slice(keys, func(i, j int) bool { return lessKey(keys[i].key, keys[j].key) })
		o.write("{")
		for i, m := range keys {
			if i > 0 {
				o.write(",")
			}
			o.text(m.key)
			o.write(":")
			o.value(m.value)
		}
		o.write("}")
	}
}
func (v *Value) WriteCanonical(w io.Writer) error {
	o := output{w: w, buffer: make([]byte, 0, 16384)}
	o.value(v)
	o.flush()
	return o.err
}
func (v *Value) Digest() string {
	h := sha256.New()
	_ = v.WriteCanonical(h)
	return hex.EncodeToString(h.Sum(nil))
}
func (v *Value) Pointer(pointer string) (*Value, bool) {
	return v.pointer(utf16.Encode([]rune(pointer)))
}
func (v *Value) PointerJSON(raw []byte) (*Value, bool) {
	p, e := Parse(raw)
	if e != nil || p.kind != '"' {
		return nil, false
	}
	if p.ascii != "" {
		return v.pointer(utf16.Encode([]rune(p.ascii)))
	}
	return v.pointer(p.text)
}
func (v *Value) pointer(pointer []uint16) (*Value, bool) {
	if len(pointer) == 0 {
		return v, true
	}
	if pointer[0] != '/' {
		return nil, false
	}
	for begin := 1; begin <= len(pointer); {
		end := begin
		for end < len(pointer) && pointer[end] != '/' {
			end++
		}
		part := pointer[begin:end]
		key := make([]uint16, 0, len(part))
		for i := 0; i < len(part); i++ {
			u := part[i]
			if u == '~' {
				if i+1 >= len(part) || (part[i+1] != '0' && part[i+1] != '1') {
					return nil, false
				}
				i++
				u = '~'
				if part[i] == '1' {
					u = '/'
				}
			}
			key = append(key, u)
		}
		switch v.kind {
		case '{':
			m, ok := v.object[keyID(key)]
			if !ok {
				return nil, false
			}
			v = m.value
		case '[':
			if keyID(key) == keyID([]uint16{'l', 'e', 'n', 'g', 't', 'h'}) {
				v = &Value{kind: '0', number: float64(len(v.array))}
			} else {
				n, ok := indexKey(key)
				if !ok || n >= uint64(len(v.array)) {
					return nil, false
				}
				v = v.array[n]
			}
		default:
			return nil, false
		}
		if end == len(pointer) {
			break
		}
		begin = end + 1
	}
	return v, true
}
