// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package knet

import "fmt"

type buffer struct {
	max         int
	cursor      int
	currentSize int
	bytes       []byte
}

func newBuffer(maxSize int) *buffer {
	var buf = new(buffer)
	buf.max = maxSize
	buf.currentSize = maxSize / 32
	buf.bytes = make([]byte, buf.currentSize)
	return buf
}

func (b *buffer) expand() {
	if b.currentSize*2 > b.max {
		b.currentSize = b.max
	} else {
		b.currentSize *= 2
	}
	buf := make([]byte, b.currentSize)
	copy(buf, b.bytes)
	b.bytes = buf
}

func (b *buffer) Write(p []byte) (int, error) {
	n := len(p)
	if b.cursor+n > b.max {
		return 0, fmt.Errorf("buffer full")
	}
	if b.cursor+n > len(b.bytes) {
		b.expand()
	}
	b.bytes = append(b.bytes[:b.cursor], p...)
	b.cursor += n
	return n, nil
}
