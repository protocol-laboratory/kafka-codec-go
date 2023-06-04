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

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func growthMemory(buf *buffer) error {
	if buf.cursor == buf.max {
		return fmt.Errorf("buffer full")
	}
	if buf.cursor == buf.currentSize && buf.currentSize < buf.max {
		if buf.currentSize*2 < buf.max {
			buf.currentSize *= 2
		} else {
			buf.currentSize = buf.max
		}
		buffer := make([]byte, buf.currentSize)
		copy(buffer, buf.bytes)
		buf.bytes = buffer
		buf.cursor = buf.currentSize
	}
	return nil
}

func TestNewBuffer(t *testing.T) {
	buf := newBuffer(1024 * 32)
	buf.cursor = buf.currentSize
	assert.EqualValues(t, 1024, buf.currentSize)
	assert.EqualValues(t, 1024, len(buf.bytes))

	err := growthMemory(buf)
	assert.NoError(t, err)
	assert.EqualValues(t, 2048, buf.currentSize)
	assert.EqualValues(t, 2048, len(buf.bytes))

	err = growthMemory(buf)
	assert.NoError(t, err)
	assert.EqualValues(t, 4096, buf.currentSize)
	assert.EqualValues(t, 4096, len(buf.bytes))

	err = growthMemory(buf)
	assert.NoError(t, err)
	assert.EqualValues(t, 8192, buf.currentSize)
	assert.EqualValues(t, 8192, len(buf.bytes))

	err = growthMemory(buf)
	assert.NoError(t, err)
	assert.EqualValues(t, 16384, buf.currentSize)
	assert.EqualValues(t, 16384, len(buf.bytes))

	err = growthMemory(buf)
	assert.NoError(t, err)
	assert.EqualValues(t, 32768, buf.currentSize)
	assert.EqualValues(t, 32768, len(buf.bytes))

	err = growthMemory(buf)
	assert.Error(t, err, "buffer full")
}
