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

package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCompactStr(t *testing.T) {
	// 1 bytes
	bytes2 := make([]byte, 2)
	value2 := string(testHex2Bytes(t, "88"))
	putCompactString(bytes2, 0, value2)
	res2, idx2 := readCompactString(bytes2, 0)
	assert.Equal(t, idx2, 2)
	assert.Equal(t, value2, res2)

	// 127bytes
	bytes3 := make([]byte, 129)
	value3 := string(testHex2Bytes(t, "00000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff0000232600000001"))
	putCompactString(bytes3, 0, value3)
	res3, idx3 := readCompactString(bytes3, 0)
	assert.Equal(t, idx3, 129)
	assert.Equal(t, res3, value3)

	// 128 bytes
	bytes4 := make([]byte, 130)
	value4 := string(testHex2Bytes(t, "00000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff000023260000000100"))
	putCompactString(bytes4, 0, value4)
	res4, idx4 := readCompactString(bytes4, 0)
	assert.Equal(t, idx4, 130)
	assert.Equal(t, res4, value4)

	// 256 bytes
	bytes5 := make([]byte, 258)
	value5 := string(testHex2Bytes(t, "00000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff00002326000000010000000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff000023260000000100"))
	putCompactString(bytes5, 0, value5)
	res5, idx5 := readCompactString(bytes5, 0)
	assert.Equal(t, idx5, 258)
	assert.Equal(t, res5, value5)
}

func TestCompactNullableStr(t *testing.T) {
	// nil
	bytes1 := make([]byte, 1)
	putCompactStringNullable(bytes1, 0, nil)
	res1, idx1 := readCompactStringNullable(bytes1, 0)
	assert.Equal(t, idx1, 1)
	assert.Nil(t, res1)

	// 1 bytes
	bytes2 := make([]byte, 2)
	value2 := string(testHex2Bytes(t, "88"))
	putCompactStringNullable(bytes2, 0, &value2)
	res2, idx2 := readCompactStringNullable(bytes2, 0)
	assert.Equal(t, idx2, 2)
	assert.Equal(t, value2, *res2)

	// 127bytes
	bytes3 := make([]byte, 129)
	value3 := string(testHex2Bytes(t, "00000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff0000232600000001"))
	putCompactStringNullable(bytes3, 0, &value3)
	res3, idx3 := readCompactStringNullable(bytes3, 0)
	assert.Equal(t, idx3, 129)
	assert.Equal(t, *res3, value3)

	// 128 bytes
	bytes4 := make([]byte, 130)
	value4 := string(testHex2Bytes(t, "00000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff000023260000000100"))
	putCompactStringNullable(bytes4, 0, &value4)
	res4, idx4 := readCompactStringNullable(bytes4, 0)
	assert.Equal(t, idx4, 130)
	assert.Equal(t, *res4, value4)

	// 256 bytes
	bytes5 := make([]byte, 258)
	value5 := string(testHex2Bytes(t, "00000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff00002326000000010000000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff000023260000000100"))
	putCompactStringNullable(bytes5, 0, &value5)
	res5, idx5 := readCompactStringNullable(bytes5, 0)
	assert.Equal(t, idx5, 258)
	assert.Equal(t, *res5, value5)
}
