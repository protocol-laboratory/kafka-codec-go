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

func readRecordHeadersSize(bytes []byte, idx int) (int, int) {
	return readVarint(bytes, idx)
}

func putRecordHeadersSize(bytes []byte, idx int, x int) int {
	return putVarint(bytes, idx, x)
}

func recordHeadersSizeLen(x int) int {
	return varintSize(x)
}

func readRecordHeaderKey(bytes []byte, idx int) (string, int) {
	strLen, idx := readVarint(bytes, idx)
	return string(bytes[idx : idx+strLen]), idx + strLen
}

func putRecordHeaderKey(bytes []byte, idx int, x string) int {
	idx = putVarint(bytes, idx, len(x))
	return copy(bytes[idx:], x) + idx
}

func recordHeaderKeyLen(x string) int {
	return varintSize(len(x)) + len(x)
}

func readRecordHeaderValue(bytes []byte, idx int) ([]byte, int) {
	strLen, idx := readVarint(bytes, idx)
	return bytes[idx : idx+strLen], idx + strLen
}

func putRecordHeaderValue(bytes []byte, idx int, x []byte) int {
	idx = putVarint(bytes, idx, len(x))
	return copy(bytes[idx:], x) + idx
}

func recordHeaderValueLen(x []byte) int {
	return varintSize(len(x)) + len(x)
}
