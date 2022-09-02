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

func readBytes(bytes []byte, idx int) ([]byte, int) {
	length, idx := readInt(bytes, idx)
	return bytes[idx : idx+length], idx + length
}

func putBytes(bytes []byte, idx int, srcBytes []byte) int {
	idx = putInt(bytes, idx, len(srcBytes))
	copy(bytes[idx:], srcBytes)
	return idx + len(srcBytes)
}

func readCompactBytes(bytes []byte, idx int) ([]byte, int) {
	auxUInt32, idx := readUVarint(bytes, idx)
	intLen := ConvertCompactLen(int(auxUInt32))
	return bytes[idx : idx+intLen], idx + intLen
}

func putCompactBytes(bytes []byte, idx int, compactBytes []byte) int {
	idx = putUVarint(bytes, idx, uint32(CompactBytesLen(compactBytes)))
	copy(bytes[idx:], compactBytes)
	return idx + len(compactBytes)
}

func readCompactNullableBytes(bytes []byte, idx int) ([]byte, int) {
	bytesLen, idx := readUVarint(bytes, idx)
	if bytesLen == 0 {
		return nil, idx
	}
	intLen := ConvertCompactLen(int(bytesLen))
	return bytes[idx : idx+intLen], idx + intLen
}

func putCompactNullableBytes(bytes []byte, idx int, content []byte) int {
	if content == nil {
		return putUVarint(bytes, idx, 0)
	}
	idx = putUVarint(bytes, idx, uint32(CompactBytesLen(content)))
	copy(bytes[idx:], content)
	return idx + len(content)
}

func putVCompactBytes(bytes []byte, idx int, authBytes []byte) int {
	if authBytes == nil {
		return putVarint(bytes, idx, -1)
	}
	idx = putVarint(bytes, idx, len(authBytes))
	copy(bytes[idx:], authBytes)
	return idx + len(authBytes)
}

func readVCompactBytes(bytes []byte, idx int) ([]byte, int) {
	var length int
	length, idx = readVarint(bytes, idx)
	if length < 0 {
		return nil, idx
	}
	return bytes[idx : idx+length], idx + length
}

func BytesLen(bytes []byte) int {
	return 4 + len(bytes)
}

func CompactBytesLen(bytes []byte) int {
	return uVarintSize(uint(len(bytes))) + len(bytes)
}

func CompactNullableBytesLen(bytes []byte) int {
	if bytes == nil {
		return 1
	}
	return uVarintSize(uint(len(bytes))) + len(bytes)
}
