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

func readArrayLen(bytes []byte, idx int) (int, int) {
	return readInt(bytes, idx)
}

func putArrayLen[T any](bytes []byte, idx int, array []T) int {
	if array == nil {
		return putInt(bytes, idx, -1)
	}
	return putInt(bytes, idx, len(array))
}

func readCompactArrayLen(bytes []byte, idx int) (int, int) {
	uVarint, i := readUVarint(bytes, idx)
	return int(uVarint - 1), i
}

func putCompactArrayLen(bytes []byte, idx int, length int) int {
	return putUVarint(bytes, idx, uint32(length+1))
}
