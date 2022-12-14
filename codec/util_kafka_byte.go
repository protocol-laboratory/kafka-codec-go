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

func readCoordinatorType(bytes []byte, idx int) (byte, int) {
	return bytes[idx], idx + 1
}

func putCoordinatorType(bytes []byte, idx int, x byte) int {
	return putByte(bytes, idx, x)
}

func readIsolationLevel(bytes []byte, idx int) (byte, int) {
	return bytes[idx], idx + 1
}

func putIsolationLevel(bytes []byte, idx int, x byte) int {
	return putByte(bytes, idx, x)
}

func readMagicByte(bytes []byte, idx int) (byte, int) {
	return bytes[idx], idx + 1
}

func putMagicByte(bytes []byte, idx int, x byte) int {
	return putByte(bytes, idx, x)
}

func readRecordAttributes(bytes []byte, idx int) (byte, int) {
	return bytes[idx], idx + 1
}

func putRecordAttributes(bytes []byte, idx int, x byte) int {
	return putByte(bytes, idx, x)
}
