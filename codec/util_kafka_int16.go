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

// This file is for kafka code int16 type. Format method as alpha order.

func readApiKey(bytes []byte, idx int) (ApiCode, int) {
	ac, idx := readInt16(bytes, idx)
	return ApiCode(ac), idx
}

func putApiKey(bytes []byte, idx int, x ApiCode) int {
	return putInt16(bytes, idx, int16(x))
}

func readApiMaxVersion(bytes []byte, idx int) (int16, int) {
	return readInt16(bytes, idx)
}

func putApiMaxVersion(bytes []byte, idx int, x int16) int {
	return putInt16(bytes, idx, x)
}

func readApiMinVersion(bytes []byte, idx int) (int16, int) {
	return readInt16(bytes, idx)
}

func putApiMinVersion(bytes []byte, idx int, x int16) int {
	return putInt16(bytes, idx, x)
}

func readApiVersion(bytes []byte, idx int) (int16, int) {
	return readInt16(bytes, idx)
}

func putApiVersion(bytes []byte, idx int, x int16) int {
	return putInt16(bytes, idx, x)
}

func readErrorCode(bytes []byte, idx int) (ErrorCode, int) {
	ec, i := readInt16(bytes, idx)
	return ErrorCode(ec), i
}

func putErrorCode(bytes []byte, idx int, errorCode ErrorCode) int {
	return putInt16(bytes, idx, int16(errorCode))
}

func readProducerEpoch(bytes []byte, idx int) (int16, int) {
	return readInt16(bytes, idx)
}

func putProducerEpoch(bytes []byte, idx int, errorCode int16) int {
	return putInt16(bytes, idx, errorCode)
}

func readRequiredAcks(bytes []byte, idx int) (int16, int) {
	return readInt16(bytes, idx)
}

func putRequiredAcks(bytes []byte, idx int, x int16) int {
	return putInt16(bytes, idx, x)
}
