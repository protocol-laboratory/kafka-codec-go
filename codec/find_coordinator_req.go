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
	"runtime/debug"
)

type FindCoordinatorReq struct {
	BaseReq
	Key     string
	KeyType byte
}

func DecodeFindCoordinatorReq(bytes []byte, version int16) (findCoordinatorReq *FindCoordinatorReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			findCoordinatorReq = nil
		}
	}()
	findCoordinatorReq = &FindCoordinatorReq{}
	findCoordinatorReq.ApiVersion = version
	idx := 0
	findCoordinatorReq.CorrelationId, idx = readCorrId(bytes, idx)
	findCoordinatorReq.ClientId, idx = readClientId(bytes, idx)
	if version == 3 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 0 {
		findCoordinatorReq.Key, idx = readString(bytes, idx)
	} else if version == 3 {
		findCoordinatorReq.Key, idx = readCoordinatorKey(bytes, idx)
	}
	if version == 0 {
		findCoordinatorReq.KeyType = 0
	} else if version == 3 {
		findCoordinatorReq.KeyType, idx = readCoordinatorType(bytes, idx)
	}
	if version == 3 {
		idx = readTaggedField(bytes, idx)
	}
	return findCoordinatorReq, nil
}

func (f *FindCoordinatorReq) BytesLength(containApiKeyVersion bool) int {
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(f.ClientId)
	length += LenTaggedField
	length += CompactStrLen(f.Key)
	length += LenCoordinatorType
	length += LenTaggedField
	return length
}

func (f *FindCoordinatorReq) Bytes(containApiKeyVersion bool) []byte {
	version := f.ApiVersion
	bytes := make([]byte, f.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, FindCoordinator)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, f.CorrelationId)
	idx = putClientId(bytes, idx, f.ClientId)
	idx = putTaggedField(bytes, idx)
	idx = putCoordinatorKey(bytes, idx, f.Key)
	idx = putCoordinatorType(bytes, idx, f.KeyType)
	idx = putTaggedField(bytes, idx)
	return bytes
}
