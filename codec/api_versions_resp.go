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

type ApiResp struct {
	BaseResp
	ErrorCode       ErrorCode
	ApiRespVersions []*ApiRespVersion
	ThrottleTime    int
}

type ApiRespVersion struct {
	ApiKey     ApiCode
	MinVersion int16
	MaxVersion int16
}

func DecodeApiResp(bytes []byte, version int16) (apiResp *ApiResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			apiResp = nil
		}
	}()
	apiResp = &ApiResp{}
	idx := 0
	apiResp.CorrelationId, idx = readCorrId(bytes, idx)
	apiResp.ErrorCode, idx = readErrorCode(bytes, idx)
	var length int
	if version == 1 {
		length, idx = readArrayLen(bytes, idx)
	} else if version == 3 {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	for i := 0; i < length; i++ {
		apiRespVersion := &ApiRespVersion{}
		apiRespVersion.ApiKey, idx = readApiKey(bytes, idx)
		apiRespVersion.MinVersion, idx = readApiMinVersion(bytes, idx)
		apiRespVersion.MaxVersion, idx = readApiMaxVersion(bytes, idx)
		if version == 3 {
			idx = readTaggedField(bytes, idx)
		}
		apiResp.ApiRespVersions = append(apiResp.ApiRespVersions, apiRespVersion)
	}
	return apiResp, nil
}

func (a *ApiResp) BytesLength(version int16) int {
	length := LenCorrId + LenErrorCode + LenArray
	if version < 3 {
		length += LenApiV0to2 * len(a.ApiRespVersions)
	} else if version == 3 {
		length += LenApiV3 * len(a.ApiRespVersions)
	}
	if version == 2 || version == 3 {
		length += LenThrottleTime
	}
	if version == 3 {
		length += LenTaggedField
	}
	return length
}

func (a *ApiResp) Bytes(version int16) []byte {
	bytes := make([]byte, a.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, a.CorrelationId)
	idx = putErrorCode(bytes, idx, a.ErrorCode)
	if version < 3 {
		idx = putArrayLen(bytes, idx, len(a.ApiRespVersions))
	}
	if version == 3 {
		idx = putCompactArrayLen(bytes, idx, len(a.ApiRespVersions))
	}
	for i := 0; i < len(a.ApiRespVersions); i++ {
		apiRespVersion := a.ApiRespVersions[i]
		idx = putApiKey(bytes, idx, apiRespVersion.ApiKey)
		idx = putApiMinVersion(bytes, idx, apiRespVersion.MinVersion)
		idx = putApiMaxVersion(bytes, idx, apiRespVersion.MaxVersion)
		if version == 3 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 2 || version == 3 {
		idx = putThrottleTime(bytes, idx, a.ThrottleTime)
	}
	if version == 3 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
