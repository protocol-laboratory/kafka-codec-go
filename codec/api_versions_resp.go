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

type ApiResp struct {
	BaseResp
	ErrorCode       int16
	ApiRespVersions []*ApiRespVersion
	ThrottleTime    int
}

type ApiRespVersion struct {
	ApiKey     ApiCode
	MinVersion int16
	MaxVersion int16
}

func (a *ApiResp) BytesLength(version int16) int {
	length := LenCorrId + LenErrorCode + LenArray
	if version == 0 {
		length += LenApiV0 * len(a.ApiRespVersions)
	}
	if version == 3 {
		length += LenApiV3*len(a.ApiRespVersions) + LenThrottleTime + 1
	}
	return length
}

func (a *ApiResp) Bytes(version int16) []byte {
	bytes := make([]byte, a.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, a.CorrelationId)
	idx = putErrorCode(bytes, idx, a.ErrorCode)
	if version == 0 {
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
	if version == 3 {
		idx = putThrottleTime(bytes, idx, a.ThrottleTime)
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
