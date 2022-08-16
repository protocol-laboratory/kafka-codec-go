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

type ApiReq struct {
	BaseReq
	ClientSoftwareName    string
	ClientSoftwareVersion string
}

func DecodeApiReq(bytes []byte, version int16) (apiReq *ApiReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			apiReq = nil
		}
	}()
	apiReq = &ApiReq{}
	apiReq.ApiVersion = version
	idx := 0
	apiReq.CorrelationId, idx = readCorrId(bytes, idx)
	apiReq.ClientId, idx = readClientId(bytes, idx)
	if version == 3 {
		idx = readTaggedField(bytes, idx)
		apiReq.ClientSoftwareName, idx = readClientSoftwareName(bytes, idx)
		apiReq.ClientSoftwareVersion, idx = readClientSoftwareVersion(bytes, idx)
		idx = readTaggedField(bytes, idx)
	}
	return apiReq, nil
}

func (a *ApiReq) BytesLength() int {
	version := a.ApiVersion
	length := LenCorrId
	length += StrLen(a.ClientId)
	if version == 3 {
		length += LenTaggedField
	}
	length += CompactStrLen(a.ClientSoftwareName)
	length += CompactStrLen(a.ClientSoftwareVersion)
	if version == 3 {
		length += LenTaggedField
	}
	return length
}

func (a *ApiReq) Bytes() []byte {
	version := a.ApiVersion
	bytes := make([]byte, a.BytesLength()+4)
	idx := 0
	idx = putApiKey(bytes, idx, ApiVersions)
	idx = putApiVersion(bytes, idx, version)
	idx = putCorrId(bytes, idx, a.CorrelationId)
	idx = putClientId(bytes, idx, a.ClientId)
	if version == 3 {
		idx = putTaggedField(bytes, idx)
	}
	idx = putCompactString(bytes, idx, a.ClientSoftwareName)
	idx = putCompactString(bytes, idx, a.ClientSoftwareVersion)
	return bytes
}
