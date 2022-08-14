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

func DecodeApiReq(bytes []byte, version int16) (apiReq *ApiReq, r any, stack []byte) {
	defer func() {
		if r = recover(); r != nil {
			stack = debug.Stack()
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
	return apiReq, nil, nil
}
