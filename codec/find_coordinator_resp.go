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

import "runtime/debug"

type FindCoordinatorResp struct {
	BaseResp
	ErrorCode    ErrorCode
	ThrottleTime int
	ErrorMessage *string
	NodeId       int32
	Host         string
	Port         int
}

func DecodeFindCoordinatorResp(bytes []byte, version int16) (fResp *FindCoordinatorResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			fResp = nil
		}
	}()
	fResp = &FindCoordinatorResp{}
	idx := 0
	fResp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 3 {
		idx = readTaggedField(bytes, idx)
		fResp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	}
	fResp.ErrorCode, idx = readErrorCode(bytes, idx)
	if version == 3 {
		fResp.ErrorMessage, idx = readFindCoordinatorErrorMessage(bytes, idx)
	}
	fResp.NodeId, idx = readNodeId(bytes, idx)
	if version == 0 {
		fResp.Host, idx = readHostString(bytes, idx)
	} else if version == 3 {
		fResp.Host, idx = readHost(bytes, idx)
	}
	fResp.Port, idx = readInt(bytes, idx)
	if version == 3 {
		idx = readTaggedField(bytes, idx)
	}
	return fResp, nil
}

func (f *FindCoordinatorResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 3 {
		result += LenTaggedField + LenThrottleTime
	}
	result += LenErrorCode
	if version == 3 {
		result += CompactNullableStrLen(f.ErrorMessage)
	}
	result += LenNodeId
	if version == 0 {
		result += StrLen(f.Host)
	} else if version == 3 {
		result += CompactStrLen(f.Host)
	}
	result += LenPort
	if version == 3 {
		result += LenTaggedField
	}
	return result
}

func (f *FindCoordinatorResp) Bytes(version int16) []byte {
	bytes := make([]byte, f.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, f.CorrelationId)
	if version == 3 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, 0)
	}
	idx = putErrorCode(bytes, idx, f.ErrorCode)
	if version == 3 {
		idx = putFindCoordinatorErrorMessage(bytes, idx, f.ErrorMessage)
	}
	idx = putNodeId(bytes, idx, f.NodeId)
	if version == 0 {
		idx = putHostString(bytes, idx, f.Host)
	} else if version == 3 {
		idx = putHost(bytes, idx, f.Host)
	}
	idx = putInt(bytes, idx, f.Port)
	if version == 3 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
