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

type HeartbeatResp struct {
	BaseResp
	ErrorCode    ErrorCode
	ThrottleTime int
}

func DecodeHeartbeatResp(bytes []byte, version int16) (resp *HeartbeatResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &HeartbeatResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 4 {
		idx = readTaggedField(bytes, idx)
		resp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	}
	resp.ErrorCode, idx = readErrorCode(bytes, idx)
	if version == 4 {
		idx = readTaggedField(bytes, idx)
	}
	return resp, nil
}

func (h *HeartbeatResp) BytesLength(version int16) int {
	length := 0
	length += LenCorrId
	if version == 4 {
		length += LenTaggedField
		length += LenThrottleTime
	}
	length += LenErrorCode
	if version == 4 {
		length += LenTaggedField
	}
	return length
}

func (h *HeartbeatResp) Bytes(version int16) []byte {
	bytes := make([]byte, h.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, h.CorrelationId)
	if version == 4 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, 0)
	}
	idx = putErrorCode(bytes, idx, h.ErrorCode)
	if version == 4 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
