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

type SyncGroupResp struct {
	BaseResp
	ThrottleTime     int
	ErrorCode        ErrorCode
	ProtocolType     string
	ProtocolName     string
	MemberAssignment string
}

func DecodeSyncGroupResp(bytes []byte, version int16) (resp *SyncGroupResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &SyncGroupResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 4 || version == 5 {
		idx = readTaggedField(bytes, idx)
		resp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	}
	resp.ErrorCode, idx = readErrorCode(bytes, idx)
	if version == 5 {
		resp.ProtocolType, idx = readProtocolType(bytes, idx)
		resp.ProtocolName, idx = readProtocolName(bytes, idx)
	}
	if version == 0 {
		idx += 2
		resp.MemberAssignment, idx = readString(bytes, idx)
	} else if version == 4 || version == 5 {
		resp.MemberAssignment, idx = readCompactString(bytes, idx)
	}
	if version == 4 || version == 5 {
		idx = readTaggedField(bytes, idx)
	}
	return resp, nil
}

func (s *SyncGroupResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 4 || version == 5 {
		result += LenTaggedField + LenThrottleTime
	}
	result += LenErrorCode
	if version == 5 {
		result += CompactStrLen(s.ProtocolType) + CompactStrLen(s.ProtocolName)
	}
	if version == 0 {
		result += 2
		result += StrLen(s.MemberAssignment)
	} else if version == 4 || version == 5 {
		result += CompactStrLen(s.MemberAssignment)
	}
	if version == 4 || version == 5 {
		result += LenTaggedField
	}
	return result
}

func (s *SyncGroupResp) Bytes(version int16) []byte {
	bytes := make([]byte, s.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, s.CorrelationId)
	if version == 4 || version == 5 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, 0)
	}
	idx = putErrorCode(bytes, idx, 0)
	if version == 5 {
		idx = putProtocolType(bytes, idx, s.ProtocolType)
		idx = putProtocolName(bytes, idx, s.ProtocolName)
	}
	if version == 0 {
		idx += 2
		idx = putString(bytes, idx, s.MemberAssignment)
	} else if version == 4 || version == 5 {
		idx = putCompactString(bytes, idx, s.MemberAssignment)
	}
	if version == 4 || version == 5 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
