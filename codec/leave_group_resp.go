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

type LeaveGroupResp struct {
	BaseResp
	ErrorCode       ErrorCode
	ThrottleTime    int
	Members         []*LeaveGroupMember
	MemberErrorCode ErrorCode
}

func DecodeLeaveGroupResp(bytes []byte, version int16) (resp *LeaveGroupResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &LeaveGroupResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 0 {
		resp.ErrorCode, idx = readErrorCode(bytes, idx)
	} else if version == 4 {
		idx = readTaggedField(bytes, idx)
		resp.ThrottleTime, idx = readThrottleTime(bytes, idx)
		resp.ErrorCode, idx = readErrorCode(bytes, idx)
		var membersLen int
		membersLen, idx = readCompactArrayLen(bytes, idx)
		for i := 0; i < membersLen; i++ {
			member := &LeaveGroupMember{}
			member.MemberId, idx = readMemberId(bytes, idx)
			member.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
			idx = readTaggedField(bytes, idx)
			resp.Members = append(resp.Members, member)
		}
		resp.MemberErrorCode, idx = readErrorCode(bytes, idx)
		idx = readTaggedField(bytes, idx)
	}
	return resp, nil
}

func (l *LeaveGroupResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 0 {
		result += LenErrorCode
	} else if version == 4 {
		result += LenTaggedField + LenThrottleTime + LenErrorCode + CompactArrayLen(len(l.Members))
		for _, val := range l.Members {
			result += CompactStrLen(val.MemberId)
			result += CompactNullableStrLen(val.GroupInstanceId)
			result += LenTaggedField
		}
		result += LenErrorCode
		result += LenTaggedField
	}
	return result
}

func (l *LeaveGroupResp) Bytes(version int16) []byte {
	bytes := make([]byte, l.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, l.CorrelationId)
	if version == 0 {
		idx = putErrorCode(bytes, idx, 0)
	} else if version == 4 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, l.ThrottleTime)
		idx = putErrorCode(bytes, idx, 0)
		bytes[idx] = byte(len(l.Members) + 1)
		idx++
		for _, member := range l.Members {
			idx = putMemberId(bytes, idx, member.MemberId)
			idx = putGroupInstanceId(bytes, idx, member.GroupInstanceId)
			idx = putTaggedField(bytes, idx)
		}
		idx = putErrorCode(bytes, idx, 0)
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
