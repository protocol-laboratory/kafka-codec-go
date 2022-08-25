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

type LeaveGroupReq struct {
	BaseReq
	GroupId string
	Members []*LeaveGroupMember
}

func DecodeLeaveGroupReq(bytes []byte, version int16) (leaveGroupReq *LeaveGroupReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			leaveGroupReq = nil
		}
	}()
	leaveGroupReq = &LeaveGroupReq{}
	leaveGroupReq.ApiVersion = version
	idx := 0
	leaveGroupReq.CorrelationId, idx = readCorrId(bytes, idx)
	leaveGroupReq.ClientId, idx = readClientId(bytes, idx)
	if version == 4 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 0 {
		leaveGroupReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 4 {
		leaveGroupReq.GroupId, idx = readGroupId(bytes, idx)
	}
	if version == 0 {
		leaveGroupReq.Members = make([]*LeaveGroupMember, 1)
		member := LeaveGroupMember{}
		member.MemberId, idx = readMemberIdString(bytes, idx)
		leaveGroupReq.Members[0] = &member
	}
	if version == 4 {
		var length int
		length, idx = readCompactArrayLen(bytes, idx)
		leaveGroupReq.Members = make([]*LeaveGroupMember, length)
		for i := 0; i < length; i++ {
			member := LeaveGroupMember{}
			member.MemberId, idx = readMemberId(bytes, idx)
			member.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
			idx = readTaggedField(bytes, idx)
			leaveGroupReq.Members[i] = &member
		}
		idx = readTaggedField(bytes, idx)
	}
	return leaveGroupReq, nil
}

func (l *LeaveGroupReq) BytesLength(containApiKeyVersion bool) int {
	version := l.ApiVersion
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(l.ClientId)
	if version == 0 {
		length += StrLen(l.GroupId)
	} else if version == 4 {
		length += LenTaggedField
		length += CompactStrLen(l.GroupId)
	}
	if version == 0 {
		members := l.Members
		if len(members) > 0 {
			member := members[0]
			length += StrLen(member.MemberId)
		}
	} else if version == 4 {
		length += CompactArrayLen(len(l.Members))
		for _, member := range l.Members {
			length += CompactStrLen(member.MemberId)
			length += CompactNullableStrLen(member.GroupInstanceId)
			length += LenTaggedField
		}
		length += LenTaggedField
	}
	return length
}

func (l *LeaveGroupReq) Bytes(containApiKeyVersion bool) []byte {
	version := l.ApiVersion
	bytes := make([]byte, l.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, LeaveGroup)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, l.CorrelationId)
	idx = putClientId(bytes, idx, l.ClientId)
	if version == 0 {
		idx = putGroupIdString(bytes, idx, l.GroupId)
	} else if version == 4 {
		idx = putTaggedField(bytes, idx)
		idx = putGroupId(bytes, idx, l.GroupId)
	}
	if version == 0 {
		members := l.Members
		if len(members) > 0 {
			member := members[0]
			idx = putMemberIdString(bytes, idx, member.MemberId)
		}
	} else if version == 4 {
		idx = putCompactArrayLen(bytes, idx, len(l.Members))
		for _, member := range l.Members {
			idx = putMemberId(bytes, idx, member.MemberId)
			idx = putGroupInstanceId(bytes, idx, member.GroupInstanceId)
			idx = putTaggedField(bytes, idx)
		}
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
