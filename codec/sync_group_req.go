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

type SyncGroupReq struct {
	BaseReq
	GroupId          string
	GenerationId     int
	MemberId         string
	GroupInstanceId  *string
	ProtocolType     string
	ProtocolName     string
	GroupAssignments []*GroupAssignment
}

type GroupAssignment struct {
	MemberId string
	// COMPACT_BYTES
	MemberAssignment string
}

func DecodeSyncGroupReq(bytes []byte, version int16) (groupReq *SyncGroupReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			groupReq = nil
		}
	}()
	groupReq = &SyncGroupReq{}
	groupReq.ApiVersion = version
	idx := 0
	groupReq.CorrelationId, idx = readCorrId(bytes, idx)
	groupReq.ClientId, idx = readClientId(bytes, idx)
	if version == 4 || version == 5 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 0 {
		groupReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 4 || version == 5 {
		groupReq.GroupId, idx = readGroupId(bytes, idx)
	}
	groupReq.GenerationId, idx = readGenerationId(bytes, idx)
	if version == 0 {
		groupReq.MemberId, idx = readMemberIdString(bytes, idx)
	} else if version == 4 || version == 5 {
		groupReq.MemberId, idx = readMemberId(bytes, idx)
	}
	if version == 4 || version == 5 {
		groupReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	}
	if version == 5 {
		groupReq.ProtocolType, idx = readProtocolType(bytes, idx)
		groupReq.ProtocolName, idx = readProtocolName(bytes, idx)
	}
	var groupAssignmentLength int
	if version == 0 {
		groupAssignmentLength, idx = readArrayLen(bytes, idx)
	} else if version == 4 || version == 5 {
		groupAssignmentLength, idx = readCompactArrayLen(bytes, idx)
	}
	for i := 0; i < groupAssignmentLength; i++ {
		groupAssignment := &GroupAssignment{}
		if version == 0 {
			groupAssignment.MemberId, idx = readMemberIdString(bytes, idx)
		} else if version == 4 || version == 5 {
			groupAssignment.MemberId, idx = readMemberId(bytes, idx)
		}
		if version == 0 {
			groupAssignment.MemberAssignment, idx = readString(bytes, idx)
		} else if version == 4 || version == 5 {
			groupAssignment.MemberAssignment, idx = readCompactString(bytes, idx)
		}
		if version == 4 || version == 5 {
			idx = readTaggedField(bytes, idx)
		}
		groupReq.GroupAssignments = append(groupReq.GroupAssignments, groupAssignment)
	}
	if version == 4 || version == 5 {
		idx = readTaggedField(bytes, idx)
	}
	return groupReq, nil
}

func (s *SyncGroupReq) BytesLength(containApiKeyVersion bool) int {
	version := s.ApiVersion
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(s.ClientId)
	if version == 0 {
		length += StrLen(s.GroupId)
	} else if version == 4 || version == 5 {
		length += LenTaggedField
		length += CompactStrLen(s.GroupId)
	}
	length += LenGenerationId
	if version == 0 {
		length += StrLen(s.MemberId)
	} else if version == 4 || version == 5 {
		length += CompactStrLen(s.MemberId)
		length += CompactNullableStrLen(s.GroupInstanceId)
	}
	if version == 5 {
		length += CompactStrLen(s.ProtocolType)
		length += CompactStrLen(s.ProtocolName)
	}
	if version == 0 {
		length += LenArray
	} else if version == 4 || version == 5 {
		length += CompactArrayLen(len(s.GroupAssignments))
	}
	for _, groupAssignment := range s.GroupAssignments {
		if version == 0 {
			length += StrLen(groupAssignment.MemberId)
			length += StrLen(groupAssignment.MemberAssignment)
		} else if version == 4 || version == 5 {
			length += CompactStrLen(groupAssignment.MemberId)
			length += CompactStrLen(groupAssignment.MemberAssignment)
		}
		if version == 4 || version == 5 {
			length += LenTaggedField
		}
	}
	if version == 4 || version == 5 {
		length += LenTaggedField
	}
	return length
}

func (s *SyncGroupReq) Bytes(containApiKeyVersion bool) []byte {
	version := s.ApiVersion
	bytes := make([]byte, s.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, SyncGroup)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, s.CorrelationId)
	idx = putClientId(bytes, idx, s.ClientId)
	if version == 0 {
		idx = putGroupIdString(bytes, idx, s.GroupId)
	} else if version == 4 || version == 5 {
		idx = putTaggedField(bytes, idx)
		idx = putGroupId(bytes, idx, s.GroupId)
	}
	idx = putGenerationId(bytes, idx, s.GenerationId)
	if version == 0 {
		idx = putMemberIdString(bytes, idx, s.MemberId)
	} else if version == 4 || version == 5 {
		idx = putMemberId(bytes, idx, s.MemberId)
		idx = putGroupInstanceId(bytes, idx, s.GroupInstanceId)
	}
	if version == 5 {
		idx = putProtocolType(bytes, idx, s.ProtocolType)
		idx = putProtocolName(bytes, idx, s.ProtocolName)
	}
	if version == 0 {
		idx = putArrayLen(bytes, idx, len(s.GroupAssignments))
	} else if version == 4 || version == 5 {
		idx = putCompactArrayLen(bytes, idx, len(s.GroupAssignments))
	}
	for _, groupAssignment := range s.GroupAssignments {
		if version == 0 {
			idx = putMemberIdString(bytes, idx, groupAssignment.MemberId)
			idx = putString(bytes, idx, groupAssignment.MemberAssignment)
		} else if version == 4 || version == 5 {
			idx = putMemberId(bytes, idx, groupAssignment.MemberId)
			idx = putCompactString(bytes, idx, groupAssignment.MemberAssignment)
		}
		if version == 4 || version == 5 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 4 || version == 5 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
