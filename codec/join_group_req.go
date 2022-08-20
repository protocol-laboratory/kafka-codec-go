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

type JoinGroupReq struct {
	BaseReq
	GroupId          string
	SessionTimeout   int
	RebalanceTimeout int
	MemberId         string
	GroupInstanceId  *string
	ProtocolType     string
	GroupProtocols   []*GroupProtocol
}

type GroupProtocol struct {
	ProtocolName     string
	ProtocolMetadata string
}

func DecodeJoinGroupReq(bytes []byte, version int16) (joinGroupReq *JoinGroupReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			joinGroupReq = nil
		}
	}()
	joinGroupReq = &JoinGroupReq{}
	joinGroupReq.ApiVersion = version
	idx := 0
	joinGroupReq.CorrelationId, idx = readCorrId(bytes, idx)
	joinGroupReq.ClientId, idx = readClientId(bytes, idx)
	if version == 6 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 1 {
		joinGroupReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 6 {
		joinGroupReq.GroupId, idx = readGroupId(bytes, idx)
	}
	joinGroupReq.SessionTimeout, idx = readInt(bytes, idx)
	joinGroupReq.RebalanceTimeout, idx = readInt(bytes, idx)
	if version == 1 {
		joinGroupReq.MemberId, idx = readMemberIdString(bytes, idx)
	} else if version == 6 {
		joinGroupReq.MemberId, idx = readMemberId(bytes, idx)
	}
	if version == 6 {
		joinGroupReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	}
	if version == 1 {
		joinGroupReq.ProtocolType, idx = readProtocolTypeString(bytes, idx)
	} else if version == 6 {
		joinGroupReq.ProtocolType, idx = readProtocolType(bytes, idx)
	}
	var length int
	if version == 1 {
		length, idx = readArrayLen(bytes, idx)
	} else if version == 6 {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	joinGroupReq.GroupProtocols = make([]*GroupProtocol, length)
	for i := 0; i < length; i++ {
		groupProtocol := GroupProtocol{}
		if version == 1 {
			groupProtocol.ProtocolName, idx = readProtocolNameString(bytes, idx)
		} else if version == 6 {
			groupProtocol.ProtocolName, idx = readProtocolName(bytes, idx)
		}
		if version == 1 {
			groupProtocol.ProtocolMetadata, idx = readString(bytes, idx)
		} else if version == 6 {
			groupProtocol.ProtocolMetadata, idx = readCompactString(bytes, idx)
		}
		if version == 6 {
			idx = readTaggedField(bytes, idx)
		}
		joinGroupReq.GroupProtocols[i] = &groupProtocol
	}
	if version == 6 {
		idx = readTaggedField(bytes, idx)
	}
	return joinGroupReq, nil
}

func (m *JoinGroupReq) BytesLength(containApiKeyVersion bool) int {
	version := m.ApiVersion
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(m.ClientId)
	if version == 1 {
		length += StrLen(m.GroupId)
	} else if version == 6 {
		length += LenTaggedField
		length += CompactStrLen(m.GroupId)
	}
	length += LenTimeout
	length += LenTimeout
	if version == 1 {
		length += StrLen(m.MemberId)
		length += StrLen(m.ProtocolType)
		length += LenArray
	} else if version == 6 {
		length += CompactStrLen(m.MemberId)
		length += CompactNullableStrLen(m.GroupInstanceId)
		length += CompactStrLen(m.ProtocolType)
		length += CompactArrayLen(len(m.GroupProtocols))
	}
	for _, groupProtocol := range m.GroupProtocols {
		if version == 1 {
			length += StrLen(groupProtocol.ProtocolName)
			length += StrLen(groupProtocol.ProtocolMetadata)
		} else if version == 6 {
			length += CompactStrLen(groupProtocol.ProtocolName)
			length += CompactStrLen(groupProtocol.ProtocolMetadata)
			length += LenTaggedField
		}
	}
	if version == 6 {
		length += LenTaggedField
	}
	return length
}

func (m *JoinGroupReq) Bytes(containApiKeyVersion bool) []byte {
	version := m.ApiVersion
	bytes := make([]byte, m.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, JoinGroup)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, m.CorrelationId)
	idx = putClientId(bytes, idx, m.ClientId)
	if version == 1 {
		idx = putGroupIdString(bytes, idx, m.GroupId)
	} else if version == 6 {
		idx = putTaggedField(bytes, idx)
		idx = putGroupId(bytes, idx, m.GroupId)
	}
	idx = putInt(bytes, idx, m.SessionTimeout)
	idx = putInt(bytes, idx, m.RebalanceTimeout)
	if version == 1 {
		idx = putMemberIdString(bytes, idx, m.MemberId)
		idx = putProtocolTypeString(bytes, idx, m.ProtocolType)
		idx = putArrayLen(bytes, idx, len(m.GroupProtocols))
	} else if version == 6 {
		idx = putMemberId(bytes, idx, m.MemberId)
		idx = putGroupInstanceId(bytes, idx, m.GroupInstanceId)
		idx = putProtocolType(bytes, idx, m.ProtocolType)
		idx = putCompactArrayLen(bytes, idx, len(m.GroupProtocols))
	}
	for _, groupProtocol := range m.GroupProtocols {
		if version == 1 {
			idx = putProtocolNameString(bytes, idx, groupProtocol.ProtocolName)
			idx = putString(bytes, idx, groupProtocol.ProtocolMetadata)
		} else if version == 6 {
			idx = putProtocolName(bytes, idx, groupProtocol.ProtocolName)
			idx = putCompactString(bytes, idx, groupProtocol.ProtocolMetadata)
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 6 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
