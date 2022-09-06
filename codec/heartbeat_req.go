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

type HeartbeatReq struct {
	BaseReq
	GroupId         string
	GenerationId    int
	MemberId        string
	GroupInstanceId *string
}

func DecodeHeartbeatReq(bytes []byte, version int16) (heartBeatReq *HeartbeatReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			heartBeatReq = nil
		}
	}()
	heartBeatReq = &HeartbeatReq{}
	heartBeatReq.ApiVersion = version
	idx := 0
	heartBeatReq.CorrelationId, idx = readCorrId(bytes, idx)
	heartBeatReq.ClientId, idx = readClientId(bytes, idx)
	if version == 4 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 0 {
		heartBeatReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 4 {
		heartBeatReq.GroupId, idx = readGroupId(bytes, idx)
	}
	heartBeatReq.GenerationId, idx = readGenerationId(bytes, idx)
	if version == 0 {
		heartBeatReq.MemberId, idx = readMemberIdString(bytes, idx)
	} else if version == 4 {
		heartBeatReq.MemberId, idx = readMemberId(bytes, idx)
	}
	if version == 4 {
		heartBeatReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
		idx = readTaggedField(bytes, idx)
	}
	return heartBeatReq, nil
}

func (h *HeartbeatReq) BytesLength(containLen bool, containApiKeyVersion bool) int {
	version := h.ApiVersion
	length := 0
	if containLen {
		length += LenLength
	}
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(h.ClientId)
	if version == 4 {
		length += LenTaggedField
	}
	if version == 0 {
		length += StrLen(h.GroupId)
	} else if version == 4 {
		length += CompactStrLen(h.GroupId)
	}
	length += LenGenerationId
	if version == 0 {
		length += StrLen(h.MemberId)
	} else if version == 4 {
		length += CompactStrLen(h.MemberId)
	}
	if version == 4 {
		length += CompactNullableStrLen(h.GroupInstanceId)
		length += LenTaggedField
	}
	return length
}

func (h *HeartbeatReq) Bytes(containLen bool, containApiKeyVersion bool) []byte {
	version := h.ApiVersion
	bytes := make([]byte, h.BytesLength(containLen, containApiKeyVersion))
	idx := 0
	if containLen {
		idx = putLength(bytes, idx)
	}
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, Heartbeat)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, h.CorrelationId)
	idx = putClientId(bytes, idx, h.ClientId)
	if version == 4 {
		idx = putTaggedField(bytes, idx)
	}
	if version == 0 {
		idx = putGroupIdString(bytes, idx, h.GroupId)
	} else if version == 4 {
		idx = putGroupId(bytes, idx, h.GroupId)
	}
	idx = putGenerationId(bytes, idx, h.GenerationId)
	if version == 0 {
		idx = putMemberIdString(bytes, idx, h.MemberId)
	} else if version == 4 {
		idx = putMemberId(bytes, idx, h.MemberId)
	}
	if version == 4 {
		idx = putGroupInstanceId(bytes, idx, h.GroupInstanceId)
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
