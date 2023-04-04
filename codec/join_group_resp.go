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

type JoinGroupResp struct {
	BaseResp
	ErrorCode    ErrorCode
	ThrottleTime int
	GenerationId int
	ProtocolType *string
	ProtocolName string
	LeaderId     string
	MemberId     string
	Members      []*Member
}

type Member struct {
	MemberId        string
	GroupInstanceId *string
	Metadata        []byte
}

func DecodeJoinGroupResp(bytes []byte, version int16) (resp *JoinGroupResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &JoinGroupResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 6 || version == 7 {
		idx = readTaggedField(bytes, idx)
		resp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	}
	resp.ErrorCode, idx = readErrorCode(bytes, idx)
	resp.GenerationId, idx = readGenerationId(bytes, idx)
	if version == 7 {
		resp.ProtocolType, idx = readProtocolTypeNullable(bytes, idx)
	}
	if version == 1 {
		resp.ProtocolName, idx = readProtocolNameString(bytes, idx)
	} else if version == 6 || version == 7 {
		resp.ProtocolName, idx = readProtocolName(bytes, idx)
	}
	if version == 1 {
		resp.LeaderId, idx = readGroupLeaderIdString(bytes, idx)
	} else if version == 6 || version == 7 {
		resp.LeaderId, idx = readGroupLeaderId(bytes, idx)
	}
	if version == 1 {
		resp.MemberId, idx = readMemberIdString(bytes, idx)
	} else if version == 6 || version == 7 {
		resp.MemberId, idx = readMemberId(bytes, idx)
	}
	var membersLen int
	if version == 1 {
		membersLen, idx = readArrayLen(bytes, idx)
	} else if version == 6 || version == 7 {
		membersLen, idx = readCompactArrayLen(bytes, idx)
	}
	for i := 0; i < membersLen; i++ {
		member := &Member{}
		if version == 1 {
			member.MemberId, idx = readMemberIdString(bytes, idx)
		} else if version == 6 || version == 7 {
			member.MemberId, idx = readMemberId(bytes, idx)
		}
		if version == 6 || version == 7 {
			member.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
		}
		if version == 1 {
			var metadataBytes []byte
			metadataBytes, idx = readBytes(bytes, idx)
			member.Metadata = metadataBytes
		} else if version == 6 || version == 7 {
			member.Metadata, idx = readCompactBytes(bytes, idx)
		}
		if version == 6 || version == 7 {
			idx = readTaggedField(bytes, idx)
		}
		resp.Members = append(resp.Members, member)
	}
	if version == 6 || version == 7 {
		idx = readTaggedField(bytes, idx)
	}
	return resp, nil
}

func (j *JoinGroupResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 6 || version == 7 {
		result += LenTaggedField + LenThrottleTime
	}
	result += LenErrorCode + LenGenerationId
	if version == 7 {
		result += CompactNullableStrLen(j.ProtocolType)
	}
	if version == 1 {
		result += StrLen(j.ProtocolName)
	} else if version == 6 || version == 7 {
		result += CompactStrLen(j.ProtocolName)
	}
	if version == 1 {
		result += StrLen(j.LeaderId)
	} else if version == 6 || version == 7 {
		result += CompactStrLen(j.LeaderId)
	}
	if version == 1 {
		result += StrLen(j.MemberId)
	} else if version == 6 || version == 7 {
		result += CompactStrLen(j.MemberId)
	}
	if version == 1 {
		result += LenArray
	} else if version == 6 || version == 7 {
		result += CompactArrayLen(len(j.Members))
	}
	for _, val := range j.Members {
		if version == 1 {
			result += StrLen(val.MemberId)
		} else if version == 6 || version == 7 {
			result += CompactStrLen(val.MemberId)
		}
		if version == 6 || version == 7 {
			result += CompactNullableStrLen(val.GroupInstanceId)
		}
		if version == 1 {
			result += BytesLen(val.Metadata)
		} else if version == 6 || version == 7 {
			result += CompactBytesLen(val.Metadata)
		}
		if version == 6 || version == 7 {
			result += LenTaggedField
		}
	}
	if version == 6 || version == 7 {
		result += LenTaggedField
	}
	return result
}

func (j *JoinGroupResp) Bytes(version int16, containLen bool) []byte {
	length := j.BytesLength(version)
	var bytes []byte
	idx := 0
	if containLen {
		bytes = make([]byte, length+4)
		idx = putInt(bytes, idx, length)
	} else {
		bytes = make([]byte, length)
	}

	idx = putCorrId(bytes, idx, j.CorrelationId)
	if version == 6 || version == 7 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, j.ThrottleTime)
	}
	idx = putErrorCode(bytes, idx, j.ErrorCode)
	idx = putGenerationId(bytes, idx, j.GenerationId)
	if version == 7 {
		idx = putProtocolTypeNullable(bytes, idx, j.ProtocolType)
	}
	if version == 1 {
		idx = putProtocolNameString(bytes, idx, j.ProtocolName)
	} else if version == 6 || version == 7 {
		idx = putProtocolName(bytes, idx, j.ProtocolName)
	}
	if version == 1 {
		idx = putGroupLeaderIdString(bytes, idx, j.LeaderId)
	} else if version == 6 || version == 7 {
		idx = putGroupLeaderId(bytes, idx, j.LeaderId)
	}
	if version == 1 {
		idx = putMemberIdString(bytes, idx, j.MemberId)
	} else if version == 6 || version == 7 {
		idx = putMemberId(bytes, idx, j.MemberId)
	}
	if version == 1 {
		idx = putArrayLen(bytes, idx, j.Members)
	} else if version == 6 || version == 7 {
		idx = putCompactArrayLen(bytes, idx, len(j.Members))
	}
	// put member
	for _, val := range j.Members {
		if version == 1 {
			idx = putMemberIdString(bytes, idx, val.MemberId)
		} else if version == 6 || version == 7 {
			idx = putMemberId(bytes, idx, val.MemberId)
		}
		if version == 6 || version == 7 {
			idx = putGroupInstanceId(bytes, idx, val.GroupInstanceId)
		}
		if version == 1 {
			idx = putBytes(bytes, idx, val.Metadata)
		} else if version == 6 || version == 7 {
			idx = putCompactBytes(bytes, idx, val.Metadata)
		}
		if version == 6 || version == 7 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 6 || version == 7 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
