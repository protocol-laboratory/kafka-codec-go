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

type OffsetCommitResp struct {
	BaseResp
	ThrottleTime  int
	TopicRespList []*OffsetCommitTopicResp
}

type OffsetCommitTopicResp struct {
	Topic             string
	PartitionRespList []*OffsetCommitPartitionResp
}

type OffsetCommitPartitionResp struct {
	PartitionId int
	ErrorCode   ErrorCode
}

func DecodeOffsetCommitResp(bytes []byte, version int16) (resp *OffsetCommitResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &OffsetCommitResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 8 {
		idx = readTaggedField(bytes, idx)
		resp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	}
	var topicRespLen int
	if version == 2 {
		topicRespLen, idx = readArrayLen(bytes, idx)
	} else if version == 8 {
		topicRespLen, idx = readCompactArrayLen(bytes, idx)
	}
	if topicRespLen > len(bytes) {
		return nil, InvalidProtocolContent
	}
	resp.TopicRespList = make([]*OffsetCommitTopicResp, topicRespLen)
	for i := 0; i < topicRespLen; i++ {
		topicResp := &OffsetCommitTopicResp{}
		if version == 2 {
			topicResp.Topic, idx = readTopicString(bytes, idx)
		} else if version == 8 {
			topicResp.Topic, idx = readTopic(bytes, idx)
		}
		var partitionRespLen int
		if version == 2 {
			partitionRespLen, idx = readArrayLen(bytes, idx)
		} else if version == 8 {
			partitionRespLen, idx = readCompactArrayLen(bytes, idx)
		}
		if partitionRespLen > len(bytes) {
			return nil, InvalidProtocolContent
		}
		topicResp.PartitionRespList = make([]*OffsetCommitPartitionResp, partitionRespLen)
		for j := 0; j < partitionRespLen; j++ {
			partitionResp := &OffsetCommitPartitionResp{}
			partitionResp.PartitionId, idx = readPartitionId(bytes, idx)
			partitionResp.ErrorCode, idx = readErrorCode(bytes, idx)
			if version == 8 {
				idx = readTaggedField(bytes, idx)
			}
			topicResp.PartitionRespList[j] = partitionResp
		}
		if version == 8 {
			idx = readTaggedField(bytes, idx)
		}
		resp.TopicRespList[i] = topicResp
	}
	if version == 8 {
		idx = readTaggedField(bytes, idx)
	}
	return resp, nil
}

func (o *OffsetCommitResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 8 {
		result += LenTaggedField + LenThrottleTime
	}
	if version == 2 {
		result += LenArray
	} else if version == 8 {
		result += CompactArrayLen(len(o.TopicRespList))
	}
	for _, val := range o.TopicRespList {
		if version == 2 {
			result += StrLen(val.Topic)
		} else if version == 8 {
			result += CompactStrLen(val.Topic)
		}
		if version == 2 {
			result += LenArray
		} else if version == 8 {
			result += CompactArrayLen(len(val.PartitionRespList))
		}
		for range val.PartitionRespList {
			result += LenPartitionId + LenErrorCode
			if version == 8 {
				result += LenTaggedField
			}
		}
		if version == 8 {
			result += LenTaggedField
		}
	}
	if version == 8 {
		result += LenTaggedField
	}
	return result
}

func (o *OffsetCommitResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	if version == 8 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	}
	if version == 2 {
		idx = putArrayLen(bytes, idx, o.TopicRespList)
	} else if version == 8 {
		idx = putCompactArrayLen(bytes, idx, len(o.TopicRespList))
	}
	for _, topic := range o.TopicRespList {
		if version == 2 {
			idx = putTopicString(bytes, idx, topic.Topic)
		} else if version == 8 {
			idx = putTopic(bytes, idx, topic.Topic)
		}
		if version == 2 {
			idx = putArrayLen(bytes, idx, topic.PartitionRespList)
		} else if version == 8 {
			idx = putCompactArrayLen(bytes, idx, len(topic.PartitionRespList))
		}
		for _, partition := range topic.PartitionRespList {
			idx = putInt(bytes, idx, partition.PartitionId)
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			if version == 8 {
				idx = putTaggedField(bytes, idx)
			}
		}
		if version == 8 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 8 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
