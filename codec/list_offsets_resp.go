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

type ListOffsetsResp struct {
	BaseResp
	ErrorCode     ErrorCode
	ThrottleTime  int
	TopicRespList []*ListOffsetsTopicResp
}

type ListOffsetsTopicResp struct {
	Topic             string
	PartitionRespList []*ListOffsetsPartitionResp
}

type ListOffsetsPartitionResp struct {
	PartitionId int
	ErrorCode   ErrorCode
	Timestamp   int64
	Offset      int64
	LeaderEpoch int32
}

func DecodeListOffsetsResp(bytes []byte, version int16) (resp *ListOffsetsResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &ListOffsetsResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 5 || version == 6 {
		resp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	}
	var topicRespLen int
	if version == 1 || version == 5 {
		topicRespLen, idx = readArrayLen(bytes, idx)
	} else if version == 6 {
		idx = readTaggedField(bytes, idx)
		topicRespLen, idx = readCompactArrayLen(bytes, idx)
	}
	for i := 0; i < topicRespLen; i++ {
		topicResp := &ListOffsetsTopicResp{}
		if version == 1 || version == 5 {
			topicResp.Topic, idx = readTopicString(bytes, idx)
		} else if version == 6 {
			topicResp.Topic, idx = readTopic(bytes, idx)
		}
		var partitionRespLen int
		if version == 1 || version == 5 {
			partitionRespLen, idx = readArrayLen(bytes, idx)
		} else if version == 6 {
			partitionRespLen, idx = readCompactArrayLen(bytes, idx)
		}
		for j := 0; j < partitionRespLen; j++ {
			partitionResp := &ListOffsetsPartitionResp{}
			partitionResp.PartitionId, idx = readPartitionId(bytes, idx)
			partitionResp.ErrorCode, idx = readErrorCode(bytes, idx)
			partitionResp.Timestamp, idx = readTime(bytes, idx)
			partitionResp.Offset, idx = readOffset(bytes, idx)
			if version == 5 || version == 6 {
				partitionResp.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			}
			if version == 6 {
				idx = readTaggedField(bytes, idx)
			}
			topicResp.PartitionRespList = append(topicResp.PartitionRespList, partitionResp)
		}
		if version == 6 {
			idx = readTaggedField(bytes, idx)
		}
		resp.TopicRespList = append(resp.TopicRespList, topicResp)
	}
	if version == 6 {
		idx = readTaggedField(bytes, idx)
	}
	return resp, nil
}

func (o *ListOffsetsResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 5 || version == 6 {
		result += LenThrottleTime
	}
	if version == 1 || version == 5 {
		result += LenArray
	} else if version == 6 {
		result += LenTaggedField
		result += CompactArrayLen(len(o.TopicRespList))
	}
	for _, val := range o.TopicRespList {
		if version == 1 || version == 5 {
			result += StrLen(val.Topic) + LenArray
		} else if version == 6 {
			result += CompactStrLen(val.Topic)
			result += CompactArrayLen(len(val.PartitionRespList))
		}
		for range val.PartitionRespList {
			result += LenPartitionId + LenErrorCode + LenTime + LenOffset
			if version == 5 || version == 6 {
				result += LenLeaderEpoch
			}
			if version == 6 {
				result += LenTaggedField
			}
		}
		if version == 6 {
			result += LenTaggedField
		}
	}
	if version == 6 {
		result += LenTaggedField
	}
	return result
}

func (o *ListOffsetsResp) Bytes(version int16, containLen bool) []byte {
	length := o.BytesLength(version)
	var bytes []byte
	idx := 0
	if containLen {
		bytes = make([]byte, length+4)
		idx = putInt(bytes, idx, length)
	} else {
		bytes = make([]byte, length)
	}

	idx = putCorrId(bytes, idx, o.CorrelationId)
	if version == 5 || version == 6 {
		idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	}
	if version == 1 || version == 5 {
		idx = putArrayLen(bytes, idx, o.TopicRespList)
	} else if version == 6 {
		idx = putTaggedField(bytes, idx)
		idx = putCompactArrayLen(bytes, idx, len(o.TopicRespList))
	}
	for _, topic := range o.TopicRespList {
		if version == 1 || version == 5 {
			idx = putTopicString(bytes, idx, topic.Topic)
			idx = putArrayLen(bytes, idx, topic.PartitionRespList)
		} else if version == 6 {
			idx = putTopic(bytes, idx, topic.Topic)
			idx = putCompactArrayLen(bytes, idx, len(topic.PartitionRespList))
		}
		for _, p := range topic.PartitionRespList {
			idx = putPartitionId(bytes, idx, p.PartitionId)
			idx = putErrorCode(bytes, idx, p.ErrorCode)
			idx = putTime(bytes, idx, p.Timestamp)
			idx = putOffset(bytes, idx, p.Offset)
			if version == 5 || version == 6 {
				idx = putLeaderEpoch(bytes, idx, p.LeaderEpoch)
			}
			if version == 6 {
				idx = putTaggedField(bytes, idx)
			}
		}
		if version == 6 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 6 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
