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

func (o *ListOffsetsResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	if version == 5 || version == 6 {
		idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	}
	if version == 1 || version == 5 {
		idx = putArrayLen(bytes, idx, len(o.TopicRespList))
	} else if version == 6 {
		idx = putTaggedField(bytes, idx)
		idx = putCompactArrayLen(bytes, idx, len(o.TopicRespList))
	}
	for _, topic := range o.TopicRespList {
		if version == 1 || version == 5 {
			idx = putTopicString(bytes, idx, topic.Topic)
			idx = putArrayLen(bytes, idx, len(topic.PartitionRespList))
		} else if version == 6 {
			idx = putTopic(bytes, idx, topic.Topic)
			idx = putCompactArrayLen(bytes, idx, len(topic.PartitionRespList))
		}
		for _, p := range topic.PartitionRespList {
			idx = putInt(bytes, idx, p.PartitionId)
			idx = putErrorCode(bytes, idx, p.ErrorCode)
			idx = putInt64(bytes, idx, p.Timestamp)
			idx = putInt64(bytes, idx, p.Offset)
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
