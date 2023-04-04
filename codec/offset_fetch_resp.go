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

type OffsetFetchResp struct {
	BaseResp
	ThrottleTime  int
	ErrorCode     ErrorCode
	TopicRespList []*OffsetFetchTopicResp
}

type OffsetFetchTopicResp struct {
	Topic             string
	PartitionRespList []*OffsetFetchPartitionResp
}

type OffsetFetchPartitionResp struct {
	PartitionId int
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
	ErrorCode   ErrorCode
}

func DecodeOffsetFetchResp(bytes []byte, version int16) (resp *OffsetFetchResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &OffsetFetchResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 6 || version == 7 {
		idx = readTaggedField(bytes, idx)
		resp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	}
	var topicRespLen int
	if version == 1 {
		topicRespLen, idx = readArrayLen(bytes, idx)
	} else if version == 6 || version == 7 {
		topicRespLen, idx = readCompactArrayLen(bytes, idx)
	}
	if topicRespLen > len(bytes) {
		return nil, InvalidProtocolContent
	}
	resp.TopicRespList = make([]*OffsetFetchTopicResp, topicRespLen)
	for i := 0; i < topicRespLen; i++ {
		topic := &OffsetFetchTopicResp{}
		if version == 1 {
			topic.Topic, idx = readTopicString(bytes, idx)
		} else if version == 6 || version == 7 {
			topic.Topic, idx = readTopic(bytes, idx)
		}
		var partitionRespLen int
		if version == 1 {
			partitionRespLen, idx = readArrayLen(bytes, idx)
		} else if version == 6 || version == 7 {
			partitionRespLen, idx = readCompactArrayLen(bytes, idx)
		}
		if partitionRespLen > len(bytes) {
			return nil, InvalidProtocolContent
		}
		topic.PartitionRespList = make([]*OffsetFetchPartitionResp, partitionRespLen)
		for j := 0; j < partitionRespLen; j++ {
			partitionResp := &OffsetFetchPartitionResp{}
			partitionResp.PartitionId, idx = readPartitionId(bytes, idx)
			partitionResp.Offset, idx = readOffset(bytes, idx)
			if version == 6 || version == 7 {
				partitionResp.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			}
			if version == 1 {
				partitionResp.Metadata, idx = readNullableString(bytes, idx)
			} else if version == 6 || version == 7 {
				partitionResp.Metadata, idx = readMetadata(bytes, idx)
			}
			partitionResp.ErrorCode, idx = readErrorCode(bytes, idx)
			if version == 6 || version == 7 {
				idx = readTaggedField(bytes, idx)
			}
			topic.PartitionRespList[j] = partitionResp
		}
		if version == 6 || version == 7 {
			idx = readTaggedField(bytes, idx)
		}
		resp.TopicRespList[i] = topic
	}
	if version == 6 || version == 7 {
		resp.ErrorCode, idx = readErrorCode(bytes, idx)
		idx = readTaggedField(bytes, idx)
	}
	return resp, nil
}

func (o *OffsetFetchResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 6 || version == 7 {
		result += LenTaggedField + LenThrottleTime
	}
	if version == 1 {
		result += LenArray
	} else if version == 6 || version == 7 {
		result += CompactArrayLen(len(o.TopicRespList))
	}
	for _, val := range o.TopicRespList {
		if version == 1 {
			result += StrLen(val.Topic)
		} else if version == 6 || version == 7 {
			result += CompactStrLen(val.Topic)
		}
		if version == 1 {
			result += LenArray
		} else if version == 6 || version == 7 {
			result += CompactArrayLen(len(val.PartitionRespList))
		}
		for _, val2 := range val.PartitionRespList {
			result += LenPartitionId + LenOffset
			if version == 6 || version == 7 {
				result += LenLeaderEpoch
			}
			if version == 1 {
				result += NullableStrLen(val2.Metadata)
			} else if version == 6 || version == 7 {
				result += CompactNullableStrLen(val2.Metadata)
			}
			result += LenErrorCode
			if version == 6 || version == 7 {
				result += LenTaggedField
			}
		}
		if version == 6 || version == 7 {
			result += LenTaggedField
		}
	}
	if version == 6 || version == 7 {
		result += LenErrorCode + LenTaggedField
	}
	return result
}

func (o *OffsetFetchResp) Bytes(version int16, containLen bool) []byte {
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
	if version == 6 || version == 7 {
		idx = putTaggedField(bytes, idx)
		idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	}
	if version == 1 {
		idx = putArrayLen(bytes, idx, o.TopicRespList)
	} else if version == 6 || version == 7 {
		idx = putCompactArrayLen(bytes, idx, len(o.TopicRespList))
	}
	for _, topic := range o.TopicRespList {
		if version == 1 {
			idx = putTopicString(bytes, idx, topic.Topic)
		} else if version == 6 || version == 7 {
			idx = putTopic(bytes, idx, topic.Topic)
		}
		if version == 1 {
			idx = putArrayLen(bytes, idx, topic.PartitionRespList)
		} else if version == 6 || version == 7 {
			idx = putCompactArrayLen(bytes, idx, len(topic.PartitionRespList))
		}
		for _, partition := range topic.PartitionRespList {
			idx = putPartitionId(bytes, idx, partition.PartitionId)
			idx = putOffset(bytes, idx, partition.Offset)
			if version == 6 || version == 7 {
				idx = putLeaderEpoch(bytes, idx, partition.LeaderEpoch)
			}
			if version == 1 {
				idx = putNullableString(bytes, idx, partition.Metadata)
			} else if version == 6 || version == 7 {
				idx = putMetadata(bytes, idx, partition.Metadata)
			}
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			if version == 6 || version == 7 {
				idx = putTaggedField(bytes, idx)
			}
		}
		if version == 6 || version == 7 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 6 || version == 7 {
		idx = putErrorCode(bytes, idx, o.ErrorCode)
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
