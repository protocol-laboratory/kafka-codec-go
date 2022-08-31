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

type OffsetCommitReq struct {
	BaseReq
	GroupId         string
	GenerationId    int
	MemberId        string
	RetentionTime   int64
	GroupInstanceId *string
	TopicReqList    []*OffsetCommitTopicReq
}

type OffsetCommitTopicReq struct {
	Topic            string
	PartitionReqList []*OffsetCommitPartitionReq
}

type OffsetCommitPartitionReq struct {
	PartitionId int
	Offset      int64
	LeaderEpoch int32
	Metadata    string
}

func DecodeOffsetCommitReq(bytes []byte, version int16) (offsetReq *OffsetCommitReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			offsetReq = nil
		}
	}()
	offsetReq = &OffsetCommitReq{}
	offsetReq.ApiVersion = version
	idx := 0
	offsetReq.CorrelationId, idx = readCorrId(bytes, idx)
	offsetReq.ClientId, idx = readClientId(bytes, idx)
	if version == 8 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 2 {
		offsetReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 8 {
		offsetReq.GroupId, idx = readGroupId(bytes, idx)
	}
	offsetReq.GenerationId, idx = readGenerationId(bytes, idx)
	if version == 2 {
		offsetReq.MemberId, idx = readMemberIdString(bytes, idx)
	} else if version == 8 {
		offsetReq.MemberId, idx = readMemberId(bytes, idx)
	}
	if version == 2 {
		offsetReq.RetentionTime, idx = readRetentionTime(bytes, idx)
	}
	if version == 8 {
		offsetReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	}
	var length int
	if version == 2 {
		length, idx = readArrayLen(bytes, idx)
	} else if version == 8 {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	if length > len(bytes) {
		return nil, InvalidProtocolContent
	}
	offsetReq.TopicReqList = make([]*OffsetCommitTopicReq, length)
	for i := 0; i < length; i++ {
		topic := &OffsetCommitTopicReq{}
		if version == 2 {
			topic.Topic, idx = readTopicString(bytes, idx)
		} else if version == 8 {
			topic.Topic, idx = readTopic(bytes, idx)
		}
		var partitionLength int
		if version == 2 {
			partitionLength, idx = readArrayLen(bytes, idx)
		} else if version == 8 {
			partitionLength, idx = readCompactArrayLen(bytes, idx)
		}
		if partitionLength > len(bytes) {
			return nil, InvalidProtocolContent
		}
		topic.PartitionReqList = make([]*OffsetCommitPartitionReq, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partition := &OffsetCommitPartitionReq{}
			partition.PartitionId, idx = readPartitionId(bytes, idx)
			partition.Offset, idx = readOffset(bytes, idx)
			if version == 8 {
				partition.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			}
			if version == 2 {
				partition.Metadata, idx = readString(bytes, idx)
			} else if version == 8 {
				partition.Metadata, idx = readCompactString(bytes, idx)
			}
			if version == 8 {
				idx = readTaggedField(bytes, idx)
			}
			topic.PartitionReqList[j] = partition
		}
		if version == 8 {
			idx = readTaggedField(bytes, idx)
		}
		offsetReq.TopicReqList[i] = topic
	}
	if version == 8 {
		idx = readTaggedField(bytes, idx)
	}
	return offsetReq, nil
}

func (o *OffsetCommitReq) BytesLength(containApiKeyVersion bool) int {
	version := o.ApiVersion
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(o.ClientId)
	if version == 8 {
		length += LenTaggedField
	}
	if version == 2 {
		length += StrLen(o.GroupId)
	} else if version == 8 {
		length += CompactStrLen(o.GroupId)
	}
	length += LenGenerationId
	if version == 2 {
		length += StrLen(o.MemberId)
	} else if version == 8 {
		length += CompactStrLen(o.MemberId)
	}
	if version == 2 {
		length += LenTime
	}
	if version == 8 {
		length += CompactNullableStrLen(o.GroupInstanceId)
	}
	if version == 2 {
		length += LenArray
	} else if version == 8 {
		length += CompactArrayLen(len(o.TopicReqList))
	}
	for _, topicReq := range o.TopicReqList {
		if version == 2 {
			length += StrLen(topicReq.Topic)
			length += LenArray
		} else if version == 8 {
			length += CompactStrLen(topicReq.Topic)
			length += CompactArrayLen(len(topicReq.PartitionReqList))
		}
		for _, partitionReq := range topicReq.PartitionReqList {
			length += LenPartitionId
			length += LenOffset
			if version == 8 {
				length += LenLeaderEpoch
			}
			if version == 2 {
				length += StrLen(partitionReq.Metadata)
			} else if version == 8 {
				length += CompactStrLen(partitionReq.Metadata)
			}
			if version == 8 {
				length += LenTaggedField
			}
		}
		if version == 8 {
			length += LenTaggedField
		}
	}
	if version == 8 {
		length += LenTaggedField
	}
	return length
}

func (o *OffsetCommitReq) Bytes(containApiKeyVersion bool) []byte {
	version := o.ApiVersion
	bytes := make([]byte, o.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, OffsetCommit)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, o.CorrelationId)
	idx = putClientId(bytes, idx, o.ClientId)
	if version == 8 {
		idx = putTaggedField(bytes, idx)
	}
	if version == 2 {
		idx = putGroupIdString(bytes, idx, o.GroupId)
	} else if version == 8 {
		idx = putGroupId(bytes, idx, o.GroupId)
	}
	idx = putGenerationId(bytes, idx, o.GenerationId)
	if version == 2 {
		idx = putMemberIdString(bytes, idx, o.MemberId)
	} else if version == 8 {
		idx = putMemberId(bytes, idx, o.MemberId)
	}
	if version == 2 {
		idx = putRetentionTime(bytes, idx, o.RetentionTime)
	}
	if version == 8 {
		idx = putGroupInstanceId(bytes, idx, o.GroupInstanceId)
	}
	if version == 2 {
		idx = putArrayLen(bytes, idx, o.TopicReqList)
	} else if version == 8 {
		idx = putCompactArrayLen(bytes, idx, len(o.TopicReqList))
	}
	for _, topicReq := range o.TopicReqList {
		if version == 2 {
			idx = putTopicString(bytes, idx, topicReq.Topic)
			idx = putArrayLen(bytes, idx, topicReq.PartitionReqList)
		} else if version == 8 {
			idx = putTopic(bytes, idx, topicReq.Topic)
			idx = putCompactArrayLen(bytes, idx, len(topicReq.PartitionReqList))
		}
		for _, partitionReq := range topicReq.PartitionReqList {
			idx = putPartitionId(bytes, idx, partitionReq.PartitionId)
			idx = putOffset(bytes, idx, partitionReq.Offset)
			if version == 8 {
				idx = putLeaderEpoch(bytes, idx, partitionReq.LeaderEpoch)
			}
			if version == 2 {
				idx = putString(bytes, idx, partitionReq.Metadata)
			} else if version == 8 {
				idx = putCompactString(bytes, idx, partitionReq.Metadata)
			}
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
