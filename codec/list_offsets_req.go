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

type ListOffsetsReq struct {
	BaseReq
	ReplicaId      int32
	IsolationLevel byte
	TopicReqList   []*ListOffsetsTopic
}

type ListOffsetsTopic struct {
	Topic            string
	PartitionReqList []*ListOffsetsPartition
}

type ListOffsetsPartition struct {
	PartitionId int
	LeaderEpoch int32
	Time        int64
}

func DecodeListOffsetsReq(bytes []byte, version int16) (offsetReq *ListOffsetsReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			offsetReq = nil
		}
	}()
	offsetReq = &ListOffsetsReq{}
	offsetReq.ApiVersion = version
	idx := 0
	offsetReq.CorrelationId, idx = readCorrId(bytes, idx)
	offsetReq.ClientId, idx = readClientId(bytes, idx)
	offsetReq.ReplicaId, idx = readReplicaId(bytes, idx)
	if version == 5 || version == 6 {
		offsetReq.IsolationLevel, idx = readIsolationLevel(bytes, idx)
	}
	var length int
	if version == 1 || version == 5 {
		length, idx = readInt(bytes, idx)
	} else if version == 6 {
		idx = readTaggedField(bytes, idx)
		length, idx = readCompactArrayLen(bytes, idx)
	}
	for i := 0; i < length; i++ {
		topic := &ListOffsetsTopic{}
		if version == 1 || version == 5 {
			topic.Topic, idx = readTopicString(bytes, idx)
		} else if version == 6 {
			topic.Topic, idx = readTopic(bytes, idx)
		}
		var partitionLength int
		if version == 1 || version == 5 {
			partitionLength, idx = readInt(bytes, idx)
		} else if version == 6 {
			partitionLength, idx = readCompactArrayLen(bytes, idx)
		}
		for j := 0; j < partitionLength; j++ {
			partition := &ListOffsetsPartition{}
			partition.PartitionId, idx = readInt(bytes, idx)
			if version == 5 || version == 6 {
				partition.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			}
			partition.Time, idx = readTime(bytes, idx)
			if version == 6 {
				idx = readTaggedField(bytes, idx)
			}
			topic.PartitionReqList = append(topic.PartitionReqList, partition)
		}
		if version == 6 {
			idx = readTaggedField(bytes, idx)
		}
		offsetReq.TopicReqList = append(offsetReq.TopicReqList, topic)
	}
	if version == 6 {
		idx = readTaggedField(bytes, idx)
	}
	return offsetReq, nil
}

func (l *ListOffsetsReq) BytesLength(containLen bool, containApiKeyVersion bool) int {
	version := l.ApiVersion
	length := 0
	if containLen {
		length += LenLength
	}
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(l.ClientId)
	length += LenReplicaId
	if version == 5 || version == 6 {
		length += LenIsolationLevel
	}
	if version == 1 || version == 5 {
		length += LenArray
	} else if version == 6 {
		length += LenTaggedField
		length += CompactArrayLen(len(l.TopicReqList))
	}
	for _, topicReq := range l.TopicReqList {
		if version == 1 || version == 5 {
			length += StrLen(topicReq.Topic)
		} else if version == 6 {
			length += CompactStrLen(topicReq.Topic)
		}
		if version == 1 || version == 5 {
			length += LenArray
		} else if version == 6 {
			length += CompactArrayLen(len(topicReq.PartitionReqList))
		}
		for range topicReq.PartitionReqList {
			length += LenPartitionId
			if version == 5 || version == 6 {
				length += LenLeaderEpoch
			}
			length += LenTime
			if version == 6 {
				length += LenTaggedField
			}
		}
		if version == 6 {
			length += LenTaggedField
		}
	}
	if version == 6 {
		length += LenTaggedField
	}
	return length
}

func (l *ListOffsetsReq) Bytes(containLen bool, containApiKeyVersion bool) []byte {
	version := l.ApiVersion
	bytes := make([]byte, l.BytesLength(containLen, containApiKeyVersion))
	idx := 0
	if containLen {
		idx = putLength(bytes, idx)
	}
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, ListOffsets)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, l.CorrelationId)
	idx = putClientId(bytes, idx, l.ClientId)
	idx = putReplicaId(bytes, idx, l.ReplicaId)
	if version == 5 || version == 6 {
		idx = putIsolationLevel(bytes, idx, l.IsolationLevel)
	}
	if version == 1 || version == 5 {
		idx = putArrayLen(bytes, idx, l.TopicReqList)
	} else if version == 6 {
		idx = putTaggedField(bytes, idx)
		idx = putCompactArrayLen(bytes, idx, len(l.TopicReqList))
	}
	for _, topicReq := range l.TopicReqList {
		if version == 1 || version == 5 {
			idx = putTopicString(bytes, idx, topicReq.Topic)
		} else if version == 6 {
			idx = putTopic(bytes, idx, topicReq.Topic)
		}
		if version == 1 || version == 5 {
			idx = putArrayLen(bytes, idx, topicReq.PartitionReqList)
		} else if version == 6 {
			idx = putCompactArrayLen(bytes, idx, len(topicReq.PartitionReqList))
		}
		for _, partition := range topicReq.PartitionReqList {
			idx = putPartitionId(bytes, idx, partition.PartitionId)
			if version == 5 || version == 6 {
				idx = putLeaderEpoch(bytes, idx, partition.LeaderEpoch)
			}
			idx = putTime(bytes, idx, partition.Time)
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
