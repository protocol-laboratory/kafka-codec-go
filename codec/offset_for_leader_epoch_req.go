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

type OffsetForLeaderEpochReq struct {
	BaseReq
	ReplicaId    int32
	TopicReqList []*OffsetLeaderEpochTopicReq
}

type OffsetLeaderEpochTopicReq struct {
	Topic            string
	PartitionReqList []*OffsetLeaderEpochPartitionReq
}

type OffsetLeaderEpochPartitionReq struct {
	PartitionId        int
	CurrentLeaderEpoch int32
	LeaderEpoch        int32
}

func DecodeOffsetForLeaderEpochReq(bytes []byte, version int16) (leaderEpochReq *OffsetForLeaderEpochReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			leaderEpochReq = nil
		}
	}()
	leaderEpochReq = &OffsetForLeaderEpochReq{}
	leaderEpochReq.ApiVersion = version
	idx := 0
	leaderEpochReq.CorrelationId, idx = readCorrId(bytes, idx)
	leaderEpochReq.ClientId, idx = readClientId(bytes, idx)
	leaderEpochReq.ReplicaId, idx = readReplicaId(bytes, idx)
	var length int
	length, idx = readArrayLen(bytes, idx)
	leaderEpochReq.TopicReqList = make([]*OffsetLeaderEpochTopicReq, length)
	for i := 0; i < length; i++ {
		topic := OffsetLeaderEpochTopicReq{}
		topic.Topic, idx = readTopicString(bytes, idx)
		var partitionLen int
		partitionLen, idx = readArrayLen(bytes, idx)
		topic.PartitionReqList = make([]*OffsetLeaderEpochPartitionReq, partitionLen)
		for j := 0; j < partitionLen; j++ {
			o := &OffsetLeaderEpochPartitionReq{}
			o.PartitionId, idx = readPartitionId(bytes, idx)
			o.CurrentLeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			o.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			topic.PartitionReqList[j] = o
		}
		leaderEpochReq.TopicReqList[i] = &topic
	}
	return leaderEpochReq, nil
}

func (o *OffsetForLeaderEpochReq) BytesLength(containApiKeyVersion bool) int {
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(o.ClientId)
	length += LenReplicaId
	length += LenArray
	for _, topicReq := range o.TopicReqList {
		length += StrLen(topicReq.Topic)
		length += LenArray
		for range topicReq.PartitionReqList {
			length += LenPartitionId
			length += LenLeaderEpoch
			length += LenLeaderEpoch
		}
	}
	return length
}

func (o *OffsetForLeaderEpochReq) Bytes(containApiKeyVersion bool) []byte {
	version := o.ApiVersion
	bytes := make([]byte, o.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, OffsetForLeaderEpoch)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, o.CorrelationId)
	idx = putClientId(bytes, idx, o.ClientId)
	idx = putReplicaId(bytes, idx, o.ReplicaId)
	idx = putArrayLen(bytes, idx, len(o.TopicReqList))
	for _, topicReq := range o.TopicReqList {
		idx = putTopicString(bytes, idx, topicReq.Topic)
		idx = putArrayLen(bytes, idx, len(topicReq.PartitionReqList))
		for _, partitionReq := range topicReq.PartitionReqList {
			idx = putPartitionId(bytes, idx, partitionReq.PartitionId)
			idx = putLeaderEpoch(bytes, idx, partitionReq.CurrentLeaderEpoch)
			idx = putLeaderEpoch(bytes, idx, partitionReq.LeaderEpoch)
		}
	}
	return bytes
}
