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
	if version == 5 {
		offsetReq.IsolationLevel, idx = readIsolationLevel(bytes, idx)
	}
	var length int
	length, idx = readInt(bytes, idx)
	offsetReq.TopicReqList = make([]*ListOffsetsTopic, length)
	for i := 0; i < length; i++ {
		topic := &ListOffsetsTopic{}
		topic.Topic, idx = readTopicString(bytes, idx)
		var partitionLength int
		partitionLength, idx := readInt(bytes, idx)
		topic.PartitionReqList = make([]*ListOffsetsPartition, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partition := &ListOffsetsPartition{}
			partition.PartitionId, idx = readInt(bytes, idx)
			if version == 5 {
				partition.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			}
			partition.Time, idx = readTime(bytes, idx)
			topic.PartitionReqList[j] = partition
		}
		offsetReq.TopicReqList[i] = topic
	}
	return offsetReq, nil
}
