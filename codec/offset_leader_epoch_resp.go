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

type OffsetForLeaderEpochResp struct {
	BaseResp
	ThrottleTime  int
	TopicRespList []*OffsetForLeaderEpochTopicResp
}

type OffsetForLeaderEpochTopicResp struct {
	Topic             string
	PartitionRespList []*OffsetForLeaderEpochPartitionResp
}

type OffsetForLeaderEpochPartitionResp struct {
	ErrorCode   ErrorCode
	PartitionId int
	LeaderEpoch int32
	Offset      int64
}

func DecodeOffsetForLeaderEpochResp(bytes []byte, version int16) (resp *OffsetForLeaderEpochResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &OffsetForLeaderEpochResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	resp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	var topicRespLen int
	topicRespLen, idx = readArrayLen(bytes, idx)
	for i := 0; i < topicRespLen; i++ {
		topicResp := &OffsetForLeaderEpochTopicResp{}
		topicResp.Topic, idx = readTopicString(bytes, idx)
		var partitionLen int
		partitionLen, idx = readArrayLen(bytes, idx)
		for i := 0; i < partitionLen; i++ {
			partitionResp := &OffsetForLeaderEpochPartitionResp{}
			partitionResp.ErrorCode, idx = readErrorCode(bytes, idx)
			partitionResp.PartitionId, idx = readPartitionId(bytes, idx)
			partitionResp.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			partitionResp.Offset, idx = readOffset(bytes, idx)
			topicResp.PartitionRespList = append(topicResp.PartitionRespList, partitionResp)
		}
		resp.TopicRespList = append(resp.TopicRespList, topicResp)
	}
	return resp, nil
}

func (o *OffsetForLeaderEpochResp) BytesLength(version int16) int {
	result := LenCorrId
	result += LenThrottleTime
	result += LenArray
	for _, val := range o.TopicRespList {
		result += StrLen(val.Topic)
		result += LenArray
		for range val.PartitionRespList {
			result += LenErrorCode
			result += LenPartitionId
			result += LenLeaderEpoch
			result += LenOffset
		}
	}
	return result
}

func (o *OffsetForLeaderEpochResp) Bytes(version int16) []byte {
	bytes := make([]byte, o.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, o.CorrelationId)
	idx = putThrottleTime(bytes, idx, o.ThrottleTime)
	idx = putArrayLen(bytes, idx, o.TopicRespList)
	for _, topic := range o.TopicRespList {
		idx = putTopicString(bytes, idx, topic.Topic)
		idx = putArrayLen(bytes, idx, topic.PartitionRespList)
		for _, partition := range topic.PartitionRespList {
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			idx = putPartitionId(bytes, idx, partition.PartitionId)
			idx = putLeaderEpoch(bytes, idx, partition.LeaderEpoch)
			idx = putOffset(bytes, idx, partition.Offset)
		}
	}
	return bytes
}
