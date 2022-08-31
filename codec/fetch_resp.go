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

type FetchResp struct {
	BaseResp
	ThrottleTime  int
	ErrorCode     ErrorCode
	SessionId     int
	TopicRespList []*FetchTopicResp
}

type FetchTopicResp struct {
	Topic             string
	PartitionRespList []*FetchPartitionResp
}

type FetchPartitionResp struct {
	PartitionIndex      int
	ErrorCode           ErrorCode
	HighWatermark       int64
	LastStableOffset    int64
	LogStartOffset      int64
	AbortedTransactions int64
	ReplicaId           int32
	RecordBatch         *RecordBatch
}

func DecodeFetchResp(bytes []byte, version int16) (fetchResp *FetchResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			fetchResp = nil
		}
	}()
	fetchResp = &FetchResp{}
	idx := 0
	fetchResp.CorrelationId, idx = readCorrId(bytes, idx)
	fetchResp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	fetchResp.ErrorCode, idx = readErrorCode(bytes, idx)
	fetchResp.SessionId, idx = readSessionId(bytes, idx)
	topicLen, idx := readArrayLen(bytes, idx)
	if topicLen > len(bytes) {
		return nil, InvalidProtocolContent
	}
	fetchResp.TopicRespList = make([]*FetchTopicResp, topicLen)
	for i := 0; i < topicLen; i++ {
		topicResp := &FetchTopicResp{}
		topicResp.Topic, idx = readTopicString(bytes, idx)
		var partitionLen int
		partitionLen, idx = readArrayLen(bytes, idx)
		if partitionLen > len(bytes) {
			return nil, InvalidProtocolContent
		}
		topicResp.PartitionRespList = make([]*FetchPartitionResp, partitionLen)
		for j := 0; j < partitionLen; j++ {
			partitionResp := &FetchPartitionResp{}
			partitionResp.PartitionIndex, idx = readPartitionId(bytes, idx)
			partitionResp.ErrorCode, idx = readErrorCode(bytes, idx)
			partitionResp.HighWatermark, idx = readOffset(bytes, idx)
			partitionResp.LastStableOffset, idx = readOffset(bytes, idx)
			partitionResp.LogStartOffset, idx = readOffset(bytes, idx)
			// todo skip transaction
			idx += 4
			if version == 11 {
				partitionResp.ReplicaId, idx = readReplicaId(bytes, idx)
			}
			var recordBatchLength int
			recordBatchLength, idx = readInt(bytes, idx)
			partitionResp.RecordBatch = DecodeRecordBatch(bytes[idx:idx+recordBatchLength-1], version)
			idx += recordBatchLength
			topicResp.PartitionRespList[j] = partitionResp
		}
		fetchResp.TopicRespList[i] = topicResp
	}
	return fetchResp, nil
}

func NewFetchResp(corrId int) *FetchResp {
	resp := FetchResp{}
	resp.CorrelationId = corrId
	return &resp
}

func (f *FetchResp) BytesLength(version int16) int {
	result := LenCorrId + LenThrottleTime + LenErrorCode + LenFetchSessionId + LenArray
	for _, t := range f.TopicRespList {
		result += StrLen(t.Topic) + LenArray
		for _, p := range t.PartitionRespList {
			result += LenPartitionId + LenErrorCode
			result += LenOffset
			result += LenLastStableOffset + LenStartOffset
			result += LenAbortTransactions
			if version == 11 {
				result += LenReplicaId
			}
			result += LenMessageSize + p.RecordBatch.BytesLength()
		}
	}
	return result
}

func (f *FetchResp) Bytes(version int16) []byte {
	bytes := make([]byte, f.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, f.CorrelationId)
	idx = putThrottleTime(bytes, idx, f.ThrottleTime)
	idx = putErrorCode(bytes, idx, f.ErrorCode)
	idx = putSessionId(bytes, idx, f.SessionId)
	idx = putArrayLen(bytes, idx, len(f.TopicRespList))
	for _, t := range f.TopicRespList {
		idx = putString(bytes, idx, t.Topic)
		idx = putArrayLen(bytes, idx, len(t.PartitionRespList))
		for _, p := range t.PartitionRespList {
			idx = putInt(bytes, idx, p.PartitionIndex)
			idx = putErrorCode(bytes, idx, p.ErrorCode)
			idx = putInt64(bytes, idx, p.HighWatermark)
			idx = putInt64(bytes, idx, p.LastStableOffset)
			idx = putInt64(bytes, idx, p.LogStartOffset)
			idx = putInt(bytes, idx, -1)
			if version == 11 {
				idx = putInt(bytes, idx, -1)
			}
			if p.RecordBatch != nil {
				idx = putRecordBatch(bytes, idx, p.RecordBatch.Bytes())
			}
		}
	}
	return bytes
}
