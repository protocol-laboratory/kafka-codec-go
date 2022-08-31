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

type ProduceResp struct {
	BaseResp
	TopicRespList []*ProduceTopicResp
	ThrottleTime  int
}

type ProduceTopicResp struct {
	Topic             string
	PartitionRespList []*ProducePartitionResp
}

type ProducePartitionResp struct {
	PartitionId     int
	ErrorCode       ErrorCode
	Offset          int64
	Time            int64
	LogStartOffset  int64
	RecordErrorList []*RecordError
	ErrorMessage    *string
}

type RecordError struct {
	BatchIndex             int32
	BatchIndexErrorMessage *string
}

func DecodeProduceResp(bytes []byte, version int16) (resp *ProduceResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &ProduceResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	var topicLength int
	topicLength, idx = readArrayLen(bytes, idx)
	if topicLength > len(bytes) {
		return nil, InvalidProtocolContent
	}
	resp.TopicRespList = make([]*ProduceTopicResp, topicLength)
	for i := 0; i < topicLength; i++ {
		produceTopicResp := &ProduceTopicResp{}
		produceTopicResp.Topic, idx = readTopicString(bytes, idx)
		var partitionLength int
		partitionLength, idx = readArrayLen(bytes, idx)
		if partitionLength > len(bytes) {
			return nil, InvalidProtocolContent
		}
		produceTopicResp.PartitionRespList = make([]*ProducePartitionResp, partitionLength)
		for j := 0; j < partitionLength; j++ {
			producePartitionResp := &ProducePartitionResp{}
			producePartitionResp.PartitionId, idx = readPartitionId(bytes, idx)
			producePartitionResp.ErrorCode, idx = readErrorCode(bytes, idx)
			producePartitionResp.Offset, idx = readOffset(bytes, idx)
			producePartitionResp.Time, idx = readTime(bytes, idx)
			producePartitionResp.LogStartOffset, idx = readLogStartOffset(bytes, idx)
			if version == 8 {
				var recordErrorLen int
				recordErrorLen, idx = readArrayLen(bytes, idx)
				if recordErrorLen > len(bytes) {
					return nil, InvalidProtocolContent
				}
				producePartitionResp.RecordErrorList = make([]*RecordError, recordErrorLen)
				for k := 0; k < recordErrorLen; k++ {
					recordError := &RecordError{}
					recordError.BatchIndex, idx = readBatchIndex(bytes, idx)
					recordError.BatchIndexErrorMessage, idx = readNullableString(bytes, idx)
					producePartitionResp.RecordErrorList[k] = recordError
				}
				producePartitionResp.ErrorMessage, idx = readNullableString(bytes, idx)
			}
			produceTopicResp.PartitionRespList[j] = producePartitionResp
		}
		resp.TopicRespList[i] = produceTopicResp
	}
	if version == 7 || version == 8 {
		resp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	}
	return resp, nil
}

func (p *ProduceResp) BytesLength(version int16) int {
	result := LenCorrId
	result += LenArray
	for _, val := range p.TopicRespList {
		result += StrLen(val.Topic)
		result += LenArray
		for _, partitionResp := range val.PartitionRespList {
			result += LenPartitionId + LenErrorCode + LenOffset
			result += LenTime + LenOffset
			if version == 8 {
				result += LenArray
				for _, recordError := range partitionResp.RecordErrorList {
					result += LenBatchIndex
					result += NullableStrLen(recordError.BatchIndexErrorMessage)
				}
				result += NullableStrLen(partitionResp.ErrorMessage)
			}
		}
	}
	if version == 7 || version == 8 {
		result += LenThrottleTime
	}
	return result
}

func (p *ProduceResp) Bytes(version int16) []byte {
	bytes := make([]byte, p.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, p.CorrelationId)
	idx = putArrayLen(bytes, idx, p.TopicRespList)
	for _, topic := range p.TopicRespList {
		idx = putTopicString(bytes, idx, topic.Topic)
		idx = putArrayLen(bytes, idx, topic.PartitionRespList)
		for _, partition := range topic.PartitionRespList {
			idx = putPartitionId(bytes, idx, partition.PartitionId)
			idx = putErrorCode(bytes, idx, partition.ErrorCode)
			idx = putOffset(bytes, idx, partition.Offset)
			idx = putTime(bytes, idx, partition.Time)
			idx = putLogStartOffset(bytes, idx, partition.LogStartOffset)
			if version == 8 {
				idx = putArrayLen(bytes, idx, partition.RecordErrorList)
				for _, recordError := range partition.RecordErrorList {
					idx = putBatchIndex(bytes, idx, recordError.BatchIndex)
					idx = putNullableString(bytes, idx, recordError.BatchIndexErrorMessage)
				}
				idx = putNullableString(bytes, idx, partition.ErrorMessage)
			}
		}
	}
	idx = putThrottleTime(bytes, idx, p.ThrottleTime)
	return bytes
}
