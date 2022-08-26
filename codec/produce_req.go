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

type ProduceReq struct {
	BaseReq
	ClientId      string
	TransactionId *string
	RequiredAcks  int16
	Timeout       int
	TopicReqList  []*ProduceTopicReq
}

type ProduceTopicReq struct {
	Topic            string
	PartitionReqList []*ProducePartitionReq
}

type ProducePartitionReq struct {
	PartitionId int
	RecordBatch *RecordBatch
}

func DecodeProduceReq(bytes []byte, version int16) (produceReq *ProduceReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			produceReq = nil
		}
	}()
	produceReq = &ProduceReq{}
	produceReq.ApiVersion = version
	idx := 0
	produceReq.CorrelationId, idx = readCorrId(bytes, idx)
	produceReq.ClientId, idx = readClientId(bytes, idx)
	produceReq.TransactionId, idx = readTransactionId(bytes, idx)
	produceReq.RequiredAcks, idx = readRequiredAcks(bytes, idx)
	produceReq.Timeout, idx = readInt(bytes, idx)
	var length int
	length, idx = readInt(bytes, idx)
	produceReq.TopicReqList = make([]*ProduceTopicReq, length)
	for i := 0; i < length; i++ {
		topic := &ProduceTopicReq{}
		topic.Topic, idx = readTopicString(bytes, idx)
		var partitionLength int
		partitionLength, idx = readInt(bytes, idx)
		topic.PartitionReqList = make([]*ProducePartitionReq, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partition := &ProducePartitionReq{}
			partition.PartitionId, idx = readPartitionId(bytes, idx)
			var recordBatchLength int
			recordBatchLength, idx = readInt(bytes, idx)
			partition.RecordBatch = DecodeRecordBatch(bytes[idx:idx+recordBatchLength-1], version)
			idx += recordBatchLength
			topic.PartitionReqList[j] = partition
		}
		produceReq.TopicReqList[i] = topic
	}
	return produceReq, nil
}

func (p *ProduceReq) BytesLength(containApiKeyVersion bool) int {
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(p.ClientId)
	length += LenTransactionalId
	length += LenRequiredAcks
	length += LenTimeout
	length += LenArray
	for _, topicReq := range p.TopicReqList {
		length += StrLen(topicReq.Topic)
		length += LenArray
		for _, partitionReq := range topicReq.PartitionReqList {
			length += LenPartitionId
			length += LenMessageSize + partitionReq.RecordBatch.BytesLength()
		}
	}
	return length
}

func (p *ProduceReq) Bytes(containApiKeyVersion bool) []byte {
	version := p.ApiVersion
	bytes := make([]byte, p.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, Produce)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, p.CorrelationId)
	idx = putClientId(bytes, idx, p.ClientId)
	idx = putTransactionId(bytes, idx, p.TransactionId)
	idx = putRequiredAcks(bytes, idx, p.RequiredAcks)
	idx = putInt(bytes, idx, p.Timeout)
	idx = putArrayLen(bytes, idx, len(p.TopicReqList))
	for _, topicReq := range p.TopicReqList {
		idx = putTopicString(bytes, idx, topicReq.Topic)
		idx = putArrayLen(bytes, idx, len(topicReq.PartitionReqList))
		for _, partitionReq := range topicReq.PartitionReqList {
			idx = putPartitionId(bytes, idx, partitionReq.PartitionId)
			if partitionReq.RecordBatch != nil {
				idx = putRecordBatch(bytes, idx, partitionReq.RecordBatch.Bytes())
			}
		}
	}
	return bytes
}
