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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeProduceReqV7(t *testing.T) {
	bytes := testHex2Bytes(t, "00000002006d5f5f5f546573744b61666b6150726f647563655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff00000f9a000000010005746f70696300000001000000000000004700000000000000000000003bffffffff022c30096c0000000000000000017df19951180000017df1995118ffffffffffffffffffffffffffff000000011200000001066d736700")
	produceReq, err := DecodeProduceReq(bytes, 7)
	assert.Nil(t, err)
	assert.Equal(t, 2, produceReq.CorrelationId)
	assert.Equal(t, "___TestKafkaProduce_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", produceReq.ClientId)
	assert.Equal(t, 3994, produceReq.Timeout)
	assert.Len(t, produceReq.TopicReqList, 1)
	topicReq := produceReq.TopicReqList[0]
	assert.Equal(t, "topic", topicReq.Topic)
	assert.Len(t, topicReq.PartitionReqList, 1)
	partitionReq := topicReq.PartitionReqList[0]
	assert.Equal(t, 0, partitionReq.PartitionId)
	recordBatch := partitionReq.RecordBatch
	var expectedOffset int64 = 0
	assert.Equal(t, expectedOffset, recordBatch.Offset)
	assert.Equal(t, 59, recordBatch.MessageSize)
	var expectedLeaderEpoch int32 = -1
	assert.Equal(t, expectedLeaderEpoch, recordBatch.LeaderEpoch)
	var expectedMagicByte byte = 2
	assert.Equal(t, expectedMagicByte, recordBatch.MagicByte)
	var expectedFlags uint16 = 0
	assert.Equal(t, expectedFlags, recordBatch.Flags)
	assert.Equal(t, 0, recordBatch.LastOffsetDelta)
	var expectedProducerId int64 = -1
	assert.Equal(t, expectedProducerId, recordBatch.ProducerId)
	var expectedBaseSequence int32 = -1
	assert.Equal(t, expectedBaseSequence, recordBatch.BaseSequence)
	assert.Len(t, recordBatch.Records, 1)
	record := recordBatch.Records[0]
	var expectedRecordAttributes byte = 0
	assert.Equal(t, expectedRecordAttributes, record.RecordAttributes)
	var expectedRelativeTimestamp int64 = 0
	assert.Equal(t, expectedRelativeTimestamp, record.RelativeTimestamp)
	assert.Equal(t, 0, record.RelativeOffset)
	assert.Nil(t, record.Key)
	assert.Equal(t, "msg", string(record.Value))
}

func TestDecodeProduceReqV8(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004002464646162333263392d663632302d343061322d616662382d313862373636393662653064ffff000100007530000000010005746f70696300000001000000000000004c000000000000000000000040ffffffff02635624670000000000000000017e685832d60000017e685832d6ffffffffffffffffffffffffffff000000011c000000066b65790a76616c756500")
	produceReq, err := DecodeProduceReq(bytes, 8)
	assert.Nil(t, err)
	assert.Equal(t, 4, produceReq.CorrelationId)
	assert.Equal(t, "ddab32c9-f620-40a2-afb8-18b76696be0d", produceReq.ClientId)
	assert.Equal(t, 30_000, produceReq.Timeout)
	assert.Len(t, produceReq.TopicReqList, 1)
	topicReq := produceReq.TopicReqList[0]
	assert.Equal(t, "topic", topicReq.Topic)
	assert.Len(t, topicReq.PartitionReqList, 1)
	partitionReq := topicReq.PartitionReqList[0]
	assert.Equal(t, 0, partitionReq.PartitionId)
	recordBatch := partitionReq.RecordBatch
	var expectedOffset int64 = 0
	assert.Equal(t, expectedOffset, recordBatch.Offset)
	assert.Equal(t, 64, recordBatch.MessageSize)
	var expectedLeaderEpoch int32 = -1
	assert.Equal(t, expectedLeaderEpoch, recordBatch.LeaderEpoch)
	var expectedMagicByte byte = 2
	assert.Equal(t, expectedMagicByte, recordBatch.MagicByte)
	var expectedFlags uint16 = 0
	assert.Equal(t, expectedFlags, recordBatch.Flags)
	assert.Equal(t, 0, recordBatch.LastOffsetDelta)
	var expectedProducerId int64 = -1
	assert.Equal(t, expectedProducerId, recordBatch.ProducerId)
	var expectedBaseSequence int32 = -1
	assert.Equal(t, expectedBaseSequence, recordBatch.BaseSequence)
	assert.Len(t, recordBatch.Records, 1)
	record := recordBatch.Records[0]
	var expectedRecordAttributes byte = 0
	assert.Equal(t, expectedRecordAttributes, record.RecordAttributes)
	var expectedRelativeTimestamp int64 = 0
	assert.Equal(t, expectedRelativeTimestamp, record.RelativeTimestamp)
	assert.Equal(t, 0, record.RelativeOffset)
	assert.Equal(t, []byte("key"), record.Key)
	assert.Equal(t, "value", string(record.Value))
}
