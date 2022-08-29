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

func TestDecodeIllegalListOffsetReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeListOffsetsReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeListOffsetsReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff000000010005746f7069630000000100000000ffffffffffffffff")
	listOffsetReq, err := DecodeListOffsetsReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 4, listOffsetReq.CorrelationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", listOffsetReq.ClientId)
	assert.Equal(t, int32(-1), listOffsetReq.ReplicaId)
	assert.Equal(t, uint8(0), listOffsetReq.IsolationLevel)
	assert.Len(t, listOffsetReq.TopicReqList, 1)
	offsetTopic := listOffsetReq.TopicReqList[0]
	assert.Equal(t, "topic", offsetTopic.Topic)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 0, offsetPartition.PartitionId)
	assert.Equal(t, int32(0), offsetPartition.LeaderEpoch)
	assert.Equal(t, int64(-1), offsetPartition.Time)
}

func TestEncodeListOffsetsReqV1(t *testing.T) {
	listOffsetsReq := &ListOffsetsReq{}
	listOffsetsReq.ApiVersion = 1
	listOffsetsReq.CorrelationId = 4
	listOffsetsReq.ClientId = "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)"
	listOffsetsReq.ReplicaId = -1
	listOffsetsReq.IsolationLevel = 0
	listOffsetsReq.TopicReqList = make([]*ListOffsetsTopic, 1)
	partitions := make([]*ListOffsetsPartition, 1)
	partition := &ListOffsetsPartition{0, 0, -1}
	partitions[0] = partition
	topic := &ListOffsetsTopic{"topic", partitions}
	listOffsetsReq.TopicReqList[0] = topic
	codeBytes := listOffsetsReq.Bytes(true)
	assert.Equal(t, testHex2Bytes(t, "0002000100000004006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff000000010005746f7069630000000100000000ffffffffffffffff"), codeBytes)
}

func TestDecodeAndCodeListOffsetsReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff000000010005746f7069630000000100000000ffffffffffffffff")
	listOffsetReq, err := DecodeListOffsetsReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 4, listOffsetReq.CorrelationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", listOffsetReq.ClientId)
	assert.Equal(t, int32(-1), listOffsetReq.ReplicaId)
	assert.Equal(t, uint8(0), listOffsetReq.IsolationLevel)
	assert.Len(t, listOffsetReq.TopicReqList, 1)
	offsetTopic := listOffsetReq.TopicReqList[0]
	assert.Equal(t, "topic", offsetTopic.Topic)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 0, offsetPartition.PartitionId)
	assert.Equal(t, int32(0), offsetPartition.LeaderEpoch)
	assert.Equal(t, int64(-1), offsetPartition.Time)
	codeBytes := listOffsetReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeListOffsetsReqV5(t *testing.T) {
	bytes := testHex2Bytes(t, "00000008002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31ffffffff00000000010006746573742d35000000010000000000000000fffffffffffffffe")
	listOffsetReq, err := DecodeListOffsetsReq(bytes, 5)
	assert.Nil(t, err)
	assert.Equal(t, 8, listOffsetReq.CorrelationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1", listOffsetReq.ClientId)
	assert.Equal(t, int32(-1), listOffsetReq.ReplicaId)
	assert.Equal(t, uint8(0), listOffsetReq.IsolationLevel)
	assert.Len(t, listOffsetReq.TopicReqList, 1)
	offsetTopic := listOffsetReq.TopicReqList[0]
	assert.Equal(t, "test-5", offsetTopic.Topic)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 0, offsetPartition.PartitionId)
	assert.Equal(t, int32(0), offsetPartition.LeaderEpoch)
	assert.Equal(t, int64(-2), offsetPartition.Time)
}

func TestEncodeListOffsetsReqV5(t *testing.T) {
	listOffsetsReq := &ListOffsetsReq{}
	listOffsetsReq.ApiVersion = 5
	listOffsetsReq.CorrelationId = 8
	listOffsetsReq.ClientId = "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1"
	listOffsetsReq.ReplicaId = -1
	listOffsetsReq.IsolationLevel = 0
	listOffsetsReq.TopicReqList = make([]*ListOffsetsTopic, 1)
	partitions := make([]*ListOffsetsPartition, 1)
	partition := &ListOffsetsPartition{0, 0, -2}
	partitions[0] = partition
	topic := &ListOffsetsTopic{"test-5", partitions}
	listOffsetsReq.TopicReqList[0] = topic
	codeBytes := listOffsetsReq.Bytes(true)
	assert.Equal(t, testHex2Bytes(t, "0002000500000008002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31ffffffff00000000010006746573742d35000000010000000000000000fffffffffffffffe"), codeBytes)
}

func TestDecodeAndCodeListOffsetsReqV5(t *testing.T) {
	bytes := testHex2Bytes(t, "00000008002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31ffffffff00000000010006746573742d35000000010000000000000000fffffffffffffffe")
	listOffsetReq, err := DecodeListOffsetsReq(bytes, 5)
	assert.Nil(t, err)
	assert.Equal(t, 8, listOffsetReq.CorrelationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1", listOffsetReq.ClientId)
	assert.Equal(t, int32(-1), listOffsetReq.ReplicaId)
	assert.Equal(t, uint8(0), listOffsetReq.IsolationLevel)
	assert.Len(t, listOffsetReq.TopicReqList, 1)
	offsetTopic := listOffsetReq.TopicReqList[0]
	assert.Equal(t, "test-5", offsetTopic.Topic)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 0, offsetPartition.PartitionId)
	assert.Equal(t, int32(0), offsetPartition.LeaderEpoch)
	assert.Equal(t, int64(-2), offsetPartition.Time)
	codeBytes := listOffsetReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeListOffsetsReqV6(t *testing.T) {
	bytes := testHex2Bytes(t, "000000070027636f6e73756d65722d68706354657374546f7069633b7465737447726f75702d31313232332d3100ffffffff00020d68706354657374546f706963060000000400000000ffffffffffffffff000000000200000000ffffffffffffffff000000000300000000ffffffffffffffff000000000000000000ffffffffffffffff000000000100000000ffffffffffffffff000000")
	listOffsetReq, err := DecodeListOffsetsReq(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, 7, listOffsetReq.CorrelationId)
	assert.Equal(t, "consumer-hpcTestTopic;testGroup-11223-1", listOffsetReq.ClientId)
	assert.Equal(t, int32(16777215), listOffsetReq.ReplicaId)
	assert.Equal(t, uint8(255), listOffsetReq.IsolationLevel)
	assert.Len(t, listOffsetReq.TopicReqList, 1)
	offsetTopic := listOffsetReq.TopicReqList[0]
	assert.Equal(t, "hpcTestTopic", offsetTopic.Topic)
	assert.Len(t, offsetTopic.PartitionReqList, 5)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 4, offsetPartition.PartitionId)
	assert.Equal(t, int32(0), offsetPartition.LeaderEpoch)
	assert.Equal(t, int64(-1), offsetPartition.Time)
}

func TestEncodeListOffsetsReqV6(t *testing.T) {
	listOffsetsReq := &ListOffsetsReq{}
	listOffsetsReq.ApiVersion = 6
	listOffsetsReq.CorrelationId = 7
	listOffsetsReq.ClientId = "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1"
	listOffsetsReq.ReplicaId = -1
	listOffsetsReq.IsolationLevel = 0
	listOffsetsReq.TopicReqList = make([]*ListOffsetsTopic, 1)
	partitions := make([]*ListOffsetsPartition, 1)
	partition := &ListOffsetsPartition{0, 0, -2}
	partitions[0] = partition
	topic := &ListOffsetsTopic{"test-5", partitions}
	listOffsetsReq.TopicReqList[0] = topic
	codeBytes := listOffsetsReq.Bytes(true)
	assert.Equal(t, testHex2Bytes(t, "0002000600000007002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31ffffffff00000207746573742d35020000000000000000fffffffffffffffe000000"), codeBytes)
}

func TestDecodeAndCodeListOffsetsReqV6(t *testing.T) {
	bytes := testHex2Bytes(t, "000000070027636f6e73756d65722d68706354657374546f7069633b7465737447726f75702d31313232332d3100ffffffff00020d68706354657374546f706963060000000400000000ffffffffffffffff000000000200000000ffffffffffffffff000000000300000000ffffffffffffffff000000000000000000ffffffffffffffff000000000100000000ffffffffffffffff000000")
	listOffsetReq, err := DecodeListOffsetsReq(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, 7, listOffsetReq.CorrelationId)
	assert.Equal(t, "consumer-hpcTestTopic;testGroup-11223-1", listOffsetReq.ClientId)
	assert.Equal(t, int32(16777215), listOffsetReq.ReplicaId)
	assert.Equal(t, uint8(255), listOffsetReq.IsolationLevel)
	assert.Len(t, listOffsetReq.TopicReqList, 1)
	offsetTopic := listOffsetReq.TopicReqList[0]
	assert.Equal(t, "hpcTestTopic", offsetTopic.Topic)
	assert.Len(t, offsetTopic.PartitionReqList, 5)
	offsetPartition := offsetTopic.PartitionReqList[0]
	assert.Equal(t, 4, offsetPartition.PartitionId)
	assert.Equal(t, int32(0), offsetPartition.LeaderEpoch)
	assert.Equal(t, int64(-1), offsetPartition.Time)
	codeBytes := listOffsetReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}
