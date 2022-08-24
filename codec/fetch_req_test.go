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

func TestDecodeIllegalFetchReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeFetchReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeFetchReqV10(t *testing.T) {
	bytes := testHex2Bytes(t, "00000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff0000232600000001000f427f0000000000ffffffff000000010005746f7069630000000100000000ffffffff00000000000000000000000000000000000f427f00000000")
	fetchReq, err := DecodeFetchReq(bytes, 10)
	assert.Nil(t, err)
	assert.Equal(t, 6, fetchReq.CorrelationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", fetchReq.ClientId)
	assert.Equal(t, int32(-1), fetchReq.ReplicaId)
	assert.Equal(t, 8998, fetchReq.MaxWaitTime)
	assert.Equal(t, 1, fetchReq.MinBytes)
	assert.Equal(t, 1000063, fetchReq.MaxBytes)
	assert.Equal(t, uint8(0), fetchReq.IsolationLevel)
	assert.Equal(t, 0, fetchReq.FetchSessionId)
	assert.Equal(t, int32(-1), fetchReq.FetchSessionEpoch)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "topic", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 1)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 0, fetchPartitionReq.PartitionId)
	assert.Equal(t, int32(-1), fetchPartitionReq.CurrentLeaderEpoch)
	assert.Equal(t, int64(0), fetchPartitionReq.FetchOffset)
	assert.Equal(t, int64(0), fetchPartitionReq.LogStartOffset)
	assert.Equal(t, 1000063, fetchPartitionReq.PartitionMaxBytes)
}

func TestCodeFetchReqV10(t *testing.T) {
	fetchReq := &FetchReq{}
	fetchReq.ApiVersion = 10
	fetchReq.CorrelationId = 6
	fetchReq.ClientId = "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)"
	fetchReq.ReplicaId = -1
	fetchReq.MaxWaitTime = 8998
	fetchReq.MinBytes = 1
	fetchReq.MaxBytes = 1000063
	fetchReq.IsolationLevel = 0
	fetchReq.FetchSessionId = 0
	fetchReq.FetchSessionEpoch = -1
	partitionReq := &FetchPartitionReq{0, -1, 0, 0, 0, 1000063}
	topicReq := &FetchTopicReq{"topic", []*FetchPartitionReq{partitionReq}}
	fetchReq.TopicReqList = []*FetchTopicReq{topicReq}
	codeBytes := fetchReq.Bytes(true)
	assert.Equal(t, testHex2Bytes(t, "0001000a00000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff0000232600000001000f427f0000000000ffffffff000000010005746f7069630000000100000000ffffffff00000000000000000000000000000000000f427f"), codeBytes)
}

func TestDecodeAndCodeFetchReqV10(t *testing.T) {
	bytes := testHex2Bytes(t, "00000006006d5f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29ffffffff0000232600000001000f427f0000000000ffffffff000000010005746f7069630000000100000000ffffffff00000000000000000000000000000000000f427f")
	fetchReq, err := DecodeFetchReq(bytes, 10)
	assert.Nil(t, err)
	assert.Equal(t, 6, fetchReq.CorrelationId)
	assert.Equal(t, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", fetchReq.ClientId)
	assert.Equal(t, int32(-1), fetchReq.ReplicaId)
	assert.Equal(t, 8998, fetchReq.MaxWaitTime)
	assert.Equal(t, 1, fetchReq.MinBytes)
	assert.Equal(t, 1000063, fetchReq.MaxBytes)
	assert.Equal(t, uint8(0), fetchReq.IsolationLevel)
	assert.Equal(t, 0, fetchReq.FetchSessionId)
	assert.Equal(t, int32(-1), fetchReq.FetchSessionEpoch)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "topic", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 1)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 0, fetchPartitionReq.PartitionId)
	assert.Equal(t, int32(-1), fetchPartitionReq.CurrentLeaderEpoch)
	assert.Equal(t, int64(0), fetchPartitionReq.FetchOffset)
	assert.Equal(t, int64(0), fetchPartitionReq.LogStartOffset)
	assert.Equal(t, 1000063, fetchPartitionReq.PartitionMaxBytes)
	codeBytes := fetchReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeFetchReqV11(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000a002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31ffffffff000001f40000000103200000000000000000000000000000010006746573742d350000000100000000000000000000000000000000ffffffffffffffff00100000000000000000")
	fetchReq, err := DecodeFetchReq(bytes, 11)
	assert.Nil(t, err)
	assert.Equal(t, 10, fetchReq.CorrelationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1", fetchReq.ClientId)
	assert.Equal(t, 500, fetchReq.MaxWaitTime)
	assert.Equal(t, 1, fetchReq.MinBytes)
	assert.Equal(t, 52428800, fetchReq.MaxBytes)
	assert.Equal(t, uint8(0), fetchReq.IsolationLevel)
	assert.Equal(t, 0, fetchReq.FetchSessionId)
	assert.Equal(t, int32(0), fetchReq.FetchSessionEpoch)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "test-5", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 1)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 0, fetchPartitionReq.PartitionId)
	assert.Equal(t, int32(0), fetchPartitionReq.CurrentLeaderEpoch)
	assert.Equal(t, int64(0), fetchPartitionReq.FetchOffset)
	assert.Equal(t, 0, fetchPartitionReq.LastFetchedEpoch)
	assert.Equal(t, int64(-1), fetchPartitionReq.LogStartOffset)
	assert.Equal(t, 1048576, fetchPartitionReq.PartitionMaxBytes)
}

func TestCodeFetchReqV11(t *testing.T) {
	fetchReq := &FetchReq{}
	fetchReq.ApiVersion = 11
	fetchReq.CorrelationId = 10
	fetchReq.ClientId = "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1"
	fetchReq.ReplicaId = -1
	fetchReq.MaxWaitTime = 500
	fetchReq.MinBytes = 1
	fetchReq.MaxBytes = 52428800
	fetchReq.IsolationLevel = 0
	fetchReq.FetchSessionId = 0
	fetchReq.FetchSessionEpoch = 0
	partitionReq := &FetchPartitionReq{0, 0, 0, 0, -1, 1048576}
	topicReq := &FetchTopicReq{"test-5", []*FetchPartitionReq{partitionReq}}
	fetchReq.TopicReqList = []*FetchTopicReq{topicReq}
	codeBytes := fetchReq.Bytes(true)
	assert.Equal(t, testHex2Bytes(t, "0001000b0000000a002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31ffffffff000001f40000000103200000000000000000000000000000010006746573742d350000000100000000000000000000000000000000ffffffffffffffff00100000"), codeBytes)
}

func TestDecodeAndCodeFetchReqV11(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000a002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31ffffffff000001f40000000103200000000000000000000000000000010006746573742d350000000100000000000000000000000000000000ffffffffffffffff00100000")
	fetchReq, err := DecodeFetchReq(bytes, 11)
	assert.Nil(t, err)
	assert.Equal(t, 10, fetchReq.CorrelationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1", fetchReq.ClientId)
	assert.Equal(t, 500, fetchReq.MaxWaitTime)
	assert.Equal(t, 1, fetchReq.MinBytes)
	assert.Equal(t, 52428800, fetchReq.MaxBytes)
	assert.Equal(t, uint8(0), fetchReq.IsolationLevel)
	assert.Equal(t, 0, fetchReq.FetchSessionId)
	assert.Equal(t, int32(0), fetchReq.FetchSessionEpoch)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "test-5", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 1)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 0, fetchPartitionReq.PartitionId)
	assert.Equal(t, int32(0), fetchPartitionReq.CurrentLeaderEpoch)
	assert.Equal(t, int64(0), fetchPartitionReq.FetchOffset)
	assert.Equal(t, 0, fetchPartitionReq.LastFetchedEpoch)
	assert.Equal(t, int64(-1), fetchPartitionReq.LogStartOffset)
	assert.Equal(t, 1048576, fetchPartitionReq.PartitionMaxBytes)
	codeBytes := fetchReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeFetchReqMultiPartitionV11(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000a002f636f6e73756d65722d32393561656562302d633564342d343161632d386339302d3237656538363632383066322d31ffffffff000001f40000000103200000000000000000000000000000010004746573740000000200000001000000000000000000000960ffffffffffffffff00100000000000000000000000000000000009c4ffffffffffffffff00100000000000000000")
	fetchReq, err := DecodeFetchReq(bytes, 11)
	assert.Nil(t, err)
	assert.Equal(t, 10, fetchReq.CorrelationId)
	assert.Equal(t, "consumer-295aeeb0-c5d4-41ac-8c90-27ee866280f2-1", fetchReq.ClientId)
	assert.Equal(t, 500, fetchReq.MaxWaitTime)
	assert.Equal(t, 1, fetchReq.MinBytes)
	assert.Equal(t, 52428800, fetchReq.MaxBytes)
	assert.Equal(t, uint8(0), fetchReq.IsolationLevel)
	assert.Equal(t, 0, fetchReq.FetchSessionId)
	assert.Equal(t, int32(0), fetchReq.FetchSessionEpoch)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "test", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 2)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 1, fetchPartitionReq.PartitionId)
	assert.Equal(t, int32(0), fetchPartitionReq.CurrentLeaderEpoch)
	assert.Equal(t, int64(2400), fetchPartitionReq.FetchOffset)
	assert.Equal(t, 0, fetchPartitionReq.LastFetchedEpoch)
	assert.Equal(t, int64(-1), fetchPartitionReq.LogStartOffset)
	assert.Equal(t, 1048576, fetchPartitionReq.PartitionMaxBytes)

	fetchPartitionReq2 := fetchTopicReq.PartitionReqList[1]
	assert.Equal(t, 0, fetchPartitionReq2.PartitionId)
	assert.Equal(t, int32(0), fetchPartitionReq2.CurrentLeaderEpoch)
	assert.Equal(t, int64(2500), fetchPartitionReq2.FetchOffset)
	assert.Equal(t, 0, fetchPartitionReq2.LastFetchedEpoch)
	assert.Equal(t, int64(-1), fetchPartitionReq2.LogStartOffset)
	assert.Equal(t, 1048576, fetchPartitionReq2.PartitionMaxBytes)
}

func TestCodeFetchReqMultiPartitionV11(t *testing.T) {
	fetchReq := &FetchReq{}
	fetchReq.ApiVersion = 11
	fetchReq.CorrelationId = 10
	fetchReq.ClientId = "consumer-295aeeb0-c5d4-41ac-8c90-27ee866280f2-1"
	fetchReq.ReplicaId = -1
	fetchReq.MaxWaitTime = 500
	fetchReq.MinBytes = 1
	fetchReq.MaxBytes = 52428800
	fetchReq.IsolationLevel = 0
	fetchReq.FetchSessionId = 0
	fetchReq.FetchSessionEpoch = 0
	partitionReq1 := &FetchPartitionReq{1, 0, 2400, 0, -1, 1048576}
	partitionReq2 := &FetchPartitionReq{0, 0, 2500, 0, -1, 1048576}
	topicReq := &FetchTopicReq{"test", []*FetchPartitionReq{partitionReq1, partitionReq2}}
	fetchReq.TopicReqList = []*FetchTopicReq{topicReq}
	codeBytes := fetchReq.Bytes(true)
	assert.Equal(t, testHex2Bytes(t, "0001000b0000000a002f636f6e73756d65722d32393561656562302d633564342d343161632d386339302d3237656538363632383066322d31ffffffff000001f40000000103200000000000000000000000000000010004746573740000000200000001000000000000000000000960ffffffffffffffff00100000000000000000000000000000000009c4ffffffffffffffff00100000"), codeBytes)
}

func TestDecodeAndCodeFetchReqMultiPartitionV11(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000a002f636f6e73756d65722d32393561656562302d633564342d343161632d386339302d3237656538363632383066322d31ffffffff000001f40000000103200000000000000000000000000000010004746573740000000200000001000000000000000000000960ffffffffffffffff00100000000000000000000000000000000009c4ffffffffffffffff00100000")
	fetchReq, err := DecodeFetchReq(bytes, 11)
	assert.Nil(t, err)
	assert.Equal(t, 10, fetchReq.CorrelationId)
	assert.Equal(t, "consumer-295aeeb0-c5d4-41ac-8c90-27ee866280f2-1", fetchReq.ClientId)
	assert.Equal(t, 500, fetchReq.MaxWaitTime)
	assert.Equal(t, 1, fetchReq.MinBytes)
	assert.Equal(t, 52428800, fetchReq.MaxBytes)
	assert.Equal(t, uint8(0), fetchReq.IsolationLevel)
	assert.Equal(t, 0, fetchReq.FetchSessionId)
	assert.Equal(t, int32(0), fetchReq.FetchSessionEpoch)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "test", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 2)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 1, fetchPartitionReq.PartitionId)
	assert.Equal(t, int32(0), fetchPartitionReq.CurrentLeaderEpoch)
	assert.Equal(t, int64(2400), fetchPartitionReq.FetchOffset)
	assert.Equal(t, 0, fetchPartitionReq.LastFetchedEpoch)
	assert.Equal(t, int64(-1), fetchPartitionReq.LogStartOffset)
	assert.Equal(t, 1048576, fetchPartitionReq.PartitionMaxBytes)

	fetchPartitionReq2 := fetchTopicReq.PartitionReqList[1]
	assert.Equal(t, 0, fetchPartitionReq2.PartitionId)
	assert.Equal(t, int32(0), fetchPartitionReq2.CurrentLeaderEpoch)
	assert.Equal(t, int64(2500), fetchPartitionReq2.FetchOffset)
	assert.Equal(t, 0, fetchPartitionReq2.LastFetchedEpoch)
	assert.Equal(t, int64(-1), fetchPartitionReq2.LogStartOffset)
	assert.Equal(t, 1048576, fetchPartitionReq2.PartitionMaxBytes)
	codeBytes := fetchReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}
