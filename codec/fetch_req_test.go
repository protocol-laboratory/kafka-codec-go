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
	var expectedReplicaId int32 = -1
	assert.Equal(t, expectedReplicaId, fetchReq.ReplicaId)
	assert.Equal(t, 8998, fetchReq.MaxWaitTime)
	assert.Equal(t, 1, fetchReq.MinBytes)
	assert.Equal(t, 1000063, fetchReq.MaxBytes)
	var expectedIsolationLevel uint8 = 0
	assert.Equal(t, expectedIsolationLevel, fetchReq.IsolationLevel)
	assert.Equal(t, 0, fetchReq.FetchSessionId)
	var expectedFetchSessionEpoch int32 = -1
	assert.Equal(t, expectedFetchSessionEpoch, fetchReq.FetchSessionEpoch)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "topic", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 1)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 0, fetchPartitionReq.PartitionId)
	var expectedCurrentLeaderEpoch int32 = -1
	assert.Equal(t, expectedCurrentLeaderEpoch, fetchPartitionReq.CurrentLeaderEpoch)
	var expectedFetchOffset int64 = 0
	assert.Equal(t, expectedFetchOffset, fetchPartitionReq.FetchOffset)
	var expectedLogOffset int64 = 0
	assert.Equal(t, expectedLogOffset, fetchPartitionReq.LogStartOffset)
	assert.Equal(t, 1000063, fetchPartitionReq.PartitionMaxBytes)
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
	var expectedIsolationLevel uint8 = 0
	assert.Equal(t, expectedIsolationLevel, fetchReq.IsolationLevel)
	assert.Equal(t, 0, fetchReq.FetchSessionId)
	var expectedFetchSessionEpoch int32 = 0
	assert.Equal(t, expectedFetchSessionEpoch, fetchReq.FetchSessionEpoch)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "test-5", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 1)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 0, fetchPartitionReq.PartitionId)
	var expectedCurrentLeaderEpoch int32 = 0
	assert.Equal(t, expectedCurrentLeaderEpoch, fetchPartitionReq.CurrentLeaderEpoch)
	var expectedFetchOffset int64 = 0
	assert.Equal(t, expectedFetchOffset, fetchPartitionReq.FetchOffset)
	assert.Equal(t, 0, fetchPartitionReq.LastFetchedEpoch)
	var expectedLogOffset int64 = -1
	assert.Equal(t, expectedLogOffset, fetchPartitionReq.LogStartOffset)
	assert.Equal(t, 1048576, fetchPartitionReq.PartitionMaxBytes)
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
	var expectedIsolationLevel uint8 = 0
	assert.Equal(t, expectedIsolationLevel, fetchReq.IsolationLevel)
	assert.Equal(t, 0, fetchReq.FetchSessionId)
	var expectedFetchSessionEpoch int32 = 0
	assert.Equal(t, expectedFetchSessionEpoch, fetchReq.FetchSessionEpoch)
	assert.Len(t, fetchReq.TopicReqList, 1)
	fetchTopicReq := fetchReq.TopicReqList[0]
	assert.Equal(t, "test", fetchTopicReq.Topic)
	assert.Len(t, fetchTopicReq.PartitionReqList, 2)
	fetchPartitionReq := fetchTopicReq.PartitionReqList[0]
	assert.Equal(t, 1, fetchPartitionReq.PartitionId)
	var expectedCurrentLeaderEpoch int32 = 0
	assert.Equal(t, expectedCurrentLeaderEpoch, fetchPartitionReq.CurrentLeaderEpoch)
	var expectedFetchOffset1 int64 = 2400
	assert.Equal(t, expectedFetchOffset1, fetchPartitionReq.FetchOffset)
	assert.Equal(t, 0, fetchPartitionReq.LastFetchedEpoch)
	var expectedLogOffset int64 = -1
	assert.Equal(t, expectedLogOffset, fetchPartitionReq.LogStartOffset)
	assert.Equal(t, 1048576, fetchPartitionReq.PartitionMaxBytes)

	fetchPartitionReq2 := fetchTopicReq.PartitionReqList[1]
	assert.Equal(t, 0, fetchPartitionReq2.PartitionId)
	assert.Equal(t, expectedCurrentLeaderEpoch, fetchPartitionReq2.CurrentLeaderEpoch)
	var expectedFetchOffset2 int64 = 2500
	assert.Equal(t, expectedFetchOffset2, fetchPartitionReq2.FetchOffset)
	assert.Equal(t, 0, fetchPartitionReq2.LastFetchedEpoch)
	assert.Equal(t, expectedLogOffset, fetchPartitionReq2.LogStartOffset)
	assert.Equal(t, 1048576, fetchPartitionReq2.PartitionMaxBytes)
}
