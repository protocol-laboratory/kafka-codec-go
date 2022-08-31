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

func TestDecodeIllegalMetadataReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeMetadataReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeMetadataV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001006d5f5f5f546573744b61666b6150726f647563655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29000000010005746f706963")
	metadataTopicReq, err := DecodeMetadataReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, metadataTopicReq.CorrelationId)
	assert.Equal(t, "___TestKafkaProduce_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", metadataTopicReq.ClientId)
	assert.Len(t, metadataTopicReq.Topics, 1)
	topicReq := metadataTopicReq.Topics[0]
	assert.Equal(t, "topic", topicReq.Topic)
	assert.False(t, metadataTopicReq.AllowAutoTopicCreation)
	assert.False(t, metadataTopicReq.IncludeClusterAuthorizedOperations)
	assert.False(t, metadataTopicReq.IncludeTopicAuthorizedOperations)
}

func TestCodeMetadataV1(t *testing.T) {
	metadataReq := &MetadataReq{}
	metadataReq.ApiVersion = 1
	metadataReq.CorrelationId = 1
	metadataReq.ClientId = "___TestKafkaProduce_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)"
	metadataReq.Topics = []*MetadataTopicReq{
		{
			Topic: "topic",
		},
	}
	bytes := metadataReq.Bytes(true)
	expectBytes := testHex2Bytes(t, "0003000100000001006d5f5f5f546573744b61666b6150726f647563655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29000000010005746f706963")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndEncodeMetadataV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001006d5f5f5f546573744b61666b6150726f647563655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f29000000010005746f706963")
	metadataTopicReq, err := DecodeMetadataReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, metadataTopicReq.CorrelationId)
	assert.Equal(t, "___TestKafkaProduce_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)", metadataTopicReq.ClientId)
	assert.Len(t, metadataTopicReq.Topics, 1)
	topicReq := metadataTopicReq.Topics[0]
	assert.Equal(t, "topic", topicReq.Topic)
	assert.False(t, metadataTopicReq.AllowAutoTopicCreation)
	assert.False(t, metadataTopicReq.IncludeClusterAuthorizedOperations)
	assert.False(t, metadataTopicReq.IncludeTopicAuthorizedOperations)
	codeBytes := metadataTopicReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeMetadataReqV5(t *testing.T) {
	bytes := testHex2Bytes(t, "000000000006736172616d61ffffffff00")
	metadataTopicReq, err := DecodeMetadataReq(bytes, 5)
	assert.Nil(t, err)
	assert.Equal(t, metadataTopicReq.CorrelationId, 0)
	assert.Equal(t, "sarama", metadataTopicReq.ClientId)
	assert.Nil(t, metadataTopicReq.Topics)
	assert.False(t, metadataTopicReq.AllowAutoTopicCreation)
	assert.False(t, metadataTopicReq.IncludeTopicAuthorizedOperations)
	assert.False(t, metadataTopicReq.IncludeClusterAuthorizedOperations)
}

func TestEncodeMetadataReqV5(t *testing.T) {
	metadataReq := &MetadataReq{}
	metadataReq.ApiVersion = 5
	metadataReq.CorrelationId = 0
	metadataReq.ClientId = "sarama"
	metadataReq.Topics = nil
	bytes := metadataReq.Bytes(true)
	expectBytes := testHex2Bytes(t, "00030005000000000006736172616d61ffffffff00")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndEncodeMetadataReqV5(t *testing.T) {
	bytes := testHex2Bytes(t, "000000000006736172616d61ffffffff00")
	metadataTopicReq, err := DecodeMetadataReq(bytes, 5)
	assert.Nil(t, err)
	assert.Equal(t, metadataTopicReq.CorrelationId, 0)
	assert.Equal(t, "sarama", metadataTopicReq.ClientId)
	assert.Nil(t, metadataTopicReq.Topics)
	assert.False(t, metadataTopicReq.AllowAutoTopicCreation)
	assert.False(t, metadataTopicReq.IncludeTopicAuthorizedOperations)
	assert.False(t, metadataTopicReq.IncludeClusterAuthorizedOperations)
	codeBytes := metadataTopicReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeMetadataV8(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001000a70726f64756365722d3100000001000a746573742d746f706963010000")
	metadataTopicReq, err := DecodeMetadataReq(bytes, 8)
	assert.Nil(t, err)
	assert.Equal(t, 1, metadataTopicReq.CorrelationId)
	assert.Equal(t, "producer-1", metadataTopicReq.ClientId)
	assert.Len(t, metadataTopicReq.Topics, 1)
	topicReq := metadataTopicReq.Topics[0]
	assert.Equal(t, "test-topic", topicReq.Topic)
	assert.True(t, metadataTopicReq.AllowAutoTopicCreation)
	assert.False(t, metadataTopicReq.IncludeClusterAuthorizedOperations)
	assert.False(t, metadataTopicReq.IncludeTopicAuthorizedOperations)
}

func TestEncodeMetadataReqV8(t *testing.T) {
	metadataReq := &MetadataReq{}
	metadataReq.ApiVersion = 8
	metadataReq.CorrelationId = 1
	metadataReq.ClientId = "producer-1"
	metadataReq.Topics = []*MetadataTopicReq{
		{
			Topic: "test-topic",
		},
	}
	metadataReq.AllowAutoTopicCreation = true
	metadataReq.IncludeClusterAuthorizedOperations = false
	metadataReq.IncludeTopicAuthorizedOperations = false
	bytes := metadataReq.Bytes(true)
	expectBytes := testHex2Bytes(t, "0003000800000001000a70726f64756365722d3100000001000a746573742d746f706963010000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndEncodeMetadataReqV8(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001000a70726f64756365722d3100000001000a746573742d746f706963010000")
	metadataTopicReq, err := DecodeMetadataReq(bytes, 8)
	assert.Nil(t, err)
	assert.Equal(t, 1, metadataTopicReq.CorrelationId)
	assert.Equal(t, "producer-1", metadataTopicReq.ClientId)
	assert.Len(t, metadataTopicReq.Topics, 1)
	topicReq := metadataTopicReq.Topics[0]
	assert.Equal(t, "test-topic", topicReq.Topic)
	assert.True(t, metadataTopicReq.AllowAutoTopicCreation)
	assert.False(t, metadataTopicReq.IncludeClusterAuthorizedOperations)
	assert.False(t, metadataTopicReq.IncludeTopicAuthorizedOperations)
	codeBytes := metadataTopicReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeMetadataV9(t *testing.T) {
	bytes := testHex2Bytes(t, "00000002002f636f6e73756d65722d37336664633964612d306439322d346537622d613761372d6563323636663637633137312d3100022537363465646565332d303037652d343865302d623966392d6466376637313366663730370001000000")
	metadataTopicReq, err := DecodeMetadataReq(bytes, 9)
	assert.Nil(t, err)
	assert.Equal(t, 2, metadataTopicReq.CorrelationId)
	assert.Equal(t, "consumer-73fdc9da-0d92-4e7b-a7a7-ec266f67c171-1", metadataTopicReq.ClientId)
	assert.Len(t, metadataTopicReq.Topics, 1)
	topicReq := metadataTopicReq.Topics[0]
	assert.Equal(t, "764edee3-007e-48e0-b9f9-df7f713ff707", topicReq.Topic)
	assert.True(t, metadataTopicReq.AllowAutoTopicCreation)
	assert.False(t, metadataTopicReq.IncludeClusterAuthorizedOperations)
	assert.False(t, metadataTopicReq.IncludeTopicAuthorizedOperations)
}

func TestEncodeMetadataReqV9(t *testing.T) {
	metadataReq := &MetadataReq{}
	metadataReq.ApiVersion = 9
	metadataReq.CorrelationId = 2
	metadataReq.ClientId = "consumer-a1e12365-ddfa-43fc-826e-9661fb54c274-1"
	metadataReq.Topics = []*MetadataTopicReq{
		{
			Topic: "test-3",
		},
	}
	metadataReq.AllowAutoTopicCreation = true
	metadataReq.IncludeClusterAuthorizedOperations = false
	metadataReq.IncludeTopicAuthorizedOperations = false
	bytes := metadataReq.Bytes(true)
	expectBytes := testHex2Bytes(t, "0003000900000002002f636f6e73756d65722d61316531323336352d646466612d343366632d383236652d3936363166623534633237342d31000207746573742d330001000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndEncodeMetadataReqV9(t *testing.T) {
	bytes := testHex2Bytes(t, "00000002002f636f6e73756d65722d37336664633964612d306439322d346537622d613761372d6563323636663637633137312d3100022537363465646565332d303037652d343865302d623966392d6466376637313366663730370001000000")
	metadataTopicReq, err := DecodeMetadataReq(bytes, 9)
	assert.Nil(t, err)
	assert.Equal(t, 2, metadataTopicReq.CorrelationId)
	assert.Equal(t, "consumer-73fdc9da-0d92-4e7b-a7a7-ec266f67c171-1", metadataTopicReq.ClientId)
	assert.Len(t, metadataTopicReq.Topics, 1)
	topicReq := metadataTopicReq.Topics[0]
	assert.Equal(t, "764edee3-007e-48e0-b9f9-df7f713ff707", topicReq.Topic)
	assert.True(t, metadataTopicReq.AllowAutoTopicCreation)
	assert.False(t, metadataTopicReq.IncludeClusterAuthorizedOperations)
	assert.False(t, metadataTopicReq.IncludeTopicAuthorizedOperations)
	codeBytes := metadataTopicReq.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}
