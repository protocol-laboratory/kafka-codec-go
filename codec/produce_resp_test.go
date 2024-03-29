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

func TestDecodeProduceRespV7(t *testing.T) {
	produceRespBytes := testHex2Bytes(t, "00000002000000010005746f706963000000010000000000000000000000000000ffffffffffffffff000000000000000000000000")
	resp, err := DecodeProduceResp(produceRespBytes, 7)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 2)
	assert.Equal(t, len(resp.TopicRespList), 1)
	topicResp := resp.TopicRespList[0]
	assert.Equal(t, topicResp.Topic, "topic")
	assert.Equal(t, len(topicResp.PartitionRespList), 1)
	partitionResp := topicResp.PartitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	var offset int64 = 0
	assert.Equal(t, partitionResp.Offset, offset)
	var time int64 = -1
	assert.Equal(t, partitionResp.Time, time)
	var logStartOffset int64 = 0
	assert.Equal(t, partitionResp.LogStartOffset, logStartOffset)
}

func TestEncodeProduceRespV7(t *testing.T) {
	produceResp := ProduceResp{
		BaseResp: BaseResp{
			CorrelationId: 2,
		},
	}
	producePartitionResp := &ProducePartitionResp{}
	producePartitionResp.PartitionId = 0
	producePartitionResp.ErrorCode = 0
	producePartitionResp.Offset = 0
	producePartitionResp.Time = -1
	producePartitionResp.LogStartOffset = 0
	produceTopicResp := &ProduceTopicResp{}
	produceTopicResp.Topic = "topic"
	produceTopicResp.PartitionRespList = []*ProducePartitionResp{producePartitionResp}
	produceResp.TopicRespList = []*ProduceTopicResp{produceTopicResp}
	bytes := produceResp.Bytes(7, false)
	expectBytes := testHex2Bytes(t, "00000002000000010005746f706963000000010000000000000000000000000000ffffffffffffffff000000000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeProduceRespV7(t *testing.T) {
	produceRespBytes := testHex2Bytes(t, "00000002000000010005746f706963000000010000000000000000000000000000ffffffffffffffff000000000000000000000000")
	resp, err := DecodeProduceResp(produceRespBytes, 7)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 2)
	assert.Equal(t, len(resp.TopicRespList), 1)
	topicResp := resp.TopicRespList[0]
	assert.Equal(t, topicResp.Topic, "topic")
	codeBytes := resp.Bytes(7, false)
	assert.Equal(t, produceRespBytes, codeBytes)
}

func TestDecodeProduceRespV8(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004000000010005746f706963000000010000000000000000000000000000ffffffffffffffff00000000000000000000000100000000000a74657374206572726f72000e74657374206572726f72206d736700000000")
	resp, err := DecodeProduceResp(bytes, 8)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 4)
	assert.Equal(t, resp.ThrottleTime, 0)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "topic")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	assert.Equal(t, partitionResp.Offset, int64(0))
	assert.Equal(t, partitionResp.Time, int64(-1))
	assert.Equal(t, partitionResp.LogStartOffset, int64(0))
	assert.Equal(t, *partitionResp.ErrorMessage, "test error msg")
	recordErrorList := partitionResp.RecordErrorList
	assert.Len(t, recordErrorList, 1)
	recordError := recordErrorList[0]
	assert.Equal(t, recordError.BatchIndex, int32(0))
	assert.Equal(t, *recordError.BatchIndexErrorMessage, "test error")
}

func TestEncodeProduceRespV8(t *testing.T) {
	produceResp := ProduceResp{
		BaseResp: BaseResp{
			CorrelationId: 4,
		},
	}
	producePartitionResp := &ProducePartitionResp{}
	producePartitionResp.PartitionId = 0
	producePartitionResp.ErrorCode = 0
	producePartitionResp.Offset = 0
	producePartitionResp.Time = -1
	producePartitionResp.LogStartOffset = 0
	produceTopicResp := &ProduceTopicResp{}
	produceTopicResp.Topic = "topic"
	produceTopicResp.PartitionRespList = []*ProducePartitionResp{producePartitionResp}
	produceResp.TopicRespList = []*ProduceTopicResp{produceTopicResp}
	bytes := produceResp.Bytes(8, false)
	expectBytes := testHex2Bytes(t, "00000004000000010005746f706963000000010000000000000000000000000000ffffffffffffffff0000000000000000ffffffffffff00000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeProduceRespV8(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004000000010005746f706963000000010000000000000000000000000000ffffffffffffffff00000000000000000000000100000000000a74657374206572726f72000e74657374206572726f72206d736700000000")
	resp, err := DecodeProduceResp(bytes, 8)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 4)
	assert.Equal(t, resp.ThrottleTime, 0)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "topic")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	assert.Equal(t, partitionResp.Offset, int64(0))
	assert.Equal(t, partitionResp.Time, int64(-1))
	assert.Equal(t, partitionResp.LogStartOffset, int64(0))
	codeBytes := resp.Bytes(8, false)
	assert.Equal(t, bytes, codeBytes)
}
