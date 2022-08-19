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

func TestCodeProduceRespV7(t *testing.T) {
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
	bytes := produceResp.Bytes(7)
	expectBytes := testHex2Bytes(t, "00000002000000010005746f706963000000010000000000000000000000000000ffffffffffffffff000000000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeProduceRespV8(t *testing.T) {
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
	bytes := produceResp.Bytes(8)
	expectBytes := testHex2Bytes(t, "00000004000000010005746f706963000000010000000000000000000000000000ffffffffffffffff000000000000000000000000ffff00000000")
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
	codeBytes := resp.Bytes(7)
	assert.Equal(t, produceRespBytes, codeBytes)
}
