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

func TestDecodeOffsetCommitRespV2(t *testing.T) {
	expectBytes := testHex2Bytes(t, "00000005000000010005746f70696300000001000000000000")
	resp, err := DecodeOffsetCommitResp(expectBytes, 2)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 5)
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
}

func TestEncodeOffsetCommitRespV2(t *testing.T) {
	offsetCommitPartitionResp := &OffsetCommitPartitionResp{}
	offsetCommitPartitionResp.PartitionId = 0
	offsetCommitPartitionResp.ErrorCode = 0
	offsetCommitTopicResp := &OffsetCommitTopicResp{}
	offsetCommitTopicResp.Topic = "topic"
	offsetCommitTopicResp.PartitionRespList = []*OffsetCommitPartitionResp{offsetCommitPartitionResp}
	offsetCommitResp := OffsetCommitResp{
		BaseResp: BaseResp{
			CorrelationId: 5,
		},
	}
	offsetCommitResp.TopicRespList = []*OffsetCommitTopicResp{offsetCommitTopicResp}
	bytes := offsetCommitResp.Bytes(2)
	expectBytes := testHex2Bytes(t, "00000005000000010005746f70696300000001000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeOffsetCommitRespV2(t *testing.T) {
	expectBytes := testHex2Bytes(t, "00000005000000010005746f70696300000001000000000000")
	resp, err := DecodeOffsetCommitResp(expectBytes, 2)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 5)
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
	codeBytes := resp.Bytes(2)
	assert.Equal(t, expectBytes, codeBytes)
}

func TestDecodeOffsetCommitRespV8(t *testing.T) {
	expectBytes := testHex2Bytes(t, "0000000b00000000000207746573742d3502000000000000000000")
	resp, err := DecodeOffsetCommitResp(expectBytes, 8)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 11)
	assert.Equal(t, resp.ThrottleTime, 0)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "test-5")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.ErrorCode, NONE)
}

func TestEncodeOffsetCommitRespV8(t *testing.T) {
	offsetCommitPartitionResp := &OffsetCommitPartitionResp{}
	offsetCommitPartitionResp.PartitionId = 0
	offsetCommitPartitionResp.ErrorCode = 0
	offsetCommitTopicResp := &OffsetCommitTopicResp{}
	offsetCommitTopicResp.Topic = "test-5"
	offsetCommitTopicResp.PartitionRespList = []*OffsetCommitPartitionResp{offsetCommitPartitionResp}
	offsetCommitResp := OffsetCommitResp{
		BaseResp: BaseResp{
			CorrelationId: 11,
		},
	}
	offsetCommitResp.TopicRespList = []*OffsetCommitTopicResp{offsetCommitTopicResp}
	bytes := offsetCommitResp.Bytes(8)
	expectBytes := testHex2Bytes(t, "0000000b00000000000207746573742d3502000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeOffsetCommitRespV8(t *testing.T) {
	expectBytes := testHex2Bytes(t, "0000000b00000000000207746573742d3502000000000000000000")
	resp, err := DecodeOffsetCommitResp(expectBytes, 8)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 11)
	assert.Equal(t, resp.ThrottleTime, 0)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "test-5")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	codeBytes := resp.Bytes(8)
	assert.Equal(t, expectBytes, codeBytes)
}
