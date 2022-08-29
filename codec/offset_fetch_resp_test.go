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

func TestDecodeOffsetFetchRespV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004000000010005746f7069630000000100000000ffffffffffffffff00000000")
	resp, err := DecodeOffsetFetchResp(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 4)
	assert.Equal(t, resp.ThrottleTime, 0)
	assert.Equal(t, resp.ErrorCode, NONE)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "topic")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.Offset, int64(-1))
	assert.Equal(t, partitionResp.LeaderEpoch, int32(0))
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	assert.Equal(t, *partitionResp.Metadata, "")
}

func TestEncodeOffsetFetchRespV1(t *testing.T) {
	offsetFetchResp := OffsetFetchResp{
		BaseResp: BaseResp{
			CorrelationId: 4,
		},
	}
	offsetFetchPartitionResp := &OffsetFetchPartitionResp{}
	offsetFetchPartitionResp.PartitionId = 0
	offsetFetchPartitionResp.Offset = -1
	var str = ""
	offsetFetchPartitionResp.Metadata = &str
	offsetFetchPartitionResp.ErrorCode = 0
	offsetFetchTopicResp := &OffsetFetchTopicResp{}
	offsetFetchTopicResp.Topic = "topic"
	offsetFetchTopicResp.PartitionRespList = []*OffsetFetchPartitionResp{offsetFetchPartitionResp}
	offsetFetchResp.TopicRespList = []*OffsetFetchTopicResp{offsetFetchTopicResp}
	bytes := offsetFetchResp.Bytes(1)
	expectBytes := testHex2Bytes(t, "00000004000000010005746f7069630000000100000000ffffffffffffffff00000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeOffsetFetchRespV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004000000010005746f7069630000000100000000ffffffffffffffff00000000")
	resp, err := DecodeOffsetFetchResp(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 4)
	assert.Equal(t, resp.ThrottleTime, 0)
	assert.Equal(t, resp.ErrorCode, NONE)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "topic")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.Offset, int64(-1))
	assert.Equal(t, partitionResp.LeaderEpoch, int32(0))
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	assert.Equal(t, *partitionResp.Metadata, "")
	codeBytes := resp.Bytes(1)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeOffsetFetchRespV6(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000700000000000207746573742d350200000000ffffffffffffffffffffffff0100000000000000")
	resp, err := DecodeOffsetFetchResp(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 7)
	assert.Equal(t, resp.ThrottleTime, 0)
	assert.Equal(t, resp.ErrorCode, NONE)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "test-5")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.Offset, int64(-1))
	assert.Equal(t, partitionResp.LeaderEpoch, int32(-1))
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	assert.Equal(t, *partitionResp.Metadata, "")
}

func TestEncodeOffsetFetchRespV6(t *testing.T) {
	offsetFetchResp := OffsetFetchResp{
		BaseResp: BaseResp{
			CorrelationId: 7,
		},
	}
	offsetFetchPartitionResp := &OffsetFetchPartitionResp{}
	offsetFetchPartitionResp.PartitionId = 0
	offsetFetchPartitionResp.Offset = -1
	offsetFetchPartitionResp.LeaderEpoch = -1
	var str = ""
	offsetFetchPartitionResp.Metadata = &str
	offsetFetchPartitionResp.ErrorCode = 0
	offsetFetchTopicResp := &OffsetFetchTopicResp{}
	offsetFetchTopicResp.Topic = "test-5"
	offsetFetchTopicResp.PartitionRespList = []*OffsetFetchPartitionResp{offsetFetchPartitionResp}
	offsetFetchResp.TopicRespList = []*OffsetFetchTopicResp{offsetFetchTopicResp}
	bytes := offsetFetchResp.Bytes(6)
	expectBytes := testHex2Bytes(t, "0000000700000000000207746573742d350200000000ffffffffffffffffffffffff0100000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeOffsetFetchRespV6(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000700000000000207746573742d350200000000ffffffffffffffffffffffff0100000000000000")
	resp, err := DecodeOffsetFetchResp(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 7)
	assert.Equal(t, resp.ThrottleTime, 0)
	assert.Equal(t, resp.ErrorCode, NONE)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "test-5")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.Offset, int64(-1))
	assert.Equal(t, partitionResp.LeaderEpoch, int32(-1))
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	assert.Equal(t, *partitionResp.Metadata, "")
	codeBytes := resp.Bytes(6)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeOffsetFetchRespV7(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000700000000000207746573742d350200000000ffffffffffffffffffffffff0100000000000000")
	resp, err := DecodeOffsetFetchResp(bytes, 7)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 7)
	assert.Equal(t, resp.ThrottleTime, 0)
	assert.Equal(t, resp.ErrorCode, NONE)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "test-5")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.Offset, int64(-1))
	assert.Equal(t, partitionResp.LeaderEpoch, int32(-1))
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	assert.Equal(t, *partitionResp.Metadata, "")
}

func TestEncodeOffsetFetchRespV7(t *testing.T) {
	offsetFetchResp := OffsetFetchResp{
		BaseResp: BaseResp{
			CorrelationId: 7,
		},
	}
	offsetFetchPartitionResp := &OffsetFetchPartitionResp{}
	offsetFetchPartitionResp.PartitionId = 0
	offsetFetchPartitionResp.Offset = -1
	offsetFetchPartitionResp.LeaderEpoch = -1
	var str = ""
	offsetFetchPartitionResp.Metadata = &str
	offsetFetchPartitionResp.ErrorCode = 0
	offsetFetchTopicResp := &OffsetFetchTopicResp{}
	offsetFetchTopicResp.Topic = "test-5"
	offsetFetchTopicResp.PartitionRespList = []*OffsetFetchPartitionResp{offsetFetchPartitionResp}
	offsetFetchResp.TopicRespList = []*OffsetFetchTopicResp{offsetFetchTopicResp}
	bytes := offsetFetchResp.Bytes(7)
	expectBytes := testHex2Bytes(t, "0000000700000000000207746573742d350200000000ffffffffffffffffffffffff0100000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeOffsetFetchRespV7(t *testing.T) {
	offsetFetchResp := OffsetFetchResp{
		BaseResp: BaseResp{
			CorrelationId: 7,
		},
	}
	offsetFetchPartitionResp := &OffsetFetchPartitionResp{}
	offsetFetchPartitionResp.PartitionId = 0
	offsetFetchPartitionResp.Offset = -1
	offsetFetchPartitionResp.LeaderEpoch = -1
	var str = ""
	offsetFetchPartitionResp.Metadata = &str
	offsetFetchPartitionResp.ErrorCode = 0
	offsetFetchTopicResp := &OffsetFetchTopicResp{}
	offsetFetchTopicResp.Topic = "test-5"
	offsetFetchTopicResp.PartitionRespList = []*OffsetFetchPartitionResp{offsetFetchPartitionResp}
	offsetFetchResp.TopicRespList = []*OffsetFetchTopicResp{offsetFetchTopicResp}
	bytes := offsetFetchResp.Bytes(7)
	expectBytes := testHex2Bytes(t, "0000000700000000000207746573742d350200000000ffffffffffffffffffffffff0100000000000000")
	assert.Equal(t, expectBytes, bytes)
	codeBytes := offsetFetchResp.Bytes(7)
	assert.Equal(t, bytes, codeBytes)
}
