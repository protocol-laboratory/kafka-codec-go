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

func TestDecodeOffsetForLeaderEpochRespV3(t *testing.T) {
	bytes := testHex2Bytes(t, "00000009000000000000000100096c742d746573742d3100000001000000000000000000000000000000000006")
	resp, err := DecodeOffsetForLeaderEpochResp(bytes, 3)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 9)
	assert.Equal(t, resp.ThrottleTime, 0)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "lt-test-1")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.LeaderEpoch, int32(0))
	assert.Equal(t, partitionResp.Offset, int64(6))
	assert.Equal(t, partitionResp.ErrorCode, NONE)
}

func TestEncodeOffsetForLeaderEpochRespV3(t *testing.T) {
	offsetLeaderEpochResp := OffsetForLeaderEpochResp{
		BaseResp: BaseResp{
			CorrelationId: 9,
		},
	}
	offsetLeaderEpochPartitionResp := &OffsetForLeaderEpochPartitionResp{}
	offsetLeaderEpochPartitionResp.ErrorCode = 0
	offsetLeaderEpochPartitionResp.PartitionId = 0
	offsetLeaderEpochPartitionResp.LeaderEpoch = 0
	offsetLeaderEpochPartitionResp.Offset = 6
	offsetLeaderEpochTopicResp := &OffsetForLeaderEpochTopicResp{}
	offsetLeaderEpochTopicResp.Topic = "lt-test-1"
	offsetLeaderEpochTopicResp.PartitionRespList = []*OffsetForLeaderEpochPartitionResp{offsetLeaderEpochPartitionResp}
	offsetLeaderEpochResp.TopicRespList = []*OffsetForLeaderEpochTopicResp{offsetLeaderEpochTopicResp}
	bytes := offsetLeaderEpochResp.Bytes(3)
	expectBytes := testHex2Bytes(t, "00000009000000000000000100096c742d746573742d3100000001000000000000000000000000000000000006")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeOffsetForLeaderEpochRespV3(t *testing.T) {
	bytes := testHex2Bytes(t, "00000009000000000000000100096c742d746573742d3100000001000000000000000000000000000000000006")
	resp, err := DecodeOffsetForLeaderEpochResp(bytes, 3)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 9)
	assert.Equal(t, resp.ThrottleTime, 0)
	topicRespList := resp.TopicRespList
	assert.Len(t, topicRespList, 1)
	topicResp := topicRespList[0]
	assert.Equal(t, topicResp.Topic, "lt-test-1")
	partitionRespList := topicResp.PartitionRespList
	assert.Len(t, partitionRespList, 1)
	partitionResp := partitionRespList[0]
	assert.Equal(t, partitionResp.PartitionId, 0)
	assert.Equal(t, partitionResp.LeaderEpoch, int32(0))
	assert.Equal(t, partitionResp.Offset, int64(6))
	assert.Equal(t, partitionResp.ErrorCode, NONE)
	codeBytes := resp.Bytes(3)
	assert.Equal(t, bytes, codeBytes)
}
