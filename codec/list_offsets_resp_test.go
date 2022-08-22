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

func TestCodeListOffsetsRespV1(t *testing.T) {
	listOffsetPartitionResp := &ListOffsetsPartitionResp{}
	listOffsetPartitionResp.PartitionId = 0
	listOffsetPartitionResp.ErrorCode = 0
	listOffsetPartitionResp.Timestamp = -1
	listOffsetPartitionResp.Offset = 1
	listOffsetTopicResp := &ListOffsetsTopicResp{}
	listOffsetTopicResp.Topic = "topic"
	listOffsetTopicResp.PartitionRespList = []*ListOffsetsPartitionResp{listOffsetPartitionResp}
	listOffsetResp := ListOffsetsResp{
		BaseResp: BaseResp{
			CorrelationId: 4,
		},
	}
	listOffsetResp.TopicRespList = []*ListOffsetsTopicResp{listOffsetTopicResp}
	bytes := listOffsetResp.Bytes(1)
	expectBytes := testHex2Bytes(t, "00000004000000010005746f70696300000001000000000000ffffffffffffffff0000000000000001")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeListOffsetsRespV5(t *testing.T) {
	listOffsetPartitionResp := &ListOffsetsPartitionResp{}
	listOffsetPartitionResp.PartitionId = 0
	listOffsetPartitionResp.ErrorCode = 0
	listOffsetPartitionResp.Timestamp = -1
	listOffsetPartitionResp.Offset = 0
	listOffsetPartitionResp.LeaderEpoch = 0
	listOffsetTopicResp := &ListOffsetsTopicResp{}
	listOffsetTopicResp.Topic = "test-5"
	listOffsetTopicResp.PartitionRespList = []*ListOffsetsPartitionResp{listOffsetPartitionResp}
	listOffsetResp := ListOffsetsResp{
		BaseResp: BaseResp{
			CorrelationId: 8,
		},
	}
	listOffsetResp.TopicRespList = []*ListOffsetsTopicResp{listOffsetTopicResp}
	bytes := listOffsetResp.Bytes(5)
	expectBytes := testHex2Bytes(t, "0000000800000000000000010006746573742d3500000001000000000000ffffffffffffffff000000000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeListOffsetsRespV6(t *testing.T) {
	listOffsetPartitionResp0 := &ListOffsetsPartitionResp{}
	listOffsetPartitionResp0.PartitionId = 4
	listOffsetPartitionResp0.ErrorCode = 0
	listOffsetPartitionResp0.Timestamp = -2
	listOffsetPartitionResp0.Offset = 24602524
	listOffsetPartitionResp0.LeaderEpoch = 0

	listOffsetPartitionResp1 := &ListOffsetsPartitionResp{}
	listOffsetPartitionResp1.PartitionId = 2
	listOffsetPartitionResp1.ErrorCode = 0
	listOffsetPartitionResp1.Timestamp = -2
	listOffsetPartitionResp1.Offset = 24677045
	listOffsetPartitionResp1.LeaderEpoch = 0

	listOffsetPartitionResp2 := &ListOffsetsPartitionResp{}
	listOffsetPartitionResp2.PartitionId = 3
	listOffsetPartitionResp2.ErrorCode = 0
	listOffsetPartitionResp2.Timestamp = -2
	listOffsetPartitionResp2.Offset = 24587841
	listOffsetPartitionResp2.LeaderEpoch = 0

	listOffsetPartitionResp3 := &ListOffsetsPartitionResp{}
	listOffsetPartitionResp3.PartitionId = 0
	listOffsetPartitionResp3.ErrorCode = 0
	listOffsetPartitionResp3.Timestamp = -2
	listOffsetPartitionResp3.Offset = 25352012
	listOffsetPartitionResp3.LeaderEpoch = 0

	listOffsetPartitionResp4 := &ListOffsetsPartitionResp{}
	listOffsetPartitionResp4.PartitionId = 1
	listOffsetPartitionResp4.ErrorCode = 0
	listOffsetPartitionResp4.Timestamp = -2
	listOffsetPartitionResp4.Offset = 26764493
	listOffsetPartitionResp4.LeaderEpoch = 0
	listOffsetTopicResp := &ListOffsetsTopicResp{}
	listOffsetTopicResp.Topic = "hpcTestTopic"
	listOffsetTopicResp.PartitionRespList = []*ListOffsetsPartitionResp{listOffsetPartitionResp0, listOffsetPartitionResp1,
		listOffsetPartitionResp2, listOffsetPartitionResp3, listOffsetPartitionResp4}
	listOffsetResp := ListOffsetsResp{
		BaseResp: BaseResp{
			CorrelationId: 9,
		},
	}
	listOffsetResp.TopicRespList = []*ListOffsetsTopicResp{listOffsetTopicResp}
	bytes := listOffsetResp.Bytes(6)
	expectBytes := testHex2Bytes(t, "000000090000000000020d68706354657374546f70696306000000040000fffffffffffffffe000000000177679c0000000000000000020000fffffffffffffffe0000000001788ab50000000000000000030000fffffffffffffffe0000000001772e410000000000000000000000fffffffffffffffe000000000182d74c0000000000000000010000fffffffffffffffe00000000019864cd00000000000000")
	assert.Equal(t, expectBytes, bytes)
}
