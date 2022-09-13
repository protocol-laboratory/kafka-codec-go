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

func TestDecodeIllegalSyncGroupReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeSyncGroupReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeSyncGroupReqV0(t *testing.T) {
	bytes := testHex2Bytes(t, "00000006000570662d6d71000767726f75702d310000001a002a70662d6d712d38636238316534642d303831382d346438342d386337642d61313564373839353231313700000001002a70662d6d712d38636238316534642d303831382d346438342d386337642d6131356437383935323131370000001d000100000001000974657374546f7069630000000100000000ffffffff")
	syncReq, err := DecodeSyncGroupReq(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, 6, syncReq.CorrelationId)
	assert.Equal(t, "pf-mq", syncReq.ClientId)
	assert.Equal(t, "group-1", syncReq.GroupId)
	assert.Equal(t, 26, syncReq.GenerationId)
	assert.Equal(t, "pf-mq-8cb81e4d-0818-4d84-8c7d-a15d78952117", syncReq.MemberId)
	assert.Len(t, syncReq.GroupAssignments, 1)
	groupAssignment := syncReq.GroupAssignments[0]
	assert.Equal(t, "pf-mq-8cb81e4d-0818-4d84-8c7d-a15d78952117", groupAssignment.MemberId)
	assert.Equal(t, testHex2Bytes(t, "000100000001000974657374546f7069630000000100000000ffffffff"), []byte(groupAssignment.MemberAssignment))
}

func TestEncodeSyncGroupReqV0(t *testing.T) {
	syncGroupReq := &SyncGroupReq{}
	syncGroupReq.ApiVersion = 0
	syncGroupReq.CorrelationId = 6
	syncGroupReq.ClientId = "pf-mq"
	syncGroupReq.GroupId = "group-1"
	syncGroupReq.GenerationId = 26
	syncGroupReq.MemberId = "pf-mq-8cb81e4d-0818-4d84-8c7d-a15d78952117"
	assignments := make([]*GroupAssignment, 1)
	assignments[0] = &GroupAssignment{MemberId: "pf-mq-8cb81e4d-0818-4d84-8c7d-a15d78952117", MemberAssignment: testHex2Bytes(t, "000100000001000974657374546f7069630000000100000000ffffffff")}
	syncGroupReq.GroupAssignments = assignments
	codeBytes := syncGroupReq.Bytes(true, true)
	assert.Equal(t, codeBytes, testHex2Bytes(t, "00000099000e000000000006000570662d6d71000767726f75702d310000001a002a70662d6d712d38636238316534642d303831382d346438342d386337642d61313564373839353231313700000001002a70662d6d712d38636238316534642d303831382d346438342d386337642d6131356437383935323131370000001d000100000001000974657374546f7069630000000100000000ffffffff"))
}

func TestDecodeAndCodeSyncGroupReqV0(t *testing.T) {
	bytes := testHex2Bytes(t, "00000006000570662d6d71000767726f75702d310000001a002a70662d6d712d38636238316534642d303831382d346438342d386337642d61313564373839353231313700000001002a70662d6d712d38636238316534642d303831382d346438342d386337642d6131356437383935323131370000001d000100000001000974657374546f7069630000000100000000ffffffff")
	syncReq, err := DecodeSyncGroupReq(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, 6, syncReq.CorrelationId)
	assert.Equal(t, "pf-mq", syncReq.ClientId)
	assert.Equal(t, "group-1", syncReq.GroupId)
	assert.Equal(t, 26, syncReq.GenerationId)
	assert.Equal(t, "pf-mq-8cb81e4d-0818-4d84-8c7d-a15d78952117", syncReq.MemberId)
	assert.Len(t, syncReq.GroupAssignments, 1)
	groupAssignment := syncReq.GroupAssignments[0]
	assert.Equal(t, "pf-mq-8cb81e4d-0818-4d84-8c7d-a15d78952117", groupAssignment.MemberId)
	assert.Equal(t, testHex2Bytes(t, "000100000001000974657374546f7069630000000100000000ffffffff"), []byte(groupAssignment.MemberAssignment))
	codeBytes := syncReq.Bytes(false, false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeSyncGroupReqV4(t *testing.T) {
	bytes := testHex2Bytes(t, "00000006002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31002538646437623936622d366239342d346139622d623263632d3363623538393863396364660000000155636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d366132343963666630376663000255636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d3661323439636666303766631b0001000000010006746573742d350000000100000000ffffffff0000")
	syncReq, err := DecodeSyncGroupReq(bytes, 4)
	assert.Nil(t, err)
	assert.Equal(t, 6, syncReq.CorrelationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1", syncReq.ClientId)
	assert.Equal(t, "8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf", syncReq.GroupId)
	assert.Equal(t, 1, syncReq.GenerationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc", syncReq.MemberId)
	assert.Equal(t, syncReq.ProtocolType, "")
	assert.Equal(t, syncReq.ProtocolName, "")
	assert.Len(t, syncReq.GroupAssignments, 1)
	groupAssignment := syncReq.GroupAssignments[0]
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc", groupAssignment.MemberId)
}

func TestEncodeSyncGroupReqV4(t *testing.T) {
	syncGroupReq := &SyncGroupReq{}
	syncGroupReq.ApiVersion = 4
	syncGroupReq.CorrelationId = 6
	syncGroupReq.ClientId = "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1"
	syncGroupReq.GroupId = "8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf"
	syncGroupReq.GenerationId = 1
	syncGroupReq.MemberId = "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc"
	assignments := make([]*GroupAssignment, 1)
	assignments[0] = &GroupAssignment{MemberId: "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc", MemberAssignment: testHex2Bytes(t, "0001000000010006746573742d350000000100000000ffffffff")}
	syncGroupReq.GroupAssignments = assignments
	codeBytes := syncGroupReq.Bytes(false, true)
	assert.Equal(t, codeBytes, testHex2Bytes(t, "000e000400000006002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31002538646437623936622d366239342d346139622d623263632d3363623538393863396364660000000155636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d366132343963666630376663000255636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d3661323439636666303766631b0001000000010006746573742d350000000100000000ffffffff0000"))
}

func TestDecodeAndCodeSyncGroupReqV4(t *testing.T) {
	bytes := testHex2Bytes(t, "00000006002f636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d31002538646437623936622d366239342d346139622d623263632d3363623538393863396364660000000155636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d366132343963666630376663000255636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d3661323439636666303766631b0001000000010006746573742d350000000100000000ffffffff0000")
	syncGroupReq, err := DecodeSyncGroupReq(bytes, 4)
	assert.Nil(t, err)
	assert.Equal(t, 6, syncGroupReq.CorrelationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1", syncGroupReq.ClientId)
	assert.Equal(t, "8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf", syncGroupReq.GroupId)
	assert.Equal(t, 1, syncGroupReq.GenerationId)
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc", syncGroupReq.MemberId)
	assert.Equal(t, syncGroupReq.ProtocolType, "")
	assert.Equal(t, syncGroupReq.ProtocolName, "")
	assert.Len(t, syncGroupReq.GroupAssignments, 1)
	groupAssignment := syncGroupReq.GroupAssignments[0]
	assert.Equal(t, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc", groupAssignment.MemberId)
	encodeByte := syncGroupReq.Bytes(false, false)
	assert.Equal(t, bytes, encodeByte)
}

func TestDecodeSyncGroupReqV5(t *testing.T) {
	bytes := testHex2Bytes(t, "000000430023636f6e73756d65722d68706354657374546f7069633b7465737447726f75702d312d32001968706354657374546f7069633b7465737447726f75702d310000000149636f6e73756d65722d68706354657374546f7069633b7465737447726f75702d312d322d37633432383830362d393533382d346532352d383930652d3963346565336333303562340009636f6e73756d65720672616e67650100")
	syncGroupReq, err := DecodeSyncGroupReq(bytes, 5)
	assert.Nil(t, err)
	assert.Equal(t, 67, syncGroupReq.CorrelationId)
	assert.Equal(t, "consumer-hpcTestTopic;testGroup-1-2", syncGroupReq.ClientId)
	assert.Equal(t, "hpcTestTopic;testGroup-1", syncGroupReq.GroupId)
	assert.Equal(t, 1, syncGroupReq.GenerationId)
	assert.Equal(t, "consumer-hpcTestTopic;testGroup-1-2-7c428806-9538-4e25-890e-9c4ee3c305b4", syncGroupReq.MemberId)
	assert.Equal(t, syncGroupReq.ProtocolType, "consumer")
	assert.Equal(t, syncGroupReq.ProtocolName, "range")
}

func TestEncodeSyncGroupReqV5(t *testing.T) {
	syncGroupReq := &SyncGroupReq{}
	syncGroupReq.ApiVersion = 5
	syncGroupReq.CorrelationId = 67
	syncGroupReq.ClientId = "consumer-hpcTestTopic;testGroup-1-2"
	syncGroupReq.GroupId = "hpcTestTopic;testGroup-1"
	syncGroupReq.GenerationId = 1
	syncGroupReq.MemberId = "consumer-hpcTestTopic;testGroup-1-2-7c428806-9538-4e25-890e-9c4ee3c305b4"
	syncGroupReq.ProtocolType = "consumer"
	syncGroupReq.ProtocolName = "range"
	codeBytes := syncGroupReq.Bytes(false, true)
	assert.Equal(t, codeBytes, testHex2Bytes(t, "000e0005000000430023636f6e73756d65722d68706354657374546f7069633b7465737447726f75702d312d32001968706354657374546f7069633b7465737447726f75702d310000000149636f6e73756d65722d68706354657374546f7069633b7465737447726f75702d312d322d37633432383830362d393533382d346532352d383930652d3963346565336333303562340009636f6e73756d65720672616e67650100"))
}

func TestDecodeAndCodeSyncGroupReqV5(t *testing.T) {
	bytes := testHex2Bytes(t, "000000430023636f6e73756d65722d68706354657374546f7069633b7465737447726f75702d312d32001968706354657374546f7069633b7465737447726f75702d310000000149636f6e73756d65722d68706354657374546f7069633b7465737447726f75702d312d322d37633432383830362d393533382d346532352d383930652d3963346565336333303562340009636f6e73756d65720672616e67650100")
	syncGroupReq, err := DecodeSyncGroupReq(bytes, 5)
	assert.Nil(t, err)
	assert.Equal(t, 67, syncGroupReq.CorrelationId)
	assert.Equal(t, "consumer-hpcTestTopic;testGroup-1-2", syncGroupReq.ClientId)
	assert.Equal(t, "hpcTestTopic;testGroup-1", syncGroupReq.GroupId)
	assert.Equal(t, 1, syncGroupReq.GenerationId)
	assert.Equal(t, "consumer-hpcTestTopic;testGroup-1-2-7c428806-9538-4e25-890e-9c4ee3c305b4", syncGroupReq.MemberId)
	assert.Equal(t, syncGroupReq.ProtocolType, "consumer")
	assert.Equal(t, syncGroupReq.ProtocolName, "range")
	codeBytes := syncGroupReq.Bytes(false, false)
	assert.Equal(t, bytes, codeBytes)
}
