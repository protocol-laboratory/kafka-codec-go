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
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeIllegalJoinGroupReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeJoinGroupReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeJoinGroupReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004000570662d6d71000767726f75702d31000075300000753000000008636f6e73756d657200000002000572616e676500000015000100000001000974657374546f706963ffffffff000a726f756e64726f62696e00000015000100000001000974657374546f706963ffffffff")
	joinGroupReq, err := DecodeJoinGroupReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 4, joinGroupReq.CorrelationId)
	assert.Equal(t, "pf-mq", joinGroupReq.ClientId)
	assert.Equal(t, "group-1", joinGroupReq.GroupId)
	assert.Equal(t, 30_000, joinGroupReq.SessionTimeout)
	assert.Equal(t, 30_000, joinGroupReq.RebalanceTimeout)
	assert.Equal(t, "", joinGroupReq.MemberId)
	assert.Equal(t, "consumer", joinGroupReq.ProtocolType)
	assert.Len(t, joinGroupReq.GroupProtocols, 2)
	groupProtocol1 := joinGroupReq.GroupProtocols[0]
	assert.Equal(t, "range", groupProtocol1.ProtocolName)
	assert.Equal(t, testHex2Bytes(t, "000100000001000974657374546f706963ffffffff"), []byte(groupProtocol1.ProtocolMetadata))
	groupProtocol2 := joinGroupReq.GroupProtocols[1]
	assert.Equal(t, "roundrobin", groupProtocol2.ProtocolName)
	assert.Equal(t, testHex2Bytes(t, "000100000001000974657374546f706963ffffffff"), []byte(groupProtocol2.ProtocolMetadata))
}

func TestEncodeJoinGroupReqV1(t *testing.T) {
	joinGroupReq := &JoinGroupReq{}
	joinGroupReq.ApiVersion = 1
	joinGroupReq.CorrelationId = 4
	joinGroupReq.ClientId = "pf-mq"
	joinGroupReq.GroupId = "group-1"
	joinGroupReq.SessionTimeout = 30_000
	joinGroupReq.RebalanceTimeout = 30_000
	joinGroupReq.MemberId = ""
	joinGroupReq.ProtocolType = "consumer"
	joinGroupReq.GroupInstanceId = nil
	groupProtocols := make([]*GroupProtocol, 2)
	groupProtocols[0] = &GroupProtocol{ProtocolName: "range", ProtocolMetadata: testHex2Bytes(t, "000100000001000974657374546f706963ffffffff")}
	groupProtocols[1] = &GroupProtocol{ProtocolName: "roundrobin", ProtocolMetadata: testHex2Bytes(t, "000100000001000974657374546f706963ffffffff")}
	joinGroupReq.GroupProtocols = groupProtocols
	codeBytes := joinGroupReq.Bytes(true, true)
	expectBytes := testHex2Bytes(t, "00000075000b000100000004000570662d6d71000767726f75702d31000075300000753000000008636f6e73756d657200000002000572616e676500000015000100000001000974657374546f706963ffffffff000a726f756e64726f62696e00000015000100000001000974657374546f706963ffffffff")
	assert.Equal(t, expectBytes, codeBytes)
}

func TestDecodeAndCodeJoinGroupReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000004000570662d6d71000767726f75702d31000075300000753000000008636f6e73756d657200000002000572616e676500000015000100000001000974657374546f706963ffffffff000a726f756e64726f62696e00000015000100000001000974657374546f706963ffffffff")
	joinGroupReq, err := DecodeJoinGroupReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 4, joinGroupReq.CorrelationId)
	assert.Equal(t, "pf-mq", joinGroupReq.ClientId)
	assert.Equal(t, "group-1", joinGroupReq.GroupId)
	assert.Equal(t, 30_000, joinGroupReq.SessionTimeout)
	assert.Equal(t, 30_000, joinGroupReq.RebalanceTimeout)
	assert.Equal(t, "", joinGroupReq.MemberId)
	assert.Equal(t, "consumer", joinGroupReq.ProtocolType)
	assert.Len(t, joinGroupReq.GroupProtocols, 2)
	groupProtocol1 := joinGroupReq.GroupProtocols[0]
	assert.Equal(t, "range", groupProtocol1.ProtocolName)
	assert.Equal(t, testHex2Bytes(t, "000100000001000974657374546f706963ffffffff"), []byte(groupProtocol1.ProtocolMetadata))
	groupProtocol2 := joinGroupReq.GroupProtocols[1]
	assert.Equal(t, "roundrobin", groupProtocol2.ProtocolName)
	assert.Equal(t, testHex2Bytes(t, "000100000001000974657374546f706963ffffffff"), []byte(groupProtocol2.ProtocolMetadata))
	codeBytes := joinGroupReq.Bytes(false, false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeJoinGroupReqV6(t *testing.T) {
	bytes := testHex2Bytes(t, "00000008002f636f6e73756d65722d37336664633964612d306439322d346537622d613761372d6563323636663637633137312d31002537336664633964612d306439322d346537622d613761372d65633236366636376331373100002710000493e0010009636f6e73756d6572020672616e676535000100000001002437363465646565332d303037652d343865302d623966392d646637663731336666373037ffffffff000000000000")
	joinGroupReq, err := DecodeJoinGroupReq(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, 8, joinGroupReq.CorrelationId)
	assert.Equal(t, "consumer-73fdc9da-0d92-4e7b-a7a7-ec266f67c171-1", joinGroupReq.ClientId)
	assert.Equal(t, "73fdc9da-0d92-4e7b-a7a7-ec266f67c171", joinGroupReq.GroupId)
	assert.Equal(t, 10_000, joinGroupReq.SessionTimeout)
	assert.Equal(t, 300_000, joinGroupReq.RebalanceTimeout)
	assert.Equal(t, "", joinGroupReq.MemberId)
	assert.Equal(t, "consumer", joinGroupReq.ProtocolType)
	assert.Len(t, joinGroupReq.GroupProtocols, 1)
	groupProtocol := joinGroupReq.GroupProtocols[0]
	assert.Equal(t, "range", groupProtocol.ProtocolName)
}

func TestEncodeJoinGroupReqV6(t *testing.T) {
	joinGroupReq := &JoinGroupReq{}
	joinGroupReq.ApiVersion = 6
	joinGroupReq.CorrelationId = 8
	joinGroupReq.ClientId = "consumer-73fdc9da-0d92-4e7b-a7a7-ec266f67c171-1"
	joinGroupReq.GroupId = "73fdc9da-0d92-4e7b-a7a7-ec266f67c171"
	joinGroupReq.SessionTimeout = 10_000
	joinGroupReq.RebalanceTimeout = 300_000
	joinGroupReq.MemberId = ""
	joinGroupReq.ProtocolType = "consumer"
	joinGroupReq.GroupInstanceId = nil
	groupProtocols := make([]*GroupProtocol, 1)
	protocolMetadata, err := hex.DecodeString("000100000001002437363465646565332d303037652d343865302d623966392d646637663731336666373037ffffffff00000000")
	assert.Nil(t, err)
	groupProtocols[0] = &GroupProtocol{ProtocolName: "range", ProtocolMetadata: protocolMetadata}
	joinGroupReq.GroupProtocols = groupProtocols
	codeBytes := joinGroupReq.Bytes(false, true)
	expectBytes := testHex2Bytes(t, "000b000600000008002f636f6e73756d65722d37336664633964612d306439322d346537622d613761372d6563323636663637633137312d31002537336664633964612d306439322d346537622d613761372d65633236366636376331373100002710000493e0010009636f6e73756d6572020672616e676535000100000001002437363465646565332d303037652d343865302d623966392d646637663731336666373037ffffffff000000000000")
	assert.Equal(t, expectBytes, codeBytes)
}

func TestDecodeAndCodeJoinGroupReqV6(t *testing.T) {
	bytes := testHex2Bytes(t, "00000008002f636f6e73756d65722d37336664633964612d306439322d346537622d613761372d6563323636663637633137312d31002537336664633964612d306439322d346537622d613761372d65633236366636376331373100002710000493e0010009636f6e73756d6572020672616e676535000100000001002437363465646565332d303037652d343865302d623966392d646637663731336666373037ffffffff000000000000")
	joinGroupReq, err := DecodeJoinGroupReq(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, 8, joinGroupReq.CorrelationId)
	assert.Equal(t, "consumer-73fdc9da-0d92-4e7b-a7a7-ec266f67c171-1", joinGroupReq.ClientId)
	assert.Equal(t, "73fdc9da-0d92-4e7b-a7a7-ec266f67c171", joinGroupReq.GroupId)
	codeBytes := joinGroupReq.Bytes(false, false)
	assert.Equal(t, bytes, codeBytes)
}
