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

func TestDecodeSyncGroupRespV0(t *testing.T) {
	bytes := testHex2Bytes(t, "000000030000000000190001000000010005746f7069630000000100000000ffffffff")
	resp, err := DecodeSyncGroupResp(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 3)
	assert.Equal(t, resp.ErrorCode, NONE)
	assert.Equal(t, resp.MemberAssignment, string(testHex2Bytes(t, "0001000000010005746f7069630000000100000000ffffffff")))
}

func TestCodeSyncGroupRespV0(t *testing.T) {
	syncGroupResp := SyncGroupResp{
		BaseResp: BaseResp{
			CorrelationId: 3,
		},
	}
	syncGroupResp.ErrorCode = 0
	syncGroupResp.MemberAssignment = string(testHex2Bytes(t, "0001000000010005746f7069630000000100000000ffffffff"))
	bytes := syncGroupResp.Bytes(0)
	expectBytes := testHex2Bytes(t, "000000030000000000190001000000010005746f7069630000000100000000ffffffff")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeSyncGroupRespV0(t *testing.T) {
	bytes := testHex2Bytes(t, "000000030000000000190001000000010005746f7069630000000100000000ffffffff")
	resp, err := DecodeSyncGroupResp(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 3)
	assert.Equal(t, resp.ErrorCode, NONE)
	assert.Equal(t, resp.MemberAssignment, string(testHex2Bytes(t, "0001000000010005746f7069630000000100000000ffffffff")))
	codeBytes := resp.Bytes(0)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeSyncGroupRespV4(t *testing.T) {
	bytes := testHex2Bytes(t, "00000006000000000000001b0001000000010006746573742d350000000100000000ffffffff00")
	resp, err := DecodeSyncGroupResp(bytes, 4)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 6)
	assert.Equal(t, resp.ErrorCode, NONE)
	assert.Equal(t, resp.MemberAssignment, string(testHex2Bytes(t, "0001000000010006746573742d350000000100000000ffffffff")))
}

func TestCodeSyncGroupRespV4(t *testing.T) {
	syncGroupResp := SyncGroupResp{
		BaseResp: BaseResp{
			CorrelationId: 6,
		},
	}
	syncGroupResp.MemberAssignment = string(testHex2Bytes(t, "0001000000010006746573742d350000000100000000ffffffff"))
	bytes := syncGroupResp.Bytes(4)
	expectBytes := testHex2Bytes(t, "00000006000000000000001b0001000000010006746573742d350000000100000000ffffffff00")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeSyncGroupRespV4(t *testing.T) {
	bytes := testHex2Bytes(t, "00000006000000000000001b0001000000010006746573742d350000000100000000ffffffff00")
	resp, err := DecodeSyncGroupResp(bytes, 4)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 6)
	assert.Equal(t, resp.ErrorCode, NONE)
	assert.Equal(t, resp.MemberAssignment, string(testHex2Bytes(t, "0001000000010006746573742d350000000100000000ffffffff")))
	codeBytes := resp.Bytes(4)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeSyncGroupRespV5(t *testing.T) {
	bytes := testHex2Bytes(t, "000000430000000000000009636f6e73756d65720672616e676521000100000001000c68706354657374546f7069630000000100000002ffffffff00")
	resp, err := DecodeSyncGroupResp(bytes, 5)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 67)
	assert.Equal(t, resp.ProtocolType, "consumer")
	assert.Equal(t, resp.ProtocolName, "range")
	assert.Equal(t, resp.MemberAssignment, string(testHex2Bytes(t, "000100000001000c68706354657374546f7069630000000100000002ffffffff")))
}

func TestCodeSyncGroupRespV5(t *testing.T) {
	syncGroupResp := SyncGroupResp{
		BaseResp: BaseResp{
			CorrelationId: 67,
		},
	}
	syncGroupResp.ProtocolType = "consumer"
	syncGroupResp.ProtocolName = "range"
	syncGroupResp.MemberAssignment = string(testHex2Bytes(t, "000100000001000c68706354657374546f7069630000000100000002ffffffff"))
	bytes := syncGroupResp.Bytes(5)
	expectBytes := testHex2Bytes(t, "000000430000000000000009636f6e73756d65720672616e676521000100000001000c68706354657374546f7069630000000100000002ffffffff00")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeSyncGroupRespV5(t *testing.T) {
	bytes := testHex2Bytes(t, "000000430000000000000009636f6e73756d65720672616e676521000100000001000c68706354657374546f7069630000000100000002ffffffff00")
	resp, err := DecodeSyncGroupResp(bytes, 5)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 67)
	assert.Equal(t, resp.ProtocolType, "consumer")
	assert.Equal(t, resp.ProtocolName, "range")
	assert.Equal(t, resp.MemberAssignment, string(testHex2Bytes(t, "000100000001000c68706354657374546f7069630000000100000002ffffffff")))
	codeBytes := resp.Bytes(5)
	assert.Equal(t, bytes, codeBytes)
}
