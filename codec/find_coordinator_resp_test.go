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

func TestDecodeFindCoordinatorRespV3(t *testing.T) {
	bytes := testHex2Bytes(t, "000000000000000000000000000000000a6c6f63616c686f73740000238400")
	resp, err := DecodeFindCoordinatorResp(bytes, 3)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 0)
	var nodeId int32 = 0
	assert.Equal(t, resp.NodeId, nodeId)
	assert.Equal(t, resp.Host, "localhost")
	assert.Equal(t, resp.Port, 9092)
}

func TestCodeFindCoordinatorRespV0(t *testing.T) {
	findCoordinatorResp := FindCoordinatorResp{}
	findCoordinatorResp.CorrelationId = 1
	findCoordinatorResp.NodeId = 1
	findCoordinatorResp.Host = "localhost"
	findCoordinatorResp.Port = 9092
	bytes := findCoordinatorResp.Bytes(0)
	expectBytes := testHex2Bytes(t, "0000000100000000000100096c6f63616c686f737400002384")
	assert.Equal(t, expectBytes, bytes)
}

func TestCodeFindCoordinatorRespV3(t *testing.T) {
	findCoordinatorResp := FindCoordinatorResp{}
	findCoordinatorResp.CorrelationId = 0
	findCoordinatorResp.NodeId = 0
	findCoordinatorResp.Host = "localhost"
	findCoordinatorResp.Port = 9092
	bytes := findCoordinatorResp.Bytes(3)
	expectBytes := testHex2Bytes(t, "000000000000000000000000000000000a6c6f63616c686f73740000238400")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeCoordinatorRespV3(t *testing.T) {
	bytes := testHex2Bytes(t, "000000000000000000000000000000000a6c6f63616c686f73740000238400")
	resp, err := DecodeFindCoordinatorResp(bytes, 3)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 0)
	var nodeId int32 = 0
	assert.Equal(t, resp.NodeId, nodeId)
	assert.Equal(t, resp.Host, "localhost")
	assert.Equal(t, resp.Port, 9092)
	codeBytes := resp.Bytes(3)
	assert.Equal(t, bytes, codeBytes)
}
