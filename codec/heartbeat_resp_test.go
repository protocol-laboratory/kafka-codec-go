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

func TestDecodeHeartbeatRespV4(t *testing.T) {
	bytes := testHex2Bytes(t, "000000110000000000001b00")
	resp, err := DecodeHeartbeatResp(bytes, 4)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 17)
	assert.Equal(t, resp.ThrottleTime, 0)
	assert.Equal(t, resp.ErrorCode, REBALANCE_IN_PROGRESS)
}

func TestEncodeHeartbeatRespV4(t *testing.T) {
	heartBeatResp := HeartbeatResp{
		BaseResp: BaseResp{
			CorrelationId: 17,
		},
	}
	bytes := heartBeatResp.Bytes(4)
	expectBytes := testHex2Bytes(t, "000000110000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestEncodeHeartbeatRespWithErrV4(t *testing.T) {
	heartBeatResp := HeartbeatResp{
		BaseResp: BaseResp{
			CorrelationId: 17,
		},
		ErrorCode: REBALANCE_IN_PROGRESS,
	}
	bytes := heartBeatResp.Bytes(4)
	expectBytes := testHex2Bytes(t, "000000110000000000001b00")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeHeartbeatRespV4(t *testing.T) {
	bytes := testHex2Bytes(t, "000000110000000000001b00")
	resp, err := DecodeHeartbeatResp(bytes, 4)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 17)
	assert.Equal(t, resp.ThrottleTime, 0)
	assert.Equal(t, resp.ErrorCode, REBALANCE_IN_PROGRESS)
	codeBytes := resp.Bytes(4)
	assert.Equal(t, bytes, codeBytes)
}
