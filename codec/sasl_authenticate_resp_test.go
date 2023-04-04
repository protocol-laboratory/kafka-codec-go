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

func TestDecodeSaslAuthenticateRespV0(t *testing.T) {
	bytes := testHex2Bytes(t, "000000030000000000000000")
	resp, err := DecodeSaslAuthenticateResp(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 3)
	assert.Equal(t, resp.ErrorCode, NONE)
}

func TestEncodeSaslHandshakeAuthRespV0(t *testing.T) {
	saslHandshakeAuthResp := SaslAuthenticateResp{
		BaseResp: BaseResp{
			CorrelationId: 3,
		},
	}
	bytes := saslHandshakeAuthResp.Bytes(0, false)
	expectBytes := testHex2Bytes(t, "000000030000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeSaslAuthenticateRespV0(t *testing.T) {
	bytes := testHex2Bytes(t, "000000030000000000000000")
	resp, err := DecodeSaslAuthenticateResp(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 3)
	assert.Equal(t, resp.ErrorCode, NONE)
	codeBytes := resp.Bytes(0, false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeSaslAuthenticateRespV1(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffffa00000000000000000000000000000000")
	resp, err := DecodeSaslAuthenticateResp(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 2147483642)
	assert.Equal(t, resp.ErrorCode, NONE)
	assert.Equal(t, resp.SessionLifetime, int64(0))
}

func TestEncodeSaslHandshakeAuthRespV1(t *testing.T) {
	saslHandshakeAuthResp := SaslAuthenticateResp{
		BaseResp: BaseResp{
			CorrelationId: 2147483642,
		},
	}
	bytes := saslHandshakeAuthResp.Bytes(1, false)
	expectBytes := testHex2Bytes(t, "7ffffffa00000000000000000000000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeSaslAuthenticateRespV1(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffffa00000000000000000000000000000000")
	resp, err := DecodeSaslAuthenticateResp(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 2147483642)
	assert.Equal(t, resp.ErrorCode, NONE)
	assert.Equal(t, resp.SessionLifetime, int64(0))
	codeBytes := resp.Bytes(1, false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeSaslAuthenticateRespV2(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffffa000000010d00736c69636500736c696365000000000000138800")
	resp, err := DecodeSaslAuthenticateResp(bytes, 2)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 2147483642)
	assert.Equal(t, resp.ErrorCode, NONE)
	assert.Equal(t, resp.SessionLifetime, int64(5000))
	authBytes := generateSaslAuthUsernamePwdBytes("slice", "slice")
	assert.Equal(t, resp.AuthBytes, authBytes)
}

func TestEncodeSaslHandshakeAuthRespV2(t *testing.T) {
	saslHandshakeAuthResp := SaslAuthenticateResp{
		BaseResp: BaseResp{
			CorrelationId: 2147483642,
		},
	}
	saslHandshakeAuthResp.SessionLifetime = 5000
	authBytes := generateSaslAuthUsernamePwdBytes("slice", "slice")
	saslHandshakeAuthResp.AuthBytes = authBytes
	codeBytes := saslHandshakeAuthResp.Bytes(2, false)
	expectBytes := testHex2Bytes(t, "7ffffffa000000010d00736c69636500736c696365000000000000138800")
	assert.Equal(t, expectBytes, codeBytes)
}

func TestDecodeAndCodeSaslAuthenticateRespV2(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffffa000000010d00736c69636500736c696365000000000000138800")
	resp, err := DecodeSaslAuthenticateResp(bytes, 2)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 2147483642)
	assert.Equal(t, resp.ErrorCode, NONE)
	assert.Equal(t, resp.SessionLifetime, int64(5000))
	authBytes := generateSaslAuthUsernamePwdBytes("slice", "slice")
	assert.Equal(t, resp.AuthBytes, authBytes)
	codeBytes := resp.Bytes(2, false)
	assert.Equal(t, bytes, codeBytes)
}
