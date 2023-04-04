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

func TestDecodeSaslHandshakeRespV1(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffff90000000000010005504c41494e")
	resp, err := DecodeSaslHandshakeResp(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 2147483641)
	assert.Equal(t, resp.ErrorCode, NONE)
	mechanisms := resp.EnableMechanisms
	assert.Len(t, mechanisms, 1)
	assert.Equal(t, mechanisms[0].SaslMechanism, "PLAIN")
}

func TestEncodeSaslHandshakeRespV1(t *testing.T) {
	saslHandshakeResp := SaslHandshakeResp{
		BaseResp: BaseResp{
			CorrelationId: 2147483641,
		},
	}
	plainSaslMechanism := &EnableMechanism{SaslMechanism: "PLAIN"}
	saslHandshakeResp.EnableMechanisms = []*EnableMechanism{plainSaslMechanism}
	bytes := saslHandshakeResp.Bytes(1, false)
	expectBytes := testHex2Bytes(t, "7ffffff90000000000010005504c41494e")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeSaslHandshakeRespV1(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffff90000000000010005504c41494e")
	resp, err := DecodeSaslHandshakeResp(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 2147483641)
	assert.Equal(t, resp.ErrorCode, NONE)
	mechanisms := resp.EnableMechanisms
	assert.Len(t, mechanisms, 1)
	assert.Equal(t, mechanisms[0].SaslMechanism, "PLAIN")
	codeBytes := resp.Bytes(1, false)
	assert.Equal(t, bytes, codeBytes)
}
