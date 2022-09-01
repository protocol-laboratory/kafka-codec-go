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

func TestDecodeIllegalSaslHandshakeAuthReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeSaslAuthenticateReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeSaslHandshakeAuthReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffffa002f636f6e73756d65722d33616332336137662d346333362d343064392d393964342d6163646134376430613438642d310000000c00616c69636500616c696365")
	saslHandshakeAuthReq, err := DecodeSaslAuthenticateReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 2147483642, saslHandshakeAuthReq.CorrelationId)
	assert.Equal(t, "consumer-3ac23a7f-4c36-40d9-99d4-acda47d0a48d-1", saslHandshakeAuthReq.ClientId)
	assert.Equal(t, "alice", saslHandshakeAuthReq.Username)
	assert.Equal(t, "alice", saslHandshakeAuthReq.Password)
}

func TestEncodeSaslHandshakeAuthReqV1(t *testing.T) {
	saslHandshakeAuthReq := &SaslAuthenticateReq{}
	saslHandshakeAuthReq.ApiVersion = 1
	saslHandshakeAuthReq.CorrelationId = 2147483642
	saslHandshakeAuthReq.ClientId = "consumer-3ac23a7f-4c36-40d9-99d4-acda47d0a48d-1"
	saslHandshakeAuthReq.Username = "alice"
	saslHandshakeAuthReq.Password = "alice"
	codeBytes := saslHandshakeAuthReq.Bytes(false, true)
	assert.Equal(t, codeBytes, testHex2Bytes(t, "002400017ffffffa002f636f6e73756d65722d33616332336137662d346333362d343064392d393964342d6163646134376430613438642d310000000c00616c69636500616c696365"))
}

func TestDecodeAndCodeSaslHandshakeAuthReqV1(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffffa002f636f6e73756d65722d33616332336137662d346333362d343064392d393964342d6163646134376430613438642d310000000c00616c69636500616c696365")
	saslHandshakeAuthReq, err := DecodeSaslAuthenticateReq(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, 2147483642, saslHandshakeAuthReq.CorrelationId)
	assert.Equal(t, "consumer-3ac23a7f-4c36-40d9-99d4-acda47d0a48d-1", saslHandshakeAuthReq.ClientId)
	assert.Equal(t, "alice", saslHandshakeAuthReq.Username)
	assert.Equal(t, "alice", saslHandshakeAuthReq.Password)
	codeBytes := saslHandshakeAuthReq.Bytes(false, false)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeSaslHandshakeAuthReqV2(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffffa002f636f6e73756d65722d33616332336137662d346333362d343064392d393964342d6163646134376430613438642d31000d00616c69636500616c69636500")
	saslHandshakeAuthReq, err := DecodeSaslAuthenticateReq(bytes, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2147483642, saslHandshakeAuthReq.CorrelationId)
	assert.Equal(t, "consumer-3ac23a7f-4c36-40d9-99d4-acda47d0a48d-1", saslHandshakeAuthReq.ClientId)
	assert.Equal(t, "alice", saslHandshakeAuthReq.Username)
	assert.Equal(t, "alice", saslHandshakeAuthReq.Password)
}

func TestEncodeSaslHandshakeAuthReqV2(t *testing.T) {
	saslHandshakeAuthReq := &SaslAuthenticateReq{}
	saslHandshakeAuthReq.ApiVersion = 2
	saslHandshakeAuthReq.CorrelationId = 2147483642
	saslHandshakeAuthReq.ClientId = "consumer-3ac23a7f-4c36-40d9-99d4-acda47d0a48d-1"
	saslHandshakeAuthReq.Username = "alice"
	saslHandshakeAuthReq.Password = "alice"
	codeBytes := saslHandshakeAuthReq.Bytes(true, true)
	assert.Equal(t, codeBytes, testHex2Bytes(t, "00000048002400027ffffffa002f636f6e73756d65722d33616332336137662d346333362d343064392d393964342d6163646134376430613438642d31000d00616c69636500616c69636500"))
}

func TestDecodeAndCodeSaslHandshakeAuthReqV2(t *testing.T) {
	bytes := testHex2Bytes(t, "7ffffffa002f636f6e73756d65722d33616332336137662d346333362d343064392d393964342d6163646134376430613438642d31000d00616c69636500616c69636500")
	saslHandshakeAuthReq, err := DecodeSaslAuthenticateReq(bytes, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2147483642, saslHandshakeAuthReq.CorrelationId)
	assert.Equal(t, "consumer-3ac23a7f-4c36-40d9-99d4-acda47d0a48d-1", saslHandshakeAuthReq.ClientId)
	assert.Equal(t, "alice", saslHandshakeAuthReq.Username)
	assert.Equal(t, "alice", saslHandshakeAuthReq.Password)
	codeBytes := saslHandshakeAuthReq.Bytes(false, false)
	assert.Equal(t, bytes, codeBytes)
}
