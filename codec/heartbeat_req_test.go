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

func TestDecodeIllegalHeartbeatReq(t *testing.T) {
	bytes := make([]byte, 0)
	_, err := DecodeHeartbeatReq(bytes, 0)
	assert.NotNil(t, err)
}

func TestDecodeHeartbeatReqV4(t *testing.T) {
	bytes := testHex2Bytes(t, "0000007d0023636f6e73756d65722d68706354657374546f7069633b67726f75702d6870632d312d31001968706354657374546f7069633b67726f75702d6870632d310000000249636f6e73756d65722d68706354657374546f7069633b67726f75702d6870632d312d312d66643931663933332d393532302d346363392d393430662d3561386166666539376566370000")
	req, err := DecodeHeartbeatReq(bytes, 4)
	assert.Nil(t, err)
	assert.Equal(t, req.CorrelationId, 125)
	assert.Equal(t, req.ClientId, "consumer-hpcTestTopic;group-hpc-1-1")
	assert.Equal(t, req.GenerationId, 2)
	assert.Equal(t, req.GroupId, "hpcTestTopic;group-hpc-1")
	assert.Equal(t, req.MemberId, "consumer-hpcTestTopic;group-hpc-1-1-fd91f933-9520-4cc9-940f-5a8affe97ef7")
	assert.Nil(t, req.GroupInstanceId)
}

func TestCodeHeartbeatReqV4(t *testing.T) {
	heartbeatReq := &HeartbeatReq{}
	heartbeatReq.ApiVersion = 4
	heartbeatReq.CorrelationId = 125
	heartbeatReq.GenerationId = 2
	heartbeatReq.ClientId = "consumer-hpcTestTopic;group-hpc-1-1"
	heartbeatReq.GroupId = "hpcTestTopic;group-hpc-1"
	heartbeatReq.MemberId = "consumer-hpcTestTopic;group-hpc-1-1-fd91f933-9520-4cc9-940f-5a8affe97ef7"
	codeBytes := heartbeatReq.Bytes(true)
	assert.Equal(t, testHex2Bytes(t, "000c00040000007d0023636f6e73756d65722d68706354657374546f7069633b67726f75702d6870632d312d31001968706354657374546f7069633b67726f75702d6870632d310000000249636f6e73756d65722d68706354657374546f7069633b67726f75702d6870632d312d312d66643931663933332d393532302d346363392d393430662d3561386166666539376566370000"), codeBytes)
}

func TestDecodeAndCodeHeartbeatReqV4(t *testing.T) {
	bytes := testHex2Bytes(t, "0000007d0023636f6e73756d65722d68706354657374546f7069633b67726f75702d6870632d312d31001968706354657374546f7069633b67726f75702d6870632d310000000249636f6e73756d65722d68706354657374546f7069633b67726f75702d6870632d312d312d66643931663933332d393532302d346363392d393430662d3561386166666539376566370000")
	req, err := DecodeHeartbeatReq(bytes, 4)
	assert.Nil(t, err)
	assert.Equal(t, req.CorrelationId, 125)
	assert.Equal(t, req.ClientId, "consumer-hpcTestTopic;group-hpc-1-1")
	assert.Equal(t, req.GenerationId, 2)
	assert.Equal(t, req.GroupId, "hpcTestTopic;group-hpc-1")
	assert.Equal(t, req.MemberId, "consumer-hpcTestTopic;group-hpc-1-1-fd91f933-9520-4cc9-940f-5a8affe97ef7")
	assert.Nil(t, req.GroupInstanceId)
	codeBytes := req.Bytes(false)
	assert.Equal(t, bytes, codeBytes)
}
