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

func TestDecodeHeartbeatReqV0(t *testing.T) {
	bytes := testHex2Bytes(t, "00000008000570662d6d71000767726f75702d3100000001002a70662d6d712d33316465643736652d396463312d343430332d613465652d316432343330346137396237")
	req, err := DecodeHeartbeatReq(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, req.CorrelationId, 8)
	assert.Equal(t, req.ClientId, "pf-mq")
	assert.Equal(t, req.GenerationId, 1)
	assert.Equal(t, req.GroupId, "group-1")
	assert.Equal(t, req.MemberId, "pf-mq-31ded76e-9dc1-4403-a4ee-1d24304a79b7")
	assert.Nil(t, req.GroupInstanceId)
}

func TestEncodeHeartbeatReqV0(t *testing.T) {
	heartbeatReq := &HeartbeatReq{}
	heartbeatReq.ApiVersion = 0
	heartbeatReq.CorrelationId = 8
	heartbeatReq.GenerationId = 1
	heartbeatReq.ClientId = "pf-mq"
	heartbeatReq.GroupId = "group-1"
	heartbeatReq.MemberId = "pf-mq-31ded76e-9dc1-4403-a4ee-1d24304a79b7"
	codeBytes := heartbeatReq.Bytes(true, true)
	assert.Equal(t, testHex2Bytes(t, "00000048000c000000000008000570662d6d71000767726f75702d3100000001002a70662d6d712d33316465643736652d396463312d343430332d613465652d316432343330346137396237"), codeBytes)
}

func TestDecodeAndCodeHeartbeatReqV0(t *testing.T) {
	bytes := testHex2Bytes(t, "00000008000570662d6d71000767726f75702d3100000001002a70662d6d712d33316465643736652d396463312d343430332d613465652d316432343330346137396237")
	req, err := DecodeHeartbeatReq(bytes, 0)
	assert.Nil(t, err)
	assert.Equal(t, req.CorrelationId, 8)
	assert.Equal(t, req.ClientId, "pf-mq")
	assert.Equal(t, req.GenerationId, 1)
	assert.Equal(t, req.GroupId, "group-1")
	assert.Equal(t, req.MemberId, "pf-mq-31ded76e-9dc1-4403-a4ee-1d24304a79b7")
	assert.Nil(t, req.GroupInstanceId)
	codeBytes := req.Bytes(false, false)
	assert.Equal(t, bytes, codeBytes)
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

func TestEncodeHeartbeatReqV4(t *testing.T) {
	heartbeatReq := &HeartbeatReq{}
	heartbeatReq.ApiVersion = 4
	heartbeatReq.CorrelationId = 125
	heartbeatReq.GenerationId = 2
	heartbeatReq.ClientId = "consumer-hpcTestTopic;group-hpc-1-1"
	heartbeatReq.GroupId = "hpcTestTopic;group-hpc-1"
	heartbeatReq.MemberId = "consumer-hpcTestTopic;group-hpc-1-1-fd91f933-9520-4cc9-940f-5a8affe97ef7"
	codeBytes := heartbeatReq.Bytes(true, true)
	assert.Equal(t, testHex2Bytes(t, "00000096000c00040000007d0023636f6e73756d65722d68706354657374546f7069633b67726f75702d6870632d312d31001968706354657374546f7069633b67726f75702d6870632d310000000249636f6e73756d65722d68706354657374546f7069633b67726f75702d6870632d312d312d66643931663933332d393532302d346363392d393430662d3561386166666539376566370000"), codeBytes)
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
	codeBytes := req.Bytes(false, false)
	assert.Equal(t, bytes, codeBytes)
}
