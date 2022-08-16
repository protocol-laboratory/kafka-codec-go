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

func TestDecodeApiVersionRespV3(t *testing.T) {
	bytes := testHex2Bytes(t, "000000010000300000000000090000010000000d000002000000070000030000000c000008000000080000090000000800000a0000000400000b0000000900000c0000000400000d0000000500000e0000000500000f000000050000100000000400001100000001000012000000030000130000000700001400000006000015000000020000160000000400001700000004000018000000030000190000000300001a0000000300001b0000000100001c0000000300001d0000000200001e0000000200001f0000000200002000000004000021000000020000220000000200002300000003000024000000020000250000000300002a0000000200002b0000000200002c0000000100002d0000000000002e0000000000002f0000000000003000000001000031000000010000390000000000003c0000000000003d000000000000410000000000004200000000000000000000")
	apiResp, err := DecodeApiResp(bytes, 3)
	assert.Nil(t, err)
	assert.Equal(t, 1, apiResp.CorrelationId)
	assert.Equal(t, ErrorCode(0), apiResp.ErrorCode)
	assert.Equal(t, 47, len(apiResp.ApiRespVersions))
}
