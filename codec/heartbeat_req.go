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
	"runtime/debug"
)

type HeartbeatReq struct {
	BaseReq
	GroupId         string
	GenerationId    int
	MemberId        string
	GroupInstanceId *string
}

func DecodeHeartbeatReq(bytes []byte, version int16) (heartBeatReq *HeartbeatReq, r any, stack []byte) {
	defer func() {
		if r = recover(); r != nil {
			stack = debug.Stack()
			heartBeatReq = nil
		}
	}()
	heartBeatReq = &HeartbeatReq{}
	heartBeatReq.ApiVersion = version
	idx := 0
	heartBeatReq.CorrelationId, idx = readCorrId(bytes, idx)
	heartBeatReq.ClientId, idx = readClientId(bytes, idx)
	idx = readTaggedField(bytes, idx)
	heartBeatReq.GroupId, idx = readGroupId(bytes, idx)
	heartBeatReq.GenerationId, idx = readGenerationId(bytes, idx)
	heartBeatReq.MemberId, idx = readMemberId(bytes, idx)
	heartBeatReq.GroupInstanceId, idx = readGroupInstanceId(bytes, idx)
	idx = readTaggedField(bytes, idx)
	return heartBeatReq, nil, nil
}
