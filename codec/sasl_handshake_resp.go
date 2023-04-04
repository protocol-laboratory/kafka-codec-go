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

import "runtime/debug"

type SaslHandshakeResp struct {
	BaseResp
	ErrorCode        ErrorCode
	EnableMechanisms []*EnableMechanism
}

type EnableMechanism struct {
	SaslMechanism string
}

func DecodeSaslHandshakeResp(bytes []byte, version int16) (resp *SaslHandshakeResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &SaslHandshakeResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	resp.ErrorCode, idx = readErrorCode(bytes, idx)
	var enableMechanismsLen int
	enableMechanismsLen, idx = readArrayLen(bytes, idx)
	for i := 0; i < enableMechanismsLen; i++ {
		saslMechanism := &EnableMechanism{}
		saslMechanism.SaslMechanism, idx = readSaslMechanism(bytes, idx)
		resp.EnableMechanisms = append(resp.EnableMechanisms, saslMechanism)
	}
	return resp, nil
}

func (s *SaslHandshakeResp) BytesLength(version int16) int {
	length := LenCorrId + LenErrorCode + LenArray
	for _, val := range s.EnableMechanisms {
		length += StrLen(val.SaslMechanism)
	}
	return length
}

func (s *SaslHandshakeResp) Bytes(version int16, containLen bool) []byte {
	length := s.BytesLength(version)
	var bytes []byte
	idx := 0
	if containLen {
		bytes = make([]byte, length+4)
		idx = putInt(bytes, idx, length)
	} else {
		bytes = make([]byte, length)
	}

	idx = putCorrId(bytes, idx, s.CorrelationId)
	idx = putErrorCode(bytes, idx, s.ErrorCode)
	idx = putArrayLen(bytes, idx, s.EnableMechanisms)
	for _, val := range s.EnableMechanisms {
		idx = putSaslMechanism(bytes, idx, val.SaslMechanism)
	}
	return bytes
}
