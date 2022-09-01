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

type SaslHandshakeReq struct {
	BaseReq
	SaslMechanism string
}

func DecodeSaslHandshakeReq(bytes []byte, version int16) (saslHandshakeReq *SaslHandshakeReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			saslHandshakeReq = nil
		}
	}()
	req := &SaslHandshakeReq{}
	req.ApiVersion = version
	idx := 0
	req.CorrelationId, idx = readCorrId(bytes, idx)
	req.ClientId, idx = readClientId(bytes, idx)
	req.SaslMechanism, idx = readSaslMechanism(bytes, idx)
	return req, nil
}

func (s *SaslHandshakeReq) BytesLength(containLen bool, containApiKeyVersion bool) int {
	length := 0
	if containLen {
		length += LenLength
	}
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(s.ClientId)
	length += StrLen(s.SaslMechanism)
	return length
}

func (s *SaslHandshakeReq) Bytes(containLen bool, containApiKeyVersion bool) []byte {
	version := s.ApiVersion
	bytes := make([]byte, s.BytesLength(containLen, containApiKeyVersion))
	idx := 0
	if containLen {
		idx = putLength(bytes, idx)
	}
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, SaslHandshake)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, s.CorrelationId)
	idx = putClientId(bytes, idx, s.ClientId)
	idx = putSaslMechanism(bytes, idx, s.SaslMechanism)
	return bytes
}
