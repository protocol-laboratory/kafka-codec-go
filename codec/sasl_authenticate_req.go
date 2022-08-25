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

type SaslAuthenticateReq struct {
	BaseReq
	Username string
	Password string
}

func DecodeSaslAuthenticateReq(bytes []byte, version int16) (authReq *SaslAuthenticateReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			authReq = nil
		}
	}()
	authReq = &SaslAuthenticateReq{}
	authReq.ApiVersion = version
	idx := 0
	authReq.CorrelationId, idx = readCorrId(bytes, idx)
	authReq.ClientId, idx = readClientId(bytes, idx)
	var saslBytes []byte
	if version == 1 {
		saslBytes, idx = readBytes(bytes, idx)
	} else if version == 2 {
		idx = readTaggedField(bytes, idx)
		saslBytes, idx = readCompactBytes(bytes, idx)
	}
	authReq.Username, authReq.Password = readSaslUsernamePwdByAuthBytes(saslBytes, 0)
	if version == 2 {
		idx = readTaggedField(bytes, idx)
	}
	return authReq, nil
}

func (s *SaslAuthenticateReq) BytesLength(containApiKeyVersion bool) int {
	version := s.ApiVersion
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(s.ClientId)
	authBytes := generateSaslAuthUsernamePwdBytes(s.Username, s.Password)
	if version == 1 {
		length += BytesLen(authBytes)
	} else if version == 2 {
		length += LenTaggedField
		length += CompactBytesLen(authBytes)
	}
	if version == 2 {
		length += LenTaggedField
	}
	return length
}

func (s *SaslAuthenticateReq) Bytes(containApiKeyVersion bool) []byte {
	version := s.ApiVersion
	bytes := make([]byte, s.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, SaslAuthenticate)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, s.CorrelationId)
	idx = putClientId(bytes, idx, s.ClientId)
	authBytes := generateSaslAuthUsernamePwdBytes(s.Username, s.Password)
	if version == 1 {
		idx = putSaslAuthBytes(bytes, idx, authBytes)
	} else if version == 2 {
		idx = putTaggedField(bytes, idx)
		idx = putSaslAuthBytesCompact(bytes, idx, authBytes)
	}
	if version == 2 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
