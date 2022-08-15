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

type SaslAuthenticateResp struct {
	BaseResp
	ErrorCode       ErrorCode
	ErrorMessage    string
	AuthBytes       []byte
	SessionLifetime int64
}

func (s *SaslAuthenticateResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 2 {
		result += LenTaggedField
	}
	result += LenErrorCode
	if version == 1 {
		result += StrLen(s.ErrorMessage)
	} else if version == 2 {
		result += CompactStrLen(s.ErrorMessage)
	}
	if version == 1 {
		result += BytesLen(s.AuthBytes)
	} else if version == 2 {
		result += CompactBytesLen(s.AuthBytes)
	}
	result += LenSessionTimeout
	if version == 2 {
		result += LenTaggedField
	}
	return result
}

func (s *SaslAuthenticateResp) Bytes(version int16) []byte {
	bytes := make([]byte, s.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, s.CorrelationId)
	if version == 2 {
		idx = putTaggedField(bytes, idx)
	}
	idx = putErrorCode(bytes, idx, s.ErrorCode)
	if version == 1 {
		idx = putErrorMessageString(bytes, idx, s.ErrorMessage)
	} else if version == 2 {
		idx = putErrorMessage(bytes, idx, s.ErrorMessage)
	}
	if version == 1 {
		idx = putSaslAuthBytes(bytes, idx, s.AuthBytes)
	} else if version == 2 {
		idx = putSaslAuthBytesCompact(bytes, idx, s.AuthBytes)
	}
	idx = putSessionLifeTimeout(bytes, idx, s.SessionLifetime)
	if version == 2 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
