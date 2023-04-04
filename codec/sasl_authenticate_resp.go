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

type SaslAuthenticateResp struct {
	BaseResp
	ErrorCode       ErrorCode
	ErrorMessage    string
	AuthBytes       []byte
	SessionLifetime int64
}

func DecodeSaslAuthenticateResp(bytes []byte, version int16) (resp *SaslAuthenticateResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			resp = nil
		}
	}()
	resp = &SaslAuthenticateResp{}
	idx := 0
	resp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 2 {
		idx = readTaggedField(bytes, idx)
	}
	resp.ErrorCode, idx = readErrorCode(bytes, idx)
	if version <= 1 {
		resp.ErrorMessage, idx = readErrorMessageString(bytes, idx)
	} else if version == 2 {
		resp.ErrorMessage, idx = readErrorMessage(bytes, idx)
	}
	if version <= 1 {
		resp.AuthBytes, idx = readSaslAuthBytes(bytes, idx)
	} else if version == 2 {
		resp.AuthBytes, idx = readSaslAuthBytesCompact(bytes, idx)
	}
	if version > 0 {
		resp.SessionLifetime, idx = readSessionLifeTimeout(bytes, idx)
	}
	if version == 2 {
		idx = readTaggedField(bytes, idx)
	}
	return resp, nil
}

func (s *SaslAuthenticateResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 2 {
		result += LenTaggedField
	}
	result += LenErrorCode
	if version <= 1 {
		result += StrLen(s.ErrorMessage)
	} else if version == 2 {
		result += CompactStrLen(s.ErrorMessage)
	}
	if version <= 1 {
		result += BytesLen(s.AuthBytes)
	} else if version == 2 {
		result += CompactBytesLen(s.AuthBytes)
	}
	if version > 0 {
		result += LenSessionTimeout
	}
	if version == 2 {
		result += LenTaggedField
	}
	return result
}

func (s *SaslAuthenticateResp) Bytes(version int16, containLen bool) []byte {
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
	if version == 2 {
		idx = putTaggedField(bytes, idx)
	}
	idx = putErrorCode(bytes, idx, s.ErrorCode)
	if version <= 1 {
		idx = putErrorMessageString(bytes, idx, s.ErrorMessage)
	} else if version == 2 {
		idx = putErrorMessage(bytes, idx, s.ErrorMessage)
	}
	if version <= 1 {
		idx = putSaslAuthBytes(bytes, idx, s.AuthBytes)
	} else if version == 2 {
		idx = putSaslAuthBytesCompact(bytes, idx, s.AuthBytes)
	}
	if version > 0 {
		idx = putSessionLifeTimeout(bytes, idx, s.SessionLifetime)
	}
	if version == 2 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
