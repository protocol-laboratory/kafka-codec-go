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

type MetadataReq struct {
	BaseReq
	Topics                             []*MetadataTopicReq
	AllowAutoTopicCreation             bool
	IncludeClusterAuthorizedOperations bool
	IncludeTopicAuthorizedOperations   bool
}

type MetadataTopicReq struct {
	Topic string
}

func DecodeMetadataReq(bytes []byte, version int16) (metadataReq *MetadataReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			metadataReq = nil
		}
	}()
	metadataReq = &MetadataReq{}
	metadataReq.ApiVersion = version
	idx := 0
	metadataReq.CorrelationId, idx = readCorrId(bytes, idx)
	metadataReq.ClientId, idx = readClientId(bytes, idx)
	if version == 9 {
		idx = readTaggedField(bytes, idx)
	}
	var length int
	if version < 9 {
		length, idx = readArrayLen(bytes, idx)
	} else if version == 9 {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	for i := 0; i < length; i++ {
		metadataTopicReq := &MetadataTopicReq{}
		if version < 9 {
			metadataTopicReq.Topic, idx = readTopicString(bytes, idx)
		} else if version == 9 {
			metadataTopicReq.Topic, idx = readTopic(bytes, idx)
			idx = readTaggedField(bytes, idx)
		}
		metadataReq.Topics = append(metadataReq.Topics, metadataTopicReq)
	}
	if version > 3 && version <= 9 {
		metadataReq.AllowAutoTopicCreation, idx = readAllowAutoTopicCreation(bytes, idx)
	}
	if version > 7 && version <= 9 {
		metadataReq.IncludeClusterAuthorizedOperations, idx = readIncludeClusterAuthorizedOperations(bytes, idx)
		metadataReq.IncludeTopicAuthorizedOperations, idx = readIncludeTopicAuthorizedOperations(bytes, idx)
	}
	if version == 9 {
		idx = readTaggedField(bytes, idx)
	}
	return metadataReq, nil
}

func (m *MetadataReq) BytesLength(containLen bool, containApiKeyVersion bool) int {
	version := m.ApiVersion
	length := 0
	if containLen {
		length += LenLength
	}
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(m.ClientId)
	if version == 9 {
		length += LenTaggedField
	}
	if version < 9 {
		length += LenArray
	} else if version == 9 {
		length += CompactArrayLen(len(m.Topics))
	}
	for _, topic := range m.Topics {
		if version < 9 {
			length += StrLen(topic.Topic)
		} else if version == 9 {
			length += CompactStrLen(topic.Topic)
		}
		if version == 9 {
			length += LenTaggedField
		}
	}
	if version > 3 && version <= 9 {
		length += LenAllowAutoTopicCreation
	}
	if version > 7 && version <= 9 {
		length += LenIncludeClusterAuthorizedOperations
		length += LenIncludeTopicAuthorizedOperations
	}
	if version == 9 {
		length += LenTaggedField
	}
	return length
}

func (m *MetadataReq) Bytes(containLen bool, containApiKeyVersion bool) []byte {
	version := m.ApiVersion
	bytes := make([]byte, m.BytesLength(containLen, containApiKeyVersion))
	idx := 0
	if containLen {
		idx = putLength(bytes, idx)
	}
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, Metadata)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, m.CorrelationId)
	idx = putClientId(bytes, idx, m.ClientId)
	if version == 9 {
		idx = putTaggedField(bytes, idx)
	}
	if version < 9 {
		idx = putArrayLen(bytes, idx, m.Topics)
	} else if version == 9 {
		idx = putCompactArrayLen(bytes, idx, len(m.Topics))
	}
	for _, topic := range m.Topics {
		if version < 9 {
			idx = putTopicString(bytes, idx, topic.Topic)
		} else if version == 9 {
			idx = putTopic(bytes, idx, topic.Topic)
		}
		if version == 9 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version > 3 && version <= 9 {
		idx = putAllowAutoTopicCreation(bytes, idx, m.AllowAutoTopicCreation)
	}
	if version > 7 && version <= 9 {
		idx = putIncludeClusterAuthorizedOperations(bytes, idx, m.IncludeClusterAuthorizedOperations)
		idx = putIncludeTopicAuthorizedOperations(bytes, idx, m.IncludeTopicAuthorizedOperations)
	}
	if version == 9 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
