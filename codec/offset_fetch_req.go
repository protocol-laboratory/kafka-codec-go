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

type OffsetFetchReq struct {
	BaseReq
	GroupId             string
	TopicReqList        []*OffsetFetchTopicReq
	RequireStableOffset bool
}

type OffsetFetchTopicReq struct {
	Topic            string
	PartitionReqList []*OffsetFetchPartitionReq
}

type OffsetFetchPartitionReq struct {
	PartitionId int
}

func DecodeOffsetFetchReq(bytes []byte, version int16) (fetchReq *OffsetFetchReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			fetchReq = nil
		}
	}()
	fetchReq = &OffsetFetchReq{}
	fetchReq.ApiVersion = version
	idx := 0
	fetchReq.CorrelationId, idx = readCorrId(bytes, idx)
	fetchReq.ClientId, idx = readClientId(bytes, idx)
	if version == 6 || version == 7 {
		idx = readTaggedField(bytes, idx)
	}
	if version == 1 {
		fetchReq.GroupId, idx = readGroupIdString(bytes, idx)
	} else if version == 6 || version == 7 {
		fetchReq.GroupId, idx = readGroupId(bytes, idx)
	}
	var length int
	if version == 1 {
		length, idx = readArrayLen(bytes, idx)
	} else if version == 6 || version == 7 {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	if length > len(bytes) {
		return nil, InvalidProtocolContent
	}
	fetchReq.TopicReqList = make([]*OffsetFetchTopicReq, length)
	for i := 0; i < length; i++ {
		topic := OffsetFetchTopicReq{}
		if version == 1 {
			topic.Topic, idx = readTopicString(bytes, idx)
		} else if version == 6 || version == 7 {
			topic.Topic, idx = readTopic(bytes, idx)
		}
		var partitionLen int
		if version == 1 {
			partitionLen, idx = readArrayLen(bytes, idx)
		} else if version == 6 || version == 7 {
			partitionLen, idx = readCompactArrayLen(bytes, idx)
		}
		if partitionLen > len(bytes) {
			return nil, InvalidProtocolContent
		}
		topic.PartitionReqList = make([]*OffsetFetchPartitionReq, partitionLen)
		for j := 0; j < partitionLen; j++ {
			o := &OffsetFetchPartitionReq{}
			o.PartitionId, idx = readPartitionId(bytes, idx)
			topic.PartitionReqList[j] = o
		}
		if version == 6 || version == 7 {
			idx = readTaggedField(bytes, idx)
		}
		fetchReq.TopicReqList[i] = &topic
	}
	if version == 7 {
		if bytes[idx] == 1 {
			fetchReq.RequireStableOffset = true
		} else {
			fetchReq.RequireStableOffset = false
		}
		idx += 1
	}
	if version == 6 || version == 7 {
		idx = readTaggedField(bytes, idx)
	}
	return fetchReq, nil
}

func (o *OffsetFetchReq) BytesLength(containApiKeyVersion bool) int {
	version := o.ApiVersion
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(o.ClientId)
	if version == 6 || version == 7 {
		length += LenTaggedField
	}
	if version == 1 {
		length += StrLen(o.GroupId)
	} else if version == 6 || version == 7 {
		length += CompactStrLen(o.GroupId)
	}
	if version == 1 {
		length += LenArray
	} else if version == 6 || version == 7 {
		length += CompactArrayLen(len(o.TopicReqList))
	}
	for _, topicReq := range o.TopicReqList {
		if version == 1 {
			length += StrLen(topicReq.Topic)
			length += LenArray
		} else if version == 6 || version == 7 {
			length += CompactStrLen(topicReq.Topic)
			length += CompactArrayLen(len(topicReq.PartitionReqList))
		}
		for range topicReq.PartitionReqList {
			length += LenPartitionId
		}
		if version == 6 || version == 7 {
			length += LenTaggedField
		}
	}
	if version == 7 {
		length += LenRequireStableOffset
	}
	if version == 6 || version == 7 {
		length += LenTaggedField
	}
	return length
}

func (o *OffsetFetchReq) Bytes(containApiKeyVersion bool) []byte {
	version := o.ApiVersion
	bytes := make([]byte, o.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, OffsetFetch)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, o.CorrelationId)
	idx = putClientId(bytes, idx, o.ClientId)
	if version == 6 || version == 7 {
		idx = putTaggedField(bytes, idx)
	}
	if version == 1 {
		idx = putGroupIdString(bytes, idx, o.GroupId)
	} else if version == 6 || version == 7 {
		idx = putGroupId(bytes, idx, o.GroupId)
	}
	if version == 1 {
		idx = putArrayLen(bytes, idx, len(o.TopicReqList))
	} else if version == 6 || version == 7 {
		idx = putCompactArrayLen(bytes, idx, len(o.TopicReqList))
	}
	for _, topicReq := range o.TopicReqList {
		if version == 1 {
			idx = putTopicString(bytes, idx, topicReq.Topic)
			idx = putArrayLen(bytes, idx, len(topicReq.PartitionReqList))
		} else if version == 6 || version == 7 {
			idx = putTopic(bytes, idx, topicReq.Topic)
			idx = putCompactArrayLen(bytes, idx, len(topicReq.PartitionReqList))
		}
		for _, partitionReq := range topicReq.PartitionReqList {
			idx = putPartitionId(bytes, idx, partitionReq.PartitionId)
		}
		if version == 6 || version == 7 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 7 {
		idx = putBool(bytes, idx, o.RequireStableOffset)
	}
	if version == 6 || version == 7 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
