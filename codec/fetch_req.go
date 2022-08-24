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

type FetchReq struct {
	BaseReq
	ReplicaId         int32
	MaxWaitTime       int
	MinBytes          int
	MaxBytes          int
	IsolationLevel    byte
	FetchSessionId    int
	FetchSessionEpoch int32
	TopicReqList      []*FetchTopicReq
}

type FetchTopicReq struct {
	Topic            string
	PartitionReqList []*FetchPartitionReq
}

type FetchPartitionReq struct {
	PartitionId        int
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int
	LogStartOffset     int64
	PartitionMaxBytes  int
}

func DecodeFetchReq(bytes []byte, version int16) (fetchReq *FetchReq, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			fetchReq = nil
		}
	}()
	fetchReq = &FetchReq{}
	fetchReq.ApiVersion = version
	idx := 0
	fetchReq.CorrelationId, idx = readCorrId(bytes, idx)
	fetchReq.ClientId, idx = readClientId(bytes, idx)
	fetchReq.ReplicaId, idx = readReplicaId(bytes, idx)
	fetchReq.MaxWaitTime, idx = readMaxWaitTime(bytes, idx)
	fetchReq.MinBytes, idx = readFetchBytes(bytes, idx)
	fetchReq.MaxBytes, idx = readFetchBytes(bytes, idx)
	fetchReq.IsolationLevel, idx = readIsolationLevel(bytes, idx)
	fetchReq.FetchSessionId, idx = readFetchSessionId(bytes, idx)
	fetchReq.FetchSessionEpoch, idx = readFetchSessionEpoch(bytes, idx)
	var length int
	length, idx = readArrayLen(bytes, idx)
	fetchReq.TopicReqList = make([]*FetchTopicReq, length)
	for i := 0; i < length; i++ {
		topicReq := FetchTopicReq{}
		topicReq.Topic, idx = readTopicString(bytes, idx)
		var pLen int
		pLen, idx = readArrayLen(bytes, idx)
		topicReq.PartitionReqList = make([]*FetchPartitionReq, pLen)
		for j := 0; j < pLen; j++ {
			partition := &FetchPartitionReq{}
			partition.PartitionId, idx = readInt(bytes, idx)
			partition.CurrentLeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			partition.FetchOffset, idx = readOffset(bytes, idx)
			partition.LogStartOffset, idx = readOffset(bytes, idx)
			partition.PartitionMaxBytes, idx = readInt(bytes, idx)
			topicReq.PartitionReqList[j] = partition
		}
		fetchReq.TopicReqList[i] = &topicReq
	}
	return fetchReq, nil
}

func (m *FetchReq) BytesLength(containApiKeyVersion bool) int {
	length := 0
	if containApiKeyVersion {
		length += LenApiKey
		length += LenApiVersion
	}
	length += LenCorrId
	length += StrLen(m.ClientId)
	length += LenReplicaId
	length += LenFetchMaxWaitTime
	length += LenFetchBytes
	length += LenFetchBytes
	length += LenIsolationLevel
	length += LenFetchSessionId
	length += LenFetchSessionEpoch
	length += LenArray
	for _, topicReq := range m.TopicReqList {
		length += StrLen(topicReq.Topic)
		length += LenArray
		for range topicReq.PartitionReqList {
			length += LenPartitionId
			length += LenLeaderEpoch
			length += LenOffset
			length += LenOffset
			length += LenFetchBytes
		}
	}
	return length
}

func (m *FetchReq) Bytes(containApiKeyVersion bool) []byte {
	version := m.ApiVersion
	bytes := make([]byte, m.BytesLength(containApiKeyVersion))
	idx := 0
	if containApiKeyVersion {
		idx = putApiKey(bytes, idx, Fetch)
		idx = putApiVersion(bytes, idx, version)
	}
	idx = putCorrId(bytes, idx, m.CorrelationId)
	idx = putClientId(bytes, idx, m.ClientId)
	idx = putReplicaId(bytes, idx, m.ReplicaId)
	idx = putMaxWaitTime(bytes, idx, m.MaxWaitTime)
	idx = putFetchBytes(bytes, idx, m.MinBytes)
	idx = putFetchBytes(bytes, idx, m.MaxBytes)
	idx = putIsolationLevel(bytes, idx, m.IsolationLevel)
	idx = putFetchSessionId(bytes, idx, m.FetchSessionId)
	idx = putFetchSessionEpoch(bytes, idx, m.FetchSessionEpoch)
	idx = putArrayLen(bytes, idx, len(m.TopicReqList))
	for _, topicReq := range m.TopicReqList {
		idx = putTopicString(bytes, idx, topicReq.Topic)
		idx = putArrayLen(bytes, idx, len(topicReq.PartitionReqList))
		for _, partitionReq := range topicReq.PartitionReqList {
			idx = putPartitionId(bytes, idx, partitionReq.PartitionId)
			idx = putLeaderEpoch(bytes, idx, partitionReq.CurrentLeaderEpoch)
			idx = putOffset(bytes, idx, partitionReq.FetchOffset)
			idx = putOffset(bytes, idx, partitionReq.LogStartOffset)
			idx = putFetchBytes(bytes, idx, partitionReq.PartitionMaxBytes)

		}
	}
	return bytes
}
