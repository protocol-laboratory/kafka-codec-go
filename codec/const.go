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

const (
	LenAbortTransactions    = 4
	LenApiV0                = 6
	LenApiV3                = 7
	LenArray                = 4
	LenBaseSequence         = 4
	LenBatchIndex           = 4
	LenClusterAuthOperation = 4
	LenControllerId         = 4
	LenCorrId               = 4
	LenCrc32                = 4
	LenErrorCode            = 2
	LenFetchSessionId       = 4
	LenGenerationId         = 4
	LenIsInternalV1         = 1
	LenIsInternalV9         = 4
	LenLastStableOffset     = 8
	LenLeaderEpoch          = 4
	LenLeaderId             = 4
	LenMagicByte            = 1
	LenMessageSize          = 4
	LenNodeId               = 4
	LenOffset               = 8
	LenOffsetDelta          = 4
	LenPartitionId          = 4
	LenPort                 = 4
	LenProducerId           = 8
	LenProducerEpoch        = 2
	LenRecordAttributes     = 1
	LenReplicaId            = 4
	LenSessionTimeout       = 8
	LenStartOffset          = 8
	LenTaggedField          = 1
	LenThrottleTime         = 4
	LenTime                 = 8
	LenTopicAuthOperation   = 4
)
