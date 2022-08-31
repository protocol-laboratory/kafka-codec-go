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

type RecordBatch struct {
	Offset      int64
	MessageSize int
	LeaderEpoch int32
	MagicByte   byte
	Flags       uint16

	LastOffsetDelta int
	FirstTimestamp  int64
	LastTimestamp   int64
	ProducerId      int64
	ProducerEpoch   int16
	BaseSequence    int32
	Records         []*Record
}

func DecodeRecordBatch(bytes []byte, version int16) *RecordBatch {
	recordBatch := &RecordBatch{}
	idx := 0
	recordBatch.Offset, idx = readOffset(bytes, idx)
	recordBatch.MessageSize, idx = readMessageSize(bytes, idx)
	recordBatch.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
	recordBatch.MagicByte, idx = readMagicByte(bytes, idx)
	// todo now we skip the crc32
	idx += 4
	recordBatch.Flags, idx = readUInt16(bytes, idx)
	recordBatch.LastOffsetDelta, idx = readLastOffsetDelta(bytes, idx)
	recordBatch.FirstTimestamp, idx = readTime(bytes, idx)
	recordBatch.LastTimestamp, idx = readTime(bytes, idx)
	recordBatch.ProducerId, idx = readProducerId(bytes, idx)
	recordBatch.ProducerEpoch, idx = readProducerEpoch(bytes, idx)
	recordBatch.BaseSequence, idx = readBaseSequence(bytes, idx)
	var length int
	length, idx = readInt(bytes, idx)
	recordBatch.Records = make([]*Record, length)
	for i := 0; i < length; i++ {
		var recordLength int
		recordLength, idx = readVarint(bytes, idx)
		record := DecodeRecord(bytes[idx:idx+recordLength-1], version)
		idx += recordLength
		recordBatch.Records[i] = record
	}
	return recordBatch
}

func (r *RecordBatch) BytesLength() int {
	return LenOffset + LenMessageSize + r.RecordBatchMessageLength()
}

func (r *RecordBatch) RecordBatchMessageLength() int {
	result := LenLeaderEpoch + LenMagicByte + LenCrc32
	// record batch flags
	result += 2
	result += LenOffsetDelta
	result += LenTime
	result += LenTime
	result += LenProducerId
	result += LenProducerEpoch
	result += LenBaseSequence
	result += LenArray
	for _, record := range r.Records {
		// record size
		bytesLength := record.BytesLength()
		result += varintSize(bytesLength)
		result += bytesLength
	}
	return result
}

func (r *RecordBatch) Bytes() []byte {
	bytes := make([]byte, r.BytesLength())
	idx := 0
	idx = putOffset(bytes, idx, r.Offset)
	idx = putMessageSize(bytes, idx, r.RecordBatchMessageLength())
	idx = putLeaderEpoch(bytes, idx, r.LeaderEpoch)
	idx = putMagicByte(bytes, idx, r.MagicByte)
	crc32Start := idx
	// crc32后面计算
	idx += 4
	idx = putUInt16(bytes, idx, r.Flags)
	idx = putLastOffsetDelta(bytes, idx, r.LastOffsetDelta)
	idx = putTime(bytes, idx, r.FirstTimestamp)
	idx = putTime(bytes, idx, r.LastTimestamp)
	idx = putProducerId(bytes, idx, r.ProducerId)
	idx = putProducerEpoch(bytes, idx, r.ProducerEpoch)
	idx = putBaseSequence(bytes, idx, r.BaseSequence)
	idx = putArrayLen(bytes, idx, r.Records)
	for _, record := range r.Records {
		idx = putRecord(bytes, idx, record.Bytes())
	}
	putCrc32(bytes[crc32Start:], bytes[crc32Start+4:])
	return bytes
}
