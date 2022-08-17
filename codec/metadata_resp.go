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

type MetadataResp struct {
	BaseResp
	ThrottleTime               int
	ErrorCode                  int16
	BrokerMetadataList         []*BrokerMetadata
	ClusterId                  string
	ControllerId               int32
	TopicMetadataList          []*TopicMetadata
	ClusterAuthorizedOperation int
}

type BrokerMetadata struct {
	NodeId int32
	Host   string
	Port   int
	Rack   *string
}

type TopicMetadata struct {
	ErrorCode                ErrorCode
	Topic                    string
	IsInternal               bool
	PartitionMetadataList    []*PartitionMetadata
	TopicAuthorizedOperation int
}

type PartitionMetadata struct {
	ErrorCode       ErrorCode
	PartitionId     int
	LeaderId        int32
	LeaderEpoch     int32
	Replicas        []*Replica
	CaughtReplicas  []*Replica
	OfflineReplicas []*Replica
}

type Replica struct {
	ReplicaId int32
}

func (m *MetadataResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 8 || version == 9 {
		result += LenThrottleTime
	}
	if version == 9 {
		result += LenTaggedField
	}
	if version == 1 || version == 8 {
		result += LenArray
	} else if version == 9 {
		result += CompactArrayLen(len(m.BrokerMetadataList))
	}
	for _, val := range m.BrokerMetadataList {
		result += LenNodeId
		if version == 1 || version == 8 {
			result += StrLen(val.Host)
		} else if version == 9 {
			result += CompactStrLen(val.Host)
		}
		result += LenPort
		if version == 1 || version == 8 {
			result += 2
		} else if version == 9 {
			result += CompactNullableStrLen(val.Rack)
		}
		if version == 9 {
			result += LenTaggedField
		}
	}
	if version == 8 {
		result += StrLen(m.ClusterId)
	} else if version == 9 {
		result += CompactStrLen(m.ClusterId)
	}
	result += LenControllerId
	if version == 1 || version == 8 {
		result += LenArray
	} else if version == 9 {
		result += CompactArrayLen(len(m.TopicMetadataList))
	}
	for _, val := range m.TopicMetadataList {
		result += LenErrorCode
		if version == 1 || version == 8 {
			result += StrLen(val.Topic)
		} else if version == 9 {
			result += CompactStrLen(val.Topic)
		}
		if version == 1 || version == 8 {
			result += LenIsInternalV1
		} else if version == 9 {
			result += LenIsInternalV9
		}
		if version == 1 || version == 8 {
			result += LenArray
		} else if version == 9 {
			result += CompactArrayLen(len(val.PartitionMetadataList))
		}
		for _, partition := range val.PartitionMetadataList {
			result += LenErrorCode + LenPartitionId + LenLeaderId
			if version == 8 || version == 9 {
				result += LenLeaderEpoch
			}
			if version == 1 || version == 8 {
				result += LenArray
			} else if version == 9 {
				result += CompactArrayLen(len(partition.Replicas))
			}
			for range partition.Replicas {
				result += LenReplicaId
			}
			if version == 1 || version == 8 {
				result += LenArray
			} else if version == 9 {
				result += CompactArrayLen(len(partition.CaughtReplicas))
			}
			for range partition.CaughtReplicas {
				result += LenReplicaId
			}
			if version == 8 {
				result += LenArray
				for range partition.OfflineReplicas {
					result += LenReplicaId
				}
			} else if version == 9 {
				result += CompactArrayLen(len(partition.OfflineReplicas))
				for range partition.OfflineReplicas {
					result += LenReplicaId
				}
				result += LenTaggedField
			}
		}
		if version == 8 || version == 9 {
			result += LenTopicAuthOperation
		}
		if version == 9 {
			result += LenTaggedField
		}
	}
	if version == 8 || version == 9 {
		result += LenClusterAuthOperation
	}
	if version == 9 {
		result += LenTaggedField
	}
	return result
}

func (m *MetadataResp) Bytes(version int16) []byte {
	bytes := make([]byte, m.BytesLength(version))
	idx := 0
	idx = putCorrId(bytes, idx, m.CorrelationId)
	if version == 8 || version == 9 {
		idx = putThrottleTime(bytes, idx, 0)
	}
	if version == 9 {
		idx = putTaggedField(bytes, idx)
	}
	if version == 1 || version == 8 {
		idx = putInt(bytes, idx, len(m.BrokerMetadataList))
	} else if version == 9 {
		idx = putVarint(bytes, idx, len(m.BrokerMetadataList))
	}
	for _, brokerMetadata := range m.BrokerMetadataList {
		idx = putNodeId(bytes, idx, brokerMetadata.NodeId)
		if version == 1 || version == 8 {
			idx = putHostString(bytes, idx, brokerMetadata.Host)
		} else if version == 9 {
			idx = putHost(bytes, idx, brokerMetadata.Host)
		}
		idx = putBrokerPort(bytes, idx, brokerMetadata.Port)
		if version == 1 || version == 8 {
			// todo
			bytes[idx] = 255
			idx += 1
			bytes[idx] = 255
			idx += 1
		} else if version == 9 {
			idx = putRack(bytes, idx, brokerMetadata.Rack)
		}
		if version == 9 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 8 {
		idx = putClusterIdString(bytes, idx, m.ClusterId)
	} else if version == 9 {
		idx = putClusterId(bytes, idx, m.ClusterId)
	}
	idx = putControllerId(bytes, idx, m.ControllerId)
	if version == 1 || version == 8 {
		idx = putInt(bytes, idx, len(m.TopicMetadataList))
	} else if version == 9 {
		idx = putVarint(bytes, idx, len(m.TopicMetadataList))
	}
	for _, topicMetadata := range m.TopicMetadataList {
		idx = putErrorCode(bytes, idx, topicMetadata.ErrorCode)
		if version == 1 || version == 8 {
			idx = putTopicString(bytes, idx, topicMetadata.Topic)
		} else if version == 9 {
			idx = putTopic(bytes, idx, topicMetadata.Topic)
		}
		idx = putBool(bytes, idx, topicMetadata.IsInternal)
		if version == 1 || version == 8 {
			idx = putInt(bytes, idx, len(topicMetadata.PartitionMetadataList))
		} else if version == 9 {
			idx = putCompactArrayLen(bytes, idx, len(topicMetadata.PartitionMetadataList))
		}
		for _, partitionMetadata := range topicMetadata.PartitionMetadataList {
			idx = putErrorCode(bytes, idx, partitionMetadata.ErrorCode)
			idx = putPartitionId(bytes, idx, partitionMetadata.PartitionId)
			idx = putLeaderId(bytes, idx, partitionMetadata.LeaderId)
			if version == 8 || version == 9 {
				idx = putLeaderEpoch(bytes, idx, partitionMetadata.LeaderEpoch)
			}
			if version == 1 || version == 8 {
				idx = putInt(bytes, idx, len(partitionMetadata.Replicas))
			} else if version == 9 {
				idx = putVarint(bytes, idx, len(partitionMetadata.Replicas))
			}
			for _, replica := range partitionMetadata.Replicas {
				idx = putReplicaId(bytes, idx, replica.ReplicaId)
			}
			if version == 1 || version == 8 {
				idx = putInt(bytes, idx, len(partitionMetadata.CaughtReplicas))
			} else if version == 9 {
				idx = putVarint(bytes, idx, len(partitionMetadata.CaughtReplicas))
			}
			for _, replica := range partitionMetadata.CaughtReplicas {
				idx = putReplicaId(bytes, idx, replica.ReplicaId)
			}
			if version == 8 {
				idx = putInt(bytes, idx, len(partitionMetadata.OfflineReplicas))
				for _, replica := range partitionMetadata.OfflineReplicas {
					idx = putReplicaId(bytes, idx, replica.ReplicaId)
				}
			} else if version == 9 {
				bytes[idx] = byte(len(partitionMetadata.OfflineReplicas) + 1)
				idx++
				for _, replica := range partitionMetadata.OfflineReplicas {
					idx = putReplicaId(bytes, idx, replica.ReplicaId)
				}
				idx = putTaggedField(bytes, idx)
			}
		}
		if version == 8 || version == 9 {
			idx = putTopicAuthorizedOperation(bytes, idx, topicMetadata.TopicAuthorizedOperation)
		}
		if version == 9 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version == 8 || version == 9 {
		idx = putClusterAuthorizedOperation(bytes, idx, m.ClusterAuthorizedOperation)
	}
	if version == 9 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
