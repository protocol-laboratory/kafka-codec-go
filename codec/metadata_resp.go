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

func DecodeMetadataResp(bytes []byte, version int16) (metadataResp *MetadataResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicToError(r, debug.Stack())
			metadataResp = nil
		}
	}()
	metadataResp = &MetadataResp{}
	idx := 0
	metadataResp.CorrelationId, idx = readCorrId(bytes, idx)
	if version == 9 {
		idx = readTaggedField(bytes, idx)
	}
	if version > 2 && version <= 9 {
		metadataResp.ThrottleTime, idx = readThrottleTime(bytes, idx)
	}
	var length int
	if version < 9 {
		length, idx = readArrayLen(bytes, idx)
	} else if version == 9 {
		length, idx = readCompactArrayLen(bytes, idx)
	}
	metadataResp.BrokerMetadataList = make([]*BrokerMetadata, length)
	for i := 0; i < length; i++ {
		brokerMetadata := &BrokerMetadata{}
		brokerMetadata.NodeId, idx = readNodeId(bytes, idx)
		if version < 9 {
			brokerMetadata.Host, idx = readString(bytes, idx)
		} else if version == 9 {
			brokerMetadata.Host, idx = readCompactString(bytes, idx)
		}
		brokerMetadata.Port, idx = readBrokerPort(bytes, idx)
		if version > 0 && version < 9 {
			brokerMetadata.Rack, idx = readRackNullableString(bytes, idx)
		} else if version == 9 {
			brokerMetadata.Rack, idx = readRack(bytes, idx)
		}
		if version == 9 {
			idx = readTaggedField(bytes, idx)
		}
		metadataResp.BrokerMetadataList[i] = brokerMetadata
	}
	if version > 1 && version < 9 {
		metadataResp.ClusterId, idx = readClusterIdNullableString(bytes, idx)
	} else if version == 9 {
		metadataResp.ClusterId, idx = readClusterId(bytes, idx)
	}
	if version > 0 {
		metadataResp.ControllerId, idx = readControllerId(bytes, idx)
	}
	var topicLength int
	if version < 9 {
		topicLength, idx = readArrayLen(bytes, idx)
	} else if version == 9 {
		topicLength, idx = readCompactArrayLen(bytes, idx)
	}
	metadataResp.TopicMetadataList = make([]*TopicMetadata, topicLength)
	for i := 0; i < topicLength; i++ {
		topicMetadata := &TopicMetadata{}
		topicMetadata.ErrorCode, idx = readErrorCode(bytes, idx)
		if version < 9 {
			topicMetadata.Topic, idx = readString(bytes, idx)
		} else if version == 9 {
			topicMetadata.Topic, idx = readCompactString(bytes, idx)
		}
		if version > 0 {
			topicMetadata.IsInternal, idx = readBool(bytes, idx)
		}
		var partitionLength int
		if version < 9 {
			partitionLength, idx = readArrayLen(bytes, idx)
		} else if version == 9 {
			partitionLength, idx = readCompactArrayLen(bytes, idx)
		}
		topicMetadata.PartitionMetadataList = make([]*PartitionMetadata, partitionLength)
		for j := 0; j < partitionLength; j++ {
			partitionMetadata := &PartitionMetadata{}
			partitionMetadata.ErrorCode, idx = readErrorCode(bytes, idx)
			partitionMetadata.PartitionId, idx = readPartitionId(bytes, idx)
			partitionMetadata.LeaderId, idx = readLeaderId(bytes, idx)
			if version > 6 && version <= 9 {
				partitionMetadata.LeaderEpoch, idx = readLeaderEpoch(bytes, idx)
			}
			var replicaLength int
			if version < 9 {
				replicaLength, idx = readArrayLen(bytes, idx)
			} else if version == 9 {
				replicaLength, idx = readCompactArrayLen(bytes, idx)
			}
			partitionMetadata.Replicas = make([]*Replica, replicaLength)
			for k := 0; k < replicaLength; k++ {
				replica := &Replica{}
				replica.ReplicaId, idx = readReplicaId(bytes, idx)
				partitionMetadata.Replicas[k] = replica
			}
			var caughtReplicaLength int
			if version < 9 {
				caughtReplicaLength, idx = readArrayLen(bytes, idx)
			} else if version == 9 {
				caughtReplicaLength, idx = readCompactArrayLen(bytes, idx)
			}
			partitionMetadata.CaughtReplicas = make([]*Replica, caughtReplicaLength)
			for k := 0; k < caughtReplicaLength; k++ {
				replica := &Replica{}
				replica.ReplicaId, idx = readReplicaId(bytes, idx)
				partitionMetadata.CaughtReplicas[k] = replica
			}
			if version > 4 && version <= 9 {
				var offlineReplicaLength int
				if version < 9 {
					offlineReplicaLength, idx = readArrayLen(bytes, idx)
				} else if version == 9 {
					offlineReplicaLength, idx = readCompactArrayLen(bytes, idx)
				}
				partitionMetadata.OfflineReplicas = make([]*Replica, offlineReplicaLength)
				for k := 0; k < offlineReplicaLength; k++ {
					replica := &Replica{}
					replica.ReplicaId, idx = readReplicaId(bytes, idx)
					partitionMetadata.OfflineReplicas[k] = replica
				}
			}
			topicMetadata.PartitionMetadataList[j] = partitionMetadata
			if version == 9 {
				idx = readTaggedField(bytes, idx)
			}
		}
		if version > 7 && version <= 9 {
			topicMetadata.TopicAuthorizedOperation, idx = readTopicAuthorizedOperation(bytes, idx)
		}
		metadataResp.TopicMetadataList[i] = topicMetadata
		if version == 9 {
			idx = readTaggedField(bytes, idx)
		}
	}
	if version > 7 && version <= 9 {
		metadataResp.ClusterAuthorizedOperation, idx = readClusterAuthorizedOperation(bytes, idx)
	}
	if version == 9 {
		idx = readTaggedField(bytes, idx)
	}
	return metadataResp, nil
}

func (m *MetadataResp) BytesLength(version int16) int {
	result := LenCorrId
	if version == 9 {
		result += LenTaggedField
	}
	if version > 2 && version <= 9 {
		result += LenThrottleTime
	}
	if version < 9 {
		result += LenArray
	} else if version == 9 {
		result += CompactArrayLen(len(m.BrokerMetadataList))
	}
	for _, val := range m.BrokerMetadataList {
		result += LenNodeId
		if version < 9 {
			result += StrLen(val.Host)
		} else if version == 9 {
			result += CompactStrLen(val.Host)
		}
		result += LenPort
		if version > 0 && version < 9 {
			result += NullableStrLen(val.Rack)
		} else if version == 9 {
			result += CompactNullableStrLen(val.Rack)
		}
		if version == 9 {
			result += LenTaggedField
		}
	}
	if version > 1 && version < 9 {
		result += NullableStrLen(&m.ClusterId)
	} else if version == 9 {
		result += CompactStrLen(m.ClusterId)
	}
	if version > 0 {
		result += LenControllerId
	}
	if version < 9 {
		result += LenArray
	} else if version == 9 {
		result += CompactArrayLen(len(m.TopicMetadataList))
	}
	for _, val := range m.TopicMetadataList {
		result += LenErrorCode
		if version < 9 {
			result += StrLen(val.Topic)
		} else if version == 9 {
			result += CompactStrLen(val.Topic)
		}
		if version > 0 {
			result += LenIsInternalV1
		}
		if version < 9 {
			result += LenArray
		} else if version == 9 {
			result += CompactArrayLen(len(val.PartitionMetadataList))
		}
		for _, partition := range val.PartitionMetadataList {
			result += LenErrorCode + LenPartitionId + LenLeaderId
			if version > 6 && version <= 9 {
				result += LenLeaderEpoch
			}
			if version < 9 {
				result += LenArray
			} else if version == 9 {
				result += CompactArrayLen(len(partition.Replicas))
			}
			for range partition.Replicas {
				result += LenReplicaId
			}
			if version < 9 {
				result += LenArray
			} else if version == 9 {
				result += CompactArrayLen(len(partition.CaughtReplicas))
			}
			for range partition.CaughtReplicas {
				result += LenReplicaId
			}
			if version > 4 && version <= 9 {
				if version < 9 {
					result += LenArray
				} else if version == 9 {
					result += CompactArrayLen(len(partition.OfflineReplicas))
				}
				for range partition.OfflineReplicas {
					result += LenReplicaId
				}
			}
			if version == 9 {
				result += LenTaggedField
			}
		}
		if version > 7 && version <= 9 {
			result += LenTopicAuthOperation
		}
		if version == 9 {
			result += LenTaggedField
		}
	}
	if version > 7 && version <= 9 {
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
	if version == 9 {
		idx = putTaggedField(bytes, idx)
	}
	if version > 2 && version <= 9 {
		idx = putThrottleTime(bytes, idx, 0)
	}
	if version < 9 {
		idx = putArrayLen(bytes, idx, len(m.BrokerMetadataList))
	} else if version == 9 {
		idx = putCompactArrayLen(bytes, idx, len(m.BrokerMetadataList))
	}
	for _, brokerMetadata := range m.BrokerMetadataList {
		idx = putNodeId(bytes, idx, brokerMetadata.NodeId)
		if version < 9 {
			idx = putHostString(bytes, idx, brokerMetadata.Host)
		} else if version == 9 {
			idx = putHost(bytes, idx, brokerMetadata.Host)
		}
		idx = putBrokerPort(bytes, idx, brokerMetadata.Port)
		if version > 0 && version < 9 {
			idx = putRackNullableString(bytes, idx, brokerMetadata.Rack)
		} else if version == 9 {
			idx = putRack(bytes, idx, brokerMetadata.Rack)
		}
		if version == 9 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version > 1 && version < 9 {
		idx = putClusterIdNullableString(bytes, idx, m.ClusterId)
	} else if version == 9 {
		idx = putClusterId(bytes, idx, m.ClusterId)
	}
	if version > 0 {
		idx = putControllerId(bytes, idx, m.ControllerId)
	}
	if version < 9 {
		idx = putArrayLen(bytes, idx, len(m.TopicMetadataList))
	} else if version == 9 {
		idx = putCompactArrayLen(bytes, idx, len(m.TopicMetadataList))
	}
	for _, topicMetadata := range m.TopicMetadataList {
		idx = putErrorCode(bytes, idx, topicMetadata.ErrorCode)
		if version < 9 {
			idx = putTopicString(bytes, idx, topicMetadata.Topic)
		} else if version == 9 {
			idx = putTopic(bytes, idx, topicMetadata.Topic)
		}
		if version > 0 {
			idx = putBool(bytes, idx, topicMetadata.IsInternal)
		}
		if version < 9 {
			idx = putArrayLen(bytes, idx, len(topicMetadata.PartitionMetadataList))
		} else if version == 9 {
			idx = putCompactArrayLen(bytes, idx, len(topicMetadata.PartitionMetadataList))
		}
		for _, partitionMetadata := range topicMetadata.PartitionMetadataList {
			idx = putErrorCode(bytes, idx, partitionMetadata.ErrorCode)
			idx = putPartitionId(bytes, idx, partitionMetadata.PartitionId)
			idx = putLeaderId(bytes, idx, partitionMetadata.LeaderId)
			if version > 6 && version <= 9 {
				idx = putLeaderEpoch(bytes, idx, partitionMetadata.LeaderEpoch)
			}
			if version < 9 {
				idx = putArrayLen(bytes, idx, len(partitionMetadata.Replicas))
			} else if version == 9 {
				idx = putCompactArrayLen(bytes, idx, len(partitionMetadata.Replicas))
			}
			for _, replica := range partitionMetadata.Replicas {
				idx = putReplicaId(bytes, idx, replica.ReplicaId)
			}
			if version < 9 {
				idx = putArrayLen(bytes, idx, len(partitionMetadata.CaughtReplicas))
			} else if version == 9 {
				idx = putCompactArrayLen(bytes, idx, len(partitionMetadata.CaughtReplicas))
			}
			for _, replica := range partitionMetadata.CaughtReplicas {
				idx = putReplicaId(bytes, idx, replica.ReplicaId)
			}
			if version > 4 && version <= 9 {
				if version < 9 {
					idx = putArrayLen(bytes, idx, len(partitionMetadata.OfflineReplicas))
				} else if version == 9 {
					idx = putCompactArrayLen(bytes, idx, len(partitionMetadata.OfflineReplicas))
				}
				for _, replica := range partitionMetadata.OfflineReplicas {
					idx = putReplicaId(bytes, idx, replica.ReplicaId)
				}
			}
			if version == 9 {
				idx = putTaggedField(bytes, idx)
			}
		}
		if version > 7 && version <= 9 {
			idx = putTopicAuthorizedOperation(bytes, idx, topicMetadata.TopicAuthorizedOperation)
		}
		if version == 9 {
			idx = putTaggedField(bytes, idx)
		}
	}
	if version > 7 && version <= 9 {
		idx = putClusterAuthorizedOperation(bytes, idx, m.ClusterAuthorizedOperation)
	}
	if version == 9 {
		idx = putTaggedField(bytes, idx)
	}
	return bytes
}
