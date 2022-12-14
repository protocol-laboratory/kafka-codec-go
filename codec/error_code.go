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

type ErrorCode int16

const (
	// UNKNOWN_SERVER_ERROR
	// The server experienced an unexpected error when processing the request.
	UNKNOWN_SERVER_ERROR ErrorCode = iota - 1
	NONE
	// OFFSET_OUT_OF_RANGE
	// The requested offset is not within the range of offsets maintained by the server.
	OFFSET_OUT_OF_RANGE
	// CORRUPT_MESSAGE
	// This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.
	CORRUPT_MESSAGE
	// UNKNOWN_TOPIC_OR_PARTITION
	// This server does not host this topic-partition.
	UNKNOWN_TOPIC_OR_PARTITION
	// INVALID_FETCH_SIZE
	// The requested fetch size is invalid.
	INVALID_FETCH_SIZE
	// LEADER_NOT_AVAILABLE
	// There is no leader for this topic-partition as we are in the middle of a leadership election.
	LEADER_NOT_AVAILABLE
	// NOT_LEADER_OR_FOLLOWER
	// For requests intended only for the leader, this error indicates that the broker is not the current leader.
	// For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.
	NOT_LEADER_OR_FOLLOWER
	// REQUEST_TIMED_OUT
	// The request timed out.
	REQUEST_TIMED_OUT

	BROKER_NOT_AVAILABLE

	REPLICA_NOT_AVAILABLE

	MESSAGE_TOO_LARGE

	STALE_CONTROLLER_EPOCH

	OFFSET_METADATA_TOO_LARGE

	NETWORK_EXCEPTION

	COORDINATOR_LOAD_IN_PROGRESS

	COORDINATOR_NOT_AVAILABLE

	NOT_COORDINATOR

	INVALID_TOPIC_EXCEPTION

	RECORD_LIST_TOO_LARGE

	NOT_ENOUGH_REPLICAS

	NOT_ENOUGH_REPLICAS_AFTER_APPEND

	INVALID_REQUIRED_ACKS

	ILLEGAL_GENERATION

	INCONSISTENT_GROUP_PROTOCOL

	INVALID_GROUP_ID

	UNKNOWN_MEMBER_ID

	INVALID_SESSION_TIMEOUT

	REBALANCE_IN_PROGRESS

	INVALID_COMMIT_OFFSET_SIZE

	TOPIC_AUTHORIZATION_FAILED

	GROUP_AUTHORIZATION_FAILED

	CLUSTER_AUTHORIZATION_FAILED

	INVALID_TIMESTAMP

	UNSUPPORTED_SASL_MECHANISM

	ILLEGAL_SASL_STATE

	UNSUPPORTED_VERSION

	TOPIC_ALREADY_EXISTS

	INVALID_PARTITIONS

	INVALID_REPLICATION_FACTOR

	INVALID_REPLICA_ASSIGNMENT

	INVALID_CONFIG

	NOT_CONTROLLER

	INVALID_REQUEST

	UNSUPPORTED_FOR_MESSAGE_FORMAT

	POLICY_VIOLATION

	OUT_OF_ORDER_SEQUENCE_NUMBER

	DUPLICATE_SEQUENCE_NUMBER

	INVALID_PRODUCER_EPOCH

	INVALID_TXN_STATE

	INVALID_PRODUCER_ID_MAPPING

	INVALID_TRANSACTION_TIMEOUT

	CONCURRENT_TRANSACTIONS

	TRANSACTION_COORDINATOR_FENCED

	TRANSACTIONAL_ID_AUTHORIZATION_FAILED

	SECURITY_DISABLED

	OPERATION_NOT_ATTEMPTED

	KAFKA_STORAGE_ERROR

	LOG_DIR_NOT_FOUND

	SASL_AUTHENTICATION_FAILED

	UNKNOWN_PRODUCER_ID

	REASSIGNMENT_IN_PROGRESS

	DELEGATION_TOKEN_AUTH_DISABLED

	DELEGATION_TOKEN_NOT_FOUND

	DELEGATION_TOKEN_OWNER_MISMATCH

	DELEGATION_TOKEN_REQUEST_NOT_ALLOWED

	DELEGATION_TOKEN_AUTHORIZATION_FAILED

	DELEGATION_TOKEN_EXPIRED

	INVALID_PRINCIPAL_TYPE

	NON_EMPTY_GROUP

	GROUP_ID_NOT_FOUND

	FETCH_SESSION_ID_NOT_FOUND

	INVALID_FETCH_SESSION_EPOCH

	LISTENER_NOT_FOUND

	TOPIC_DELETION_DISABLED

	FENCED_LEADER_EPOCH

	UNKNOWN_LEADER_EPOCH

	UNSUPPORTED_COMPRESSION_TYPE

	STALE_BROKER_EPOCH

	OFFSET_NOT_AVAILABLE
	// MEMBER_ID_REQUIRED
	// The group member needs to have a valid member id before actually entering a consumer group.
	MEMBER_ID_REQUIRED

	PREFERRED_LEADER_NOT_AVAILABLE

	GROUP_MAX_SIZE_REACHED

	FENCED_INSTANCE_ID

	ELIGIBLE_LEADERS_NOT_AVAILABLE

	ELECTION_NOT_NEEDED

	NO_REASSIGNMENT_IN_PROGRESS

	GROUP_SUBSCRIBED_TO_TOPIC

	INVALID_RECORD

	UNSTABLE_OFFSET_COMMIT

	THROTTLING_QUOTA_EXCEEDED

	PRODUCER_FENCED

	RESOURCE_NOT_FOUND

	DUPLICATE_RESOURCE

	UNACCEPTABLE_CREDENTIAL

	INCONSISTENT_VOTER_SET

	INVALID_UPDATE_VERSION

	FEATURE_UPDATE_FAILED

	PRINCIPAL_DESERIALIZATION_FAILURE

	SNAPSHOT_NOT_FOUND

	POSITION_OUT_OF_RANGE

	UNKNOWN_TOPIC_ID

	DUPLICATE_BROKER_REGISTRATION

	BROKER_ID_NOT_REGISTERED

	INCONSISTENT_TOPIC_ID

	INCONSISTENT_CLUSTER_ID
)
