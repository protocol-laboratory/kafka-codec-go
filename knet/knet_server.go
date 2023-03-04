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

package knet

import (
	"encoding/binary"
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"net"
)

type KafkaNetServerConfig struct {
	Host      string
	Port      int
	BufferMax int
}

type KafkaNetServerImpl interface {
	ConnectionClosed(conn net.Conn)

	AcceptError(conn net.Conn, err error)
	ReadError(conn net.Conn, err error)
	ReactError(conn net.Conn, err error)
	WriteError(conn net.Conn, err error)

	UnSupportedApi(conn net.Conn, apiKey codec.ApiCode, apiVersion int16)
	ApiVersion(conn net.Conn, req *codec.ApiReq) (*codec.ApiResp, error)
	Fetch(conn net.Conn, req *codec.FetchReq) (*codec.FetchResp, error)
	FindCoordinator(conn net.Conn, req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, error)
	Heartbeat(conn net.Conn, req *codec.HeartbeatReq) (*codec.HeartbeatResp, error)
	JoinGroup(conn net.Conn, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error)
	LeaveGroup(conn net.Conn, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error)
	ListOffsets(conn net.Conn, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, error)
	Metadata(conn net.Conn, req *codec.MetadataReq) (*codec.MetadataResp, error)
	OffsetCommit(conn net.Conn, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, error)
	OffsetFetch(conn net.Conn, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, error)
	OffsetForLeaderEpoch(conn net.Conn, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, error)
	Produce(conn net.Conn, req *codec.ProduceReq) (*codec.ProduceResp, error)
	SaslAuthenticate(conn net.Conn, req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, error)
	SaslHandshake(conn net.Conn, req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, error)
	SyncGroup(conn net.Conn, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error)
}

func (p *KafkaNetServerConfig) addr() string {
	return fmt.Sprintf("%s:%d", p.Host, p.Port)
}

type KafkaNetServer struct {
	listener net.Listener
	impl     KafkaNetServerImpl
	config   KafkaNetServerConfig
}

func (k *KafkaNetServer) run() {
	for {
		conn, err := k.listener.Accept()
		if err != nil {
			k.impl.AcceptError(conn, err)
			break
		}
		go k.handleConn(&kafkaConn{
			conn: conn,
			buffer: &buffer{
				max:    k.config.BufferMax,
				bytes:  make([]byte, k.config.BufferMax),
				cursor: 0,
			},
		})
	}
}

type kafkaConn struct {
	conn   net.Conn
	buffer *buffer
}

func (k *KafkaNetServer) handleConn(kafkaConn *kafkaConn) {
	for {
		readLen, err := kafkaConn.conn.Read(kafkaConn.buffer.bytes[kafkaConn.buffer.cursor:])
		if err != nil {
			if isEof(err) {
				k.impl.ConnectionClosed(kafkaConn.conn)
			} else {
				k.impl.ReadError(kafkaConn.conn, err)
				k.impl.ConnectionClosed(kafkaConn.conn)
			}
			break
		}
		kafkaConn.buffer.cursor += readLen
		if kafkaConn.buffer.cursor < 4 {
			continue
		}
		length := codec.FourByteLength(kafkaConn.buffer.bytes) + 4
		if kafkaConn.buffer.cursor < length {
			continue
		}
		if length > kafkaConn.buffer.max {
			k.impl.ReadError(kafkaConn.conn, fmt.Errorf("message too long: %d", length))
			break
		}
		dstBytes, err := k.react(kafkaConn, kafkaConn.buffer.bytes[4:length])
		if err != nil {
			k.impl.ReactError(kafkaConn.conn, err)
			k.impl.ConnectionClosed(kafkaConn.conn)
			break
		}
		write, err := kafkaConn.conn.Write(dstBytes)
		if err != nil {
			k.impl.WriteError(kafkaConn.conn, err)
			k.impl.ConnectionClosed(kafkaConn.conn)
			break
		}
		if write != len(dstBytes) {
			k.impl.WriteError(kafkaConn.conn, fmt.Errorf("write %d bytes, but expect %d bytes", write, len(dstBytes)))
			k.impl.ConnectionClosed(kafkaConn.conn)
			break
		}
		kafkaConn.buffer.cursor -= length
		copy(kafkaConn.buffer.bytes[:kafkaConn.buffer.cursor], kafkaConn.buffer.bytes[length:])
	}
}

func (k *KafkaNetServer) react(kafkaConn *kafkaConn, bytes []byte) ([]byte, error) {
	if len(bytes) < 5 {
		return nil, fmt.Errorf("message too short: %d", len(bytes))
	}
	apiKey := codec.ApiCode(binary.BigEndian.Uint16(bytes))
	apiVersion := int16(binary.BigEndian.Uint16(bytes[2:]))

	switch apiKey {
	case codec.ApiVersions:
		req, err := codec.DecodeApiReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, action := k.impl.ApiVersion(kafkaConn.conn, req)
		if resp == nil {
			return nil, action
		}
		return resp.Bytes(apiVersion), action
	case codec.SaslHandshake:
		req, err := codec.DecodeSaslHandshakeReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, action := k.impl.SaslHandshake(kafkaConn.conn, req)
		if resp == nil {
			return nil, action
		}
		return resp.Bytes(apiVersion), action
	case codec.SaslAuthenticate:
		req, err := codec.DecodeSaslAuthenticateReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.SaslAuthenticate(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.Heartbeat:
		req, err := codec.DecodeHeartbeatReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.Heartbeat(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.JoinGroup:
		req, err := codec.DecodeJoinGroupReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, action := k.impl.JoinGroup(kafkaConn.conn, req)
		if resp == nil {
			return nil, action
		}
		return resp.Bytes(apiVersion), action
	case codec.SyncGroup:
		req, err := codec.DecodeSyncGroupReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.SyncGroup(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.OffsetFetch:
		req, err := codec.DecodeOffsetFetchReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.OffsetFetch(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.ListOffsets:
		req, err := codec.DecodeListOffsetsReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.ListOffsets(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.Fetch:
		req, err := codec.DecodeFetchReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.Fetch(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.OffsetCommit:
		req, err := codec.DecodeOffsetCommitReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, action := k.impl.OffsetCommit(kafkaConn.conn, req)
		if resp == nil {
			return nil, action
		}
		return resp.Bytes(apiVersion), action
	case codec.OffsetForLeaderEpoch:
		req, err := codec.DecodeOffsetForLeaderEpochReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.OffsetForLeaderEpoch(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.LeaveGroup:
		req, err := codec.DecodeLeaveGroupReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.LeaveGroup(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.Produce:
		req, err := codec.DecodeProduceReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.Produce(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		if req.RequiredAcks == 0 {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.Metadata:
		req, err := codec.DecodeMetadataReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.Metadata(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	case codec.FindCoordinator:
		req, err := codec.DecodeFindCoordinatorReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.FindCoordinator(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion), err
	}

	k.impl.UnSupportedApi(kafkaConn.conn, apiKey, apiVersion)
	return nil, fmt.Errorf("unsupported api key %d version %d", apiKey, apiVersion)
}

func NewKafkaNetServer(config KafkaNetServerConfig, impl KafkaNetServerImpl) (*KafkaNetServer, error) {
	listener, err := net.Listen("tcp", config.addr())
	if err != nil {
		return nil, err
	}
	p := &KafkaNetServer{}
	p.config = config
	if p.config.BufferMax == 0 {
		p.config.BufferMax = 5 * 1024 * 1024
	}
	p.listener = listener
	p.impl = impl
	go func() {
		p.run()
	}()
	return p, nil
}
