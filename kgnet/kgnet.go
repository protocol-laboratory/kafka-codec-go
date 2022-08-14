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

package kgnet

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"runtime/debug"
)

type GnetConfig struct {
	ListenHost   string
	ListenPort   uint16
	EventLoopNum int
}

type KafkaImpl interface {
	OnInitComplete(server gnet.Server) (action gnet.Action)
	OnOpened(c gnet.Conn) (out []byte, action gnet.Action)
	OnClosed(c gnet.Conn, err error) (action gnet.Action)
	InvalidKafkaPacket(c gnet.Conn)
	ConnError(c gnet.Conn, r any, stack []byte)
	UnSupportedApi(c gnet.Conn, apiKey codec.ApiCode, apiVersion int16)
	ApiVersion(c gnet.Conn, req *codec.ApiReq) (*codec.ApiResp, gnet.Action)
	Fetch(c gnet.Conn, req *codec.FetchReq) (*codec.FetchResp, gnet.Action)
	FindCoordinator(c gnet.Conn, req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, gnet.Action)
	Heartbeat(c gnet.Conn, req *codec.HeartbeatReq) (*codec.HeartbeatResp, gnet.Action)
	JoinGroup(c gnet.Conn, req *codec.JoinGroupReq) (*codec.JoinGroupResp, gnet.Action)
	LeaveGroup(c gnet.Conn, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, gnet.Action)
	ListOffsets(c gnet.Conn, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, gnet.Action)
	Metadata(c gnet.Conn, req *codec.MetadataReq) (*codec.MetadataResp, gnet.Action)
	OffsetCommit(c gnet.Conn, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, gnet.Action)
	OffsetFetch(c gnet.Conn, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, gnet.Action)
	OffsetForLeaderEpoch(c gnet.Conn, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, gnet.Action)
	Produce(c gnet.Conn, req *codec.ProduceReq) (*codec.ProduceResp, gnet.Action)
	SaslAuthenticate(c gnet.Conn, req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, gnet.Action)
	SaslHandshake(c gnet.Conn, req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, gnet.Action)
	SyncGroup(c gnet.Conn, req *codec.SyncGroupReq) (*codec.SyncGroupResp, gnet.Action)
}

type KafkaServer struct {
	GnetConfig GnetConfig
	impl       KafkaImpl
	*gnet.EventServer
}

func (k *KafkaServer) OnInitComplete(server gnet.Server) (action gnet.Action) {
	k.impl.OnInitComplete(server)
	return
}

func (k *KafkaServer) React(frame []byte, c gnet.Conn) (_ []byte, action gnet.Action) {
	defer func() {
		if r := recover(); r != nil {
			k.impl.ConnError(c, r, debug.Stack())
			action = gnet.Close
		}
	}()
	if len(frame) < 5 {
		k.impl.InvalidKafkaPacket(c)
		return nil, gnet.Close
	}
	apiKey := codec.ApiCode(binary.BigEndian.Uint16(frame))
	apiVersion := int16(binary.BigEndian.Uint16(frame[2:]))

	switch apiKey {
	case codec.ApiVersions:
		req, r, stack := codec.DecodeApiReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.ApiVersion(c, req)
		return resp.Bytes(apiVersion), action
	case codec.SaslHandshake:
		req, r, stack := codec.DecodeSaslHandshakeReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.SaslHandshake(c, req)
		return resp.Bytes(apiVersion), action
	case codec.SaslAuthenticate:
		req, r, stack := codec.DecodeSaslAuthenticateReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.SaslAuthenticate(c, req)
		return resp.Bytes(apiVersion), action
	case codec.Heartbeat:
		req, r, stack := codec.DecodeHeartbeatReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.Heartbeat(c, req)
		return resp.Bytes(apiVersion), action
	case codec.JoinGroup:
		req, r, stack := codec.DecodeJoinGroupReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.JoinGroup(c, req)
		return resp.Bytes(apiVersion), action
	case codec.SyncGroup:
		req, r, stack := codec.DecodeSyncGroupReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.SyncGroup(c, req)
		return resp.Bytes(apiVersion), action
	case codec.OffsetFetch:
		req, r, stack := codec.DecodeOffsetFetchReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.OffsetFetch(c, req)
		return resp.Bytes(apiVersion), action
	case codec.ListOffsets:
		req, r, stack := codec.DecodeListOffsetsReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.ListOffsets(c, req)
		return resp.Bytes(apiVersion), action
	case codec.Fetch:
		req, r, stack := codec.DecodeFetchReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.Fetch(c, req)
		return resp.Bytes(apiVersion), action
	case codec.OffsetCommit:
		req, r, stack := codec.DecodeOffsetCommitReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.OffsetCommit(c, req)
		return resp.Bytes(apiVersion), action
	case codec.OffsetForLeaderEpoch:
		req, r, stack := codec.DecodeOffsetForLeaderEpochReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.OffsetForLeaderEpoch(c, req)
		return resp.Bytes(apiVersion), action
	case codec.LeaveGroup:
		req, r, stack := codec.DecodeLeaveGroupReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.LeaveGroup(c, req)
		return resp.Bytes(apiVersion), action
	case codec.Produce:
		req, r, stack := codec.DecodeProduceReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.Produce(c, req)
		return resp.Bytes(apiVersion), action
	case codec.Metadata:
		req, r, stack := codec.DecodeMetadataReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.Metadata(c, req)
		return resp.Bytes(apiVersion), action
	case codec.FindCoordinator:
		req, r, stack := codec.DecodeFindCoordinatorReq(frame[4:], apiVersion)
		if r != nil {
			k.impl.ConnError(c, r, stack)
			return nil, gnet.Close
		}
		resp, action := k.impl.FindCoordinator(c, req)
		return resp.Bytes(apiVersion), action
	}

	k.impl.UnSupportedApi(c, apiKey, apiVersion)
	return nil, gnet.Close
}

func (k *KafkaServer) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	k.impl.OnOpened(c)
	return
}

func (k *KafkaServer) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	k.impl.OnClosed(c, err)
	return
}

func (k *KafkaServer) Run() error {
	encoderConfig := gnet.EncoderConfig{
		ByteOrder:                       binary.BigEndian,
		LengthFieldLength:               4,
		LengthAdjustment:                0,
		LengthIncludesLengthFieldLength: false,
	}
	decoderConfig := gnet.DecoderConfig{
		ByteOrder:           binary.BigEndian,
		LengthFieldOffset:   0,
		LengthFieldLength:   4,
		LengthAdjustment:    0,
		InitialBytesToStrip: 4,
	}
	kfkCodec := gnet.NewLengthFieldBasedFrameCodec(encoderConfig, decoderConfig)
	return gnet.Serve(k, fmt.Sprintf("tcp://%s:%d", k.GnetConfig.ListenHost, k.GnetConfig.ListenPort), gnet.WithNumEventLoop(k.GnetConfig.EventLoopNum), gnet.WithCodec(kfkCodec))
}

func (k *KafkaServer) Stop(ctx context.Context) error {
	addr := fmt.Sprintf("tcp://%s:%d", k.GnetConfig.ListenHost, k.GnetConfig.ListenPort)
	return gnet.Stop(context.Background(), addr)
}

func NewKafkaServer(gnetConfig GnetConfig, impl KafkaImpl) *KafkaServer {
	return &KafkaServer{
		GnetConfig: gnetConfig,
		impl:       impl,
	}
}
