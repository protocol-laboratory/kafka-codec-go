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
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"net"
	"os"
	"runtime/debug"
	"sync"
)

type KafkaNetServerConfig struct {
	Host      string
	Port      int
	BufferMax int
	tlsEnable bool
	tlsConfig *tls.Config
}

type KafkaNetServerImpl interface {
	ConnectionOpened(conn *Conn)
	ConnectionClosed(conn *Conn)

	AcceptError(conn *Conn, err error)
	ReadError(conn *Conn, err error)
	ReactError(conn *Conn, err error)
	WriteError(conn *Conn, err error)

	UnSupportedApi(conn *Conn, apiKey codec.ApiCode, apiVersion int16)
	ApiVersion(conn *Conn, req *codec.ApiReq) (*codec.ApiResp, error)
	Fetch(conn *Conn, req *codec.FetchReq) (*codec.FetchResp, error)
	FindCoordinator(conn *Conn, req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, error)
	Heartbeat(conn *Conn, req *codec.HeartbeatReq) (*codec.HeartbeatResp, error)
	JoinGroup(conn *Conn, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error)
	LeaveGroup(conn *Conn, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error)
	ListOffsets(conn *Conn, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, error)
	Metadata(conn *Conn, req *codec.MetadataReq) (*codec.MetadataResp, error)
	OffsetCommit(conn *Conn, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, error)
	OffsetFetch(conn *Conn, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, error)
	OffsetForLeaderEpoch(conn *Conn, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, error)
	Produce(conn *Conn, req *codec.ProduceReq) (*codec.ProduceResp, error)
	SaslAuthenticate(conn *Conn, req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, error)
	SaslHandshake(conn *Conn, req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, error)
	SyncGroup(conn *Conn, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error)
}

func (k *KafkaNetServerConfig) addr() string {
	return fmt.Sprintf("%s:%d", k.Host, k.Port)
}

type KafkaNetServer struct {
	listener net.Listener
	impl     KafkaNetServerImpl
	config   KafkaNetServerConfig
	quit     chan bool
	connWg   sync.WaitGroup
}

func (k *KafkaNetServer) Run() {
	defer func() {
		if err := recover(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "network server failed: %s", codec.PanicToError(err, debug.Stack()))
		}
	}()
	for {
		netConn, err := k.listener.Accept()
		if err != nil {
			select {
			case <-k.quit:
				return
			default:
				k.impl.AcceptError(&Conn{Conn: netConn}, err)
				continue
			}
		}
		k.connWg.Add(1)
		k.impl.ConnectionOpened(&Conn{Conn: netConn})
		go func() {
			var conn = &Conn{
				Conn: netConn,
			}
			defer func() {
				if err := recover(); err != nil {
					k.impl.ReactError(conn, codec.PanicToError(err, debug.Stack()))
				}
			}()
			k.HandleConn(&kafkaConn{
				conn: conn,
				buffer: &buffer{
					max:    k.config.BufferMax,
					bytes:  make([]byte, k.config.BufferMax),
					cursor: 0,
				},
			})
			k.connWg.Done()
		}()
	}
}

type kafkaConn struct {
	conn   *Conn
	buffer *buffer
}

func (k *KafkaNetServer) HandleConn(kafkaConn *kafkaConn) {
	activeClose := false
	for {
		if activeClose {
			break
		}
		// read data into buffer
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

		// process messages in buffer
		for {
			if kafkaConn.buffer.cursor < 4 {
				break
			}
			bodyLen := codec.FourByteLength(kafkaConn.buffer.bytes)
			packetLen := bodyLen + 4
			if kafkaConn.buffer.cursor < packetLen {
				break
			}
			if packetLen > kafkaConn.buffer.max {
				k.impl.ReadError(kafkaConn.conn, fmt.Errorf("message too long: %d", packetLen))
				activeClose = true
				k.activeCloseConn(kafkaConn)
				break
			}
			dstBytes, err := k.react(kafkaConn, kafkaConn.buffer.bytes[4:packetLen])
			if err != nil {
				k.impl.ReactError(kafkaConn.conn, err)
				activeClose = true
				k.activeCloseConn(kafkaConn)
				break
			}
			write, err := kafkaConn.conn.Write(dstBytes)
			if err != nil {
				k.impl.WriteError(kafkaConn.conn, err)
				activeClose = true
				k.activeCloseConn(kafkaConn)
				break
			}
			if write != len(dstBytes) {
				k.impl.WriteError(kafkaConn.conn, fmt.Errorf("write %d bytes, but expect %d bytes", write, len(dstBytes)))
				activeClose = true
				k.activeCloseConn(kafkaConn)
				break
			}
			kafkaConn.buffer.cursor -= packetLen
			copy(kafkaConn.buffer.bytes[:kafkaConn.buffer.cursor], kafkaConn.buffer.bytes[packetLen:])
		}

		// close connection if buffer is full
		if kafkaConn.buffer.cursor == kafkaConn.buffer.max {
			k.impl.ReadError(kafkaConn.conn, fmt.Errorf("buffer full"))
			activeClose = true
			k.activeCloseConn(kafkaConn)
			break
		}
	}
}

func (k *KafkaNetServer) activeCloseConn(conn *kafkaConn) {
	_ = conn.conn.Close()
	k.impl.ConnectionClosed(conn.conn)
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
		return resp.Bytes(apiVersion, true), action
	case codec.SaslHandshake:
		req, err := codec.DecodeSaslHandshakeReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, action := k.impl.SaslHandshake(kafkaConn.conn, req)
		if resp == nil {
			return nil, action
		}
		return resp.Bytes(apiVersion, true), action
	case codec.SaslAuthenticate:
		req, err := codec.DecodeSaslAuthenticateReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.SaslAuthenticate(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
	case codec.Heartbeat:
		req, err := codec.DecodeHeartbeatReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.Heartbeat(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
	case codec.JoinGroup:
		req, err := codec.DecodeJoinGroupReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, action := k.impl.JoinGroup(kafkaConn.conn, req)
		if resp == nil {
			return nil, action
		}
		return resp.Bytes(apiVersion, true), action
	case codec.SyncGroup:
		req, err := codec.DecodeSyncGroupReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.SyncGroup(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
	case codec.OffsetFetch:
		req, err := codec.DecodeOffsetFetchReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.OffsetFetch(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
	case codec.ListOffsets:
		req, err := codec.DecodeListOffsetsReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.ListOffsets(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
	case codec.Fetch:
		req, err := codec.DecodeFetchReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.Fetch(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
	case codec.OffsetCommit:
		req, err := codec.DecodeOffsetCommitReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, action := k.impl.OffsetCommit(kafkaConn.conn, req)
		if resp == nil {
			return nil, action
		}
		return resp.Bytes(apiVersion, true), action
	case codec.OffsetForLeaderEpoch:
		req, err := codec.DecodeOffsetForLeaderEpochReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.OffsetForLeaderEpoch(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
	case codec.LeaveGroup:
		req, err := codec.DecodeLeaveGroupReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.LeaveGroup(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
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
		return resp.Bytes(apiVersion, true), err
	case codec.Metadata:
		req, err := codec.DecodeMetadataReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.Metadata(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
	case codec.FindCoordinator:
		req, err := codec.DecodeFindCoordinatorReq(bytes[4:], apiVersion)
		if err != nil {
			return nil, err
		}
		resp, err := k.impl.FindCoordinator(kafkaConn.conn, req)
		if resp == nil {
			return nil, err
		}
		return resp.Bytes(apiVersion, true), err
	}

	k.impl.UnSupportedApi(kafkaConn.conn, apiKey, apiVersion)
	return nil, fmt.Errorf("unsupported api key %d version %d", apiKey, apiVersion)
}

// Stop graceful stop server
func (k *KafkaNetServer) Stop() error {
	close(k.quit)
	err := k.listener.Close()
	k.connWg.Wait()
	return err
}

func NewKafkaNetServer(config KafkaNetServerConfig, impl KafkaNetServerImpl) (*KafkaNetServer, error) {
	listener, err := net.Listen("tcp", config.addr())
	if err != nil {
		return nil, err
	}
	if config.tlsEnable {
		listener = tls.NewListener(listener, config.tlsConfig)
	}
	k := &KafkaNetServer{
		listener: listener,
		impl:     impl,
		quit:     make(chan bool),
	}
	k.config = config
	if k.config.BufferMax == 0 {
		k.config.BufferMax = 5 * 1024 * 1024
	}
	// server thread task
	go k.Run()
	return k, nil
}
