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

package main

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kafka-codec-go/knet"
	"log"
	"os"
	"os/signal"
)

type ExampleKafkaNetServerImpl struct {
}

func (e ExampleKafkaNetServerImpl) ConnectionOpened(conn *knet.Conn) {
	log.Printf("ConnectionOpened: %s", conn.RemoteAddr())
}

func (e ExampleKafkaNetServerImpl) ConnectionClosed(conn *knet.Conn) {
	log.Printf("ConnectionClosed: %s", conn.RemoteAddr())
}

func (e ExampleKafkaNetServerImpl) AcceptError(conn *knet.Conn, err error) {
	log.Printf("AcceptError: %s, %s", conn.RemoteAddr(), err)
}

func (e ExampleKafkaNetServerImpl) ReadError(conn *knet.Conn, err error) {
	log.Printf("ReadError: %s, %s", conn.RemoteAddr(), err)
}

func (e ExampleKafkaNetServerImpl) ReactError(conn *knet.Conn, err error) {
	log.Printf("ReactError: %s, %s", conn.RemoteAddr(), err)
}

func (e ExampleKafkaNetServerImpl) WriteError(conn *knet.Conn, err error) {
	log.Printf("WriteError: %s, %s", conn.RemoteAddr(), err)
}

func (e ExampleKafkaNetServerImpl) UnSupportedApi(conn *knet.Conn, apiKey codec.ApiCode, apiVersion int16) {
	log.Printf("UnSupportedApi: %s, %v, %d", conn.RemoteAddr(), apiKey, apiVersion)
}

func (e ExampleKafkaNetServerImpl) ApiVersion(conn *knet.Conn, req *codec.ApiReq) (*codec.ApiResp, error) {
	resp := codec.ApiResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	resp.ErrorCode = 0
	apiRespVersions := make([]*codec.ApiRespVersion, 18)
	apiRespVersions[0] = &codec.ApiRespVersion{ApiKey: codec.Produce, MinVersion: 0, MaxVersion: 8}
	apiRespVersions[1] = &codec.ApiRespVersion{ApiKey: codec.Fetch, MinVersion: 0, MaxVersion: 10}
	apiRespVersions[2] = &codec.ApiRespVersion{ApiKey: codec.ListOffsets, MinVersion: 0, MaxVersion: 6}
	apiRespVersions[3] = &codec.ApiRespVersion{ApiKey: codec.Metadata, MinVersion: 0, MaxVersion: 9}
	apiRespVersions[4] = &codec.ApiRespVersion{ApiKey: codec.OffsetCommit, MinVersion: 0, MaxVersion: 8}
	apiRespVersions[5] = &codec.ApiRespVersion{ApiKey: codec.OffsetFetch, MinVersion: 0, MaxVersion: 7}
	apiRespVersions[6] = &codec.ApiRespVersion{ApiKey: codec.FindCoordinator, MinVersion: 0, MaxVersion: 3}
	apiRespVersions[7] = &codec.ApiRespVersion{ApiKey: codec.JoinGroup, MinVersion: 0, MaxVersion: 6}
	apiRespVersions[8] = &codec.ApiRespVersion{ApiKey: codec.Heartbeat, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[9] = &codec.ApiRespVersion{ApiKey: codec.LeaveGroup, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[10] = &codec.ApiRespVersion{ApiKey: codec.SyncGroup, MinVersion: 0, MaxVersion: 5}
	apiRespVersions[11] = &codec.ApiRespVersion{ApiKey: codec.DescribeGroups, MinVersion: 0, MaxVersion: 5}
	apiRespVersions[12] = &codec.ApiRespVersion{ApiKey: codec.ListGroups, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[13] = &codec.ApiRespVersion{ApiKey: codec.SaslHandshake, MinVersion: 0, MaxVersion: 1}
	apiRespVersions[14] = &codec.ApiRespVersion{ApiKey: codec.ApiVersions, MinVersion: 0, MaxVersion: 3}
	apiRespVersions[15] = &codec.ApiRespVersion{ApiKey: codec.DeleteRecords, MinVersion: 0, MaxVersion: 2}
	apiRespVersions[16] = &codec.ApiRespVersion{ApiKey: codec.OffsetForLeaderEpoch, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[17] = &codec.ApiRespVersion{ApiKey: codec.SaslAuthenticate, MinVersion: 0, MaxVersion: 2}
	resp.ApiRespVersions = apiRespVersions
	resp.ThrottleTime = 0
	return &resp, nil
}

func (e ExampleKafkaNetServerImpl) Fetch(conn *knet.Conn, req *codec.FetchReq) (*codec.FetchResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) FindCoordinator(conn *knet.Conn, req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) Heartbeat(conn *knet.Conn, req *codec.HeartbeatReq) (*codec.HeartbeatResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) JoinGroup(conn *knet.Conn, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) LeaveGroup(conn *knet.Conn, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) ListOffsets(conn *knet.Conn, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) Metadata(conn *knet.Conn, req *codec.MetadataReq) (*codec.MetadataResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) OffsetCommit(conn *knet.Conn, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) OffsetFetch(conn *knet.Conn, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) OffsetForLeaderEpoch(conn *knet.Conn, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) Produce(conn *knet.Conn, req *codec.ProduceReq) (*codec.ProduceResp, error) {
	//TODO implement me
	panic("implement me")
}

func (e ExampleKafkaNetServerImpl) SaslAuthenticate(conn *knet.Conn, req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, error) {
	saslAuthResp := &codec.SaslAuthenticateResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		ErrorCode:       0,
		ErrorMessage:    "",
		AuthBytes:       nil,
		SessionLifetime: 0,
	}
	return saslAuthResp, nil
}

func (e ExampleKafkaNetServerImpl) SaslHandshake(conn *knet.Conn, req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, error) {
	saslHandshakeResp := &codec.SaslHandshakeResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		ErrorCode:        0,
		EnableMechanisms: make([]*codec.EnableMechanism, 1),
	}
	saslHandshakeResp.EnableMechanisms[0] = &codec.EnableMechanism{SaslMechanism: "PLAIN"}
	return saslHandshakeResp, nil
}

func (e ExampleKafkaNetServerImpl) SyncGroup(conn *knet.Conn, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error) {
	//TODO implement me
	panic("implement me")
}

func main() {
	config := knet.KafkaNetServerConfig{
		Host:      "localhost",
		Port:      9092,
		BufferMax: 0,
	}
	server, err := knet.NewKafkaNetServer(config, &ExampleKafkaNetServerImpl{})
	if err != nil {
		panic(err)
	}
	server.Run()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		<-interrupt
	}
}
