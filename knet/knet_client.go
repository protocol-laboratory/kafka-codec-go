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
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"net"
	"sync"
)

type KafkaNetClientConfig struct {
	Host             string
	Port             int
	BufferMax        int
	SendQueueSize    int
	PendingQueueSize int
}

func (k KafkaNetClientConfig) addr() string {
	return fmt.Sprintf("%s:%d", k.Host, k.Port)
}

type sendRequest struct {
	bytes    []byte
	callback func([]byte, error)
}

type KafkaNetClient struct {
	conn         net.Conn
	eventsChan   chan *sendRequest
	pendingQueue chan *sendRequest
	buffer       *buffer
	closeCh      chan struct{}
}

func (k *KafkaNetClient) Produce(req *codec.ProduceReq) (*codec.ProduceResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeProduceResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) Fetch(req *codec.FetchReq) (*codec.FetchResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeFetchResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) ListOffsets(req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeListOffsetsResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) Metadata(req *codec.MetadataReq) (*codec.MetadataResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeMetadataResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) OffsetCommit(req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeOffsetCommitResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) OffsetFetch(req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeOffsetFetchResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) FindCoordinator(req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeFindCoordinatorResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) JoinGroup(req *codec.JoinGroupReq) (*codec.JoinGroupResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeJoinGroupResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) Heartbeat(req *codec.HeartbeatReq) (*codec.HeartbeatResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeHeartbeatResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) LeaveGroup(req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeLeaveGroupResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) SyncGroup(req *codec.SyncGroupReq) (*codec.SyncGroupResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeSyncGroupResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) SaslHandshake(req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeSaslHandshakeResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) ApiVersions(req *codec.ApiReq) (*codec.ApiResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeApiResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) OffsetForLeaderEpoch(req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeOffsetForLeaderEpochResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) SaslAuthenticate(req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, error) {
	bytes, err := k.Send(req.Bytes(true, true))
	if err != nil {
		return nil, err
	}
	resp, err := codec.DecodeSaslAuthenticateResp(bytes, req.ApiVersion)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (k *KafkaNetClient) Send(bytes []byte) ([]byte, error) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	var result []byte
	var err error
	k.sendAsync(bytes, func(resp []byte, e error) {
		result = resp
		err = e
		wg.Done()
	})
	wg.Wait()
	if err != nil {
		return nil, err
	}
	return result[4:], nil
}

func (k *KafkaNetClient) sendAsync(bytes []byte, callback func([]byte, error)) {
	sr := &sendRequest{
		bytes:    bytes,
		callback: callback,
	}
	k.eventsChan <- sr
}

func (k *KafkaNetClient) read() {
	for {
		select {
		case req := <-k.pendingQueue:
			n, err := k.conn.Read(k.buffer.bytes[k.buffer.cursor:])
			if err != nil {
				req.callback(nil, err)
				k.closeCh <- struct{}{}
				break
			}
			k.buffer.cursor += n
			if k.buffer.cursor < 4 {
				continue
			}
			length := int(k.buffer.bytes[3]) | int(k.buffer.bytes[2])<<8 | int(k.buffer.bytes[1])<<16 | int(k.buffer.bytes[0])<<24 + 4
			if k.buffer.cursor < length {
				continue
			}
			if length > k.buffer.max {
				req.callback(nil, fmt.Errorf("response length %d is too large", length))
				k.closeCh <- struct{}{}
				break
			}
			req.callback(k.buffer.bytes[:length], nil)
			k.buffer.cursor -= length
			copy(k.buffer.bytes[:k.buffer.cursor], k.buffer.bytes[length:])
		case <-k.closeCh:
			return
		}
	}
}

func (k *KafkaNetClient) write() {
	for {
		select {
		case req := <-k.eventsChan:
			n, err := k.conn.Write(req.bytes)
			if err != nil {
				req.callback(nil, err)
				k.closeCh <- struct{}{}
				break
			}
			if n != len(req.bytes) {
				req.callback(nil, fmt.Errorf("write %d bytes, but expect %d bytes", n, len(req.bytes)))
				k.closeCh <- struct{}{}
				break
			}
			k.pendingQueue <- req
		case <-k.closeCh:
			return
		}
	}
}

func (k *KafkaNetClient) Close() {
	_ = k.conn.Close()
	k.closeCh <- struct{}{}
}

func NewKafkaNetClient(config KafkaNetClientConfig) (*KafkaNetClient, error) {
	conn, err := net.Dial("tcp", config.addr())
	if err != nil {
		return nil, err
	}
	if config.SendQueueSize == 0 {
		config.SendQueueSize = 1000
	}
	if config.PendingQueueSize == 0 {
		config.PendingQueueSize = 1000
	}
	if config.BufferMax == 0 {
		config.BufferMax = 5 * 1024 * 1024
	}
	k := &KafkaNetClient{}
	k.conn = conn
	k.eventsChan = make(chan *sendRequest, config.SendQueueSize)
	k.pendingQueue = make(chan *sendRequest, config.PendingQueueSize)
	k.buffer = newBuffer(config.BufferMax)
	k.closeCh = make(chan struct{})
	go func() {
		k.read()
	}()
	go func() {
		k.write()
	}()
	return k, nil
}
