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
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
)

func TestNetDial(t *testing.T) {
	port, err := AcquireUnusedPort()
	require.Nil(t, err)
	server, err := NewKafkaNetServer(KafkaNetServerConfig{
		Host: "localhost",
		Port: port,
	}, nil)
	require.Nil(t, err)
	go func() {
		server.Run()
	}()
	address := fmt.Sprintf("localhost:%d", port)
	conn, err := net.DialTimeout("tcp", address, time.Second*5)
	require.Nil(t, err)
	conn.Close()
}
