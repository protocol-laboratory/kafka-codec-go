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

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeJoinGroupRespV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001000000000003000572616e676500925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d32613266656536393839633800925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d3261326665653639383963380000000100925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d326132666565363938396338000000110001000000010005746f706963ffffffff")
	resp, err := DecodeJoinGroupResp(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 1)
	assert.Equal(t, resp.GenerationId, 3)
	assert.Equal(t, resp.MemberId, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8")
	assert.Equal(t, resp.LeaderId, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8")
	assert.Equal(t, resp.ProtocolName, "range")
	members := resp.Members
	assert.Equal(t, len(members), 1)
	member := members[0]
	assert.Equal(t, member.MemberId, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8")
	assert.Equal(t, member.Metadata, testHex2Bytes(t, "0001000000010005746f706963ffffffff"))
}

func TestEncodeJoinGroupRespV1(t *testing.T) {
	member := &Member{}
	member.MemberId = "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8"
	member.Metadata = testHex2Bytes(t, "0001000000010005746f706963ffffffff")
	joinGroupResp := JoinGroupResp{
		BaseResp: BaseResp{
			CorrelationId: 1,
		},
	}
	joinGroupResp.GenerationId = 3
	joinGroupResp.ProtocolName = "range"
	joinGroupResp.LeaderId = "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8"
	joinGroupResp.MemberId = "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8"
	joinGroupResp.Members = []*Member{member}
	bytes := joinGroupResp.Bytes(1)
	expectBytes := testHex2Bytes(t, "00000001000000000003000572616e676500925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d32613266656536393839633800925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d3261326665653639383963380000000100925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d326132666565363938396338000000110001000000010005746f706963ffffffff")
	assert.Equal(t, expectBytes[400:], bytes[400:])
}

func TestDecodeAndCodeJoinGroupRespV1(t *testing.T) {
	bytes := testHex2Bytes(t, "00000001000000000003000572616e676500925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d32613266656536393839633800925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d3261326665653639383963380000000100925f5f5f546573744b61666b61436f6e73756d655f696e5f676f5f64656d6f5f64656d6f5f6b61666b612e746573744068657a68616e676a69616e64654d6163426f6f6b2d50726f2e6c6f63616c20286769746875622e636f6d2f7365676d656e74696f2f6b61666b612d676f292d61336635303632622d393462632d343738642d386464622d326132666565363938396338000000110001000000010005746f706963ffffffff")
	resp, err := DecodeJoinGroupResp(bytes, 1)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 1)
	assert.Equal(t, resp.GenerationId, 3)
	assert.Equal(t, resp.MemberId, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8")
	assert.Equal(t, resp.LeaderId, "___TestKafkaConsume_in_go_demo_demo_kafka.test@hezhangjiandeMacBook-Pro.local (github.com/segmentio/kafka-go)-a3f5062b-94bc-478d-8ddb-2a2fee6989c8")
	codeBytes := resp.Bytes(1)
	assert.Equal(t, bytes, codeBytes)
}

func TestDecodeJoinGroupRespV6(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000500000000000000000000010672616e676555636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d36613234396366663037666355636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d3661323439636666303766630255636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d36613234396366663037666300170001000000010006746573742d35ffffffff000000000000")
	resp, err := DecodeJoinGroupResp(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 5)
	assert.Equal(t, resp.GenerationId, 1)
	assert.Equal(t, resp.MemberId, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc")
	assert.Equal(t, resp.LeaderId, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc")
	assert.Equal(t, resp.ProtocolName, "range")
	members := resp.Members
	assert.Equal(t, len(members), 1)
	member := members[0]
	assert.Equal(t, member.MemberId, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc")
	assert.Equal(t, member.Metadata, testHex2Bytes(t, "0001000000010006746573742d35ffffffff00000000"))
}

func TestEncodeJoinGroupRespV6(t *testing.T) {
	member := &Member{}
	member.MemberId = "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc"
	member.Metadata = testHex2Bytes(t, "0001000000010006746573742d35ffffffff00000000")
	joinGroupResp := JoinGroupResp{
		BaseResp: BaseResp{
			CorrelationId: 5,
		},
	}
	joinGroupResp.GenerationId = 1
	joinGroupResp.ProtocolName = "range"
	joinGroupResp.LeaderId = "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc"
	joinGroupResp.MemberId = "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc"
	joinGroupResp.Members = []*Member{member}
	bytes := joinGroupResp.Bytes(6)
	expectBytes := testHex2Bytes(t, "0000000500000000000000000000010672616e676555636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d36613234396366663037666355636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d3661323439636666303766630255636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d36613234396366663037666300170001000000010006746573742d35ffffffff000000000000")
	assert.Equal(t, expectBytes, bytes)
}

func TestDecodeAndCodeJoinGroupRespV6(t *testing.T) {
	bytes := testHex2Bytes(t, "0000000500000000000000000000010672616e676555636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d36613234396366663037666355636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d3661323439636666303766630255636f6e73756d65722d38646437623936622d366239342d346139622d623263632d3363623538393863396364662d312d34333361636236612d653665632d343561612d623738642d36613234396366663037666300170001000000010006746573742d35ffffffff000000000000")
	resp, err := DecodeJoinGroupResp(bytes, 6)
	assert.Nil(t, err)
	assert.Equal(t, resp.CorrelationId, 5)
	assert.Equal(t, resp.GenerationId, 1)
	assert.Equal(t, resp.MemberId, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc")
	assert.Equal(t, resp.LeaderId, "consumer-8dd7b96b-6b94-4a9b-b2cc-3cb5898c9cdf-1-433acb6a-e6ec-45aa-b78d-6a249cff07fc")
	assert.Equal(t, resp.ProtocolName, "range")
	codeBytes := resp.Bytes(6)
	assert.Equal(t, bytes, codeBytes)
}
