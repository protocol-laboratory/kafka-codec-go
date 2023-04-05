package main

import (
	"fmt"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kafka-codec-go/knet"
)

func main() {
	cli, err := knet.NewKafkaNetClient(knet.KafkaNetClientConfig{
		Host: "localhost",
		Port: 9092,
	})
	if err != nil {
		panic(err)
	}
	handshakeResp, err := cli.SaslHandshake(&codec.SaslHandshakeReq{
		BaseReq: codec.BaseReq{
			ApiVersion:    1,
			CorrelationId: 1,
			ClientId:      "1",
		},
		SaslMechanism: "PLAIN",
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", handshakeResp)
}
