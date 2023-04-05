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
	apiVersions, err := cli.ApiVersions(&codec.ApiReq{
		BaseReq: codec.BaseReq{
			ApiVersion:    0,
			CorrelationId: 1,
			ClientId:      "1",
		},
		ClientSoftwareName:    "kafka-codec-go",
		ClientSoftwareVersion: "0.0.1",
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("api version response: %+v\n", apiVersions)
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
	fmt.Printf("handshake response: %+v\n", handshakeResp)
}
