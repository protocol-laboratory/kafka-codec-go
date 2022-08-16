package kgnet

import (
	"encoding/binary"
	"fmt"
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"sync"
	"time"
)

type GnetClientConfig struct {
	Host             string
	Port             int
	SendQueueSize    int
	PendingQueueSize int
}

func (g GnetClientConfig) addr() string {
	return fmt.Sprintf("%s:%d", g.Host, g.Port)
}

type KafkaLowLevelClient struct {
	kafkaGnetClient *KafkaGnetClient
}

func (k *KafkaLowLevelClient) ApiVersions(req *codec.ApiReq) (*codec.ApiResp, error) {
	bytes, err := k.kafkaGnetClient.Send(req.Bytes())
	if err != nil {
		return nil, err
	}
	apiResp, err := codec.DecodeApiResp(bytes, 3)
	if err != nil {
		return nil, err
	}
	return apiResp, nil
}

func (k *KafkaLowLevelClient) Close() {
	k.kafkaGnetClient.Close()
}

type KafkaGnetClient struct {
	networkClient *gnet.Client
	conn          gnet.Conn
	eventsChan    chan *sendRequest
	pendingQueue  chan *sendRequest
	closeCh       chan struct{}
}

type sendRequest struct {
	bytes    []byte
	callback func([]byte, error)
}

func (k *KafkaGnetClient) run() {
	for {
		select {
		case req := <-k.eventsChan:
			err := k.conn.AsyncWrite(req.bytes)
			if err != nil {
				req.callback(nil, err)
			}
			k.pendingQueue <- req
		case <-k.closeCh:
			return
		}
	}
}

func (k *KafkaGnetClient) Send(bytes []byte) ([]byte, error) {
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
	return result, err
}

func (k *KafkaGnetClient) sendAsync(bytes []byte, callback func([]byte, error)) {
	sr := &sendRequest{
		bytes:    bytes,
		callback: callback,
	}
	k.eventsChan <- sr
}

func (k *KafkaGnetClient) OnInitComplete(server gnet.Server) (action gnet.Action) {
	return gnet.None
}

func (k *KafkaGnetClient) OnShutdown(server gnet.Server) {
}

func (k *KafkaGnetClient) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	return
}

func (k *KafkaGnetClient) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	return
}

func (k *KafkaGnetClient) PreWrite(c gnet.Conn) {
}

func (k *KafkaGnetClient) AfterWrite(c gnet.Conn, b []byte) {
}

func (k *KafkaGnetClient) React(packet []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	request := <-k.pendingQueue
	request.callback(packet, nil)
	return nil, gnet.None
}

func (k *KafkaGnetClient) Tick() (delay time.Duration, action gnet.Action) {
	return 15 * time.Second, gnet.None
}

func (k *KafkaGnetClient) Close() {
	_ = k.networkClient.Stop()
	k.closeCh <- struct{}{}
}

func newKafkaGnetClient(config GnetClientConfig) (*KafkaGnetClient, error) {
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
	k := &KafkaGnetClient{}
	var err error
	k.networkClient, err = gnet.NewClient(k, gnet.WithCodec(kfkCodec))
	if err != nil {
		return nil, err
	}
	err = k.networkClient.Start()
	if err != nil {
		return nil, err
	}
	k.conn, err = k.networkClient.Dial("tcp", config.addr())
	if err != nil {
		return nil, err
	}
	if config.SendQueueSize == 0 {
		config.SendQueueSize = 1000
	}
	if config.PendingQueueSize == 0 {
		config.PendingQueueSize = 1000
	}
	k.eventsChan = make(chan *sendRequest, config.SendQueueSize)
	k.closeCh = make(chan struct{})
	k.pendingQueue = make(chan *sendRequest, config.PendingQueueSize)
	go func() {
		k.run()
	}()
	return k, nil
}

func NewKafkaLowLevelClient(config GnetClientConfig) (*KafkaLowLevelClient, error) {
	k := &KafkaLowLevelClient{}
	var err error
	k.kafkaGnetClient, err = newKafkaGnetClient(config)
	if err != nil {
		return nil, err
	}
	return k, nil
}