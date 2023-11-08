package aab

import (
	"Dory/internal/party"
	"Dory/pkg/protobuf"
	"bytes"
	"sync"
	"testing"
)

func TestMainProgress(t *testing.T) {
	ipList := []string{"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1",
		"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1",
		"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1",
		"127.0.0.1"}
	portList := []string{"8880", "8881", "8882", "8883", "8884", "8885", "8886", "8887", "8888", "8889",
		"8870", "8871", "8872", "8873", "8874", "8875", "8876", "8877", "8878", "8879",
		"8860", "8861", "8862", "8863", "8864", "8865", "8866", "8867", "8868", "8869", "8859"}

	N := uint32(31)
	F := uint32(10)
	sk, pk := party.SigKeyGen(N, 2*F+1)

	var p []*party.HonestParty = make([]*party.HonestParty, N)
	for i := uint32(0); i < N; i++ {
		p[i] = party.NewHonestParty(N, F, i, ipList, portList, pk, sk[i])
	}

	for i := uint32(0); i < N; i++ {
		p[i].InitReceiveChannel()
	}

	for i := uint32(0); i < N; i++ {
		p[i].InitSendChannel()
	}

	testEpochs := 1024
	var wg sync.WaitGroup
	wg.Add(int(N))
	result := make([][]byte, N)

	// 起始节点(即发送最初信息的节点)
	go func() {
		for e := 1; e <= testEpochs; e++ {
			data := make([]byte, 1)
			data[0] = byte(e)
			// inputChannel <- data
			p[0].Broadcast(&protobuf.Message{
				Data: data,
				Type: "PROPOSE",
				Id:   []byte{0b11111111},
			})
		}
	}()
	// 启动每一个节点
	for i := uint32(0); i < N; i++ {
		go func(i uint32) {
			//inputChannel := make(chan []byte, MAXMESSAGE)
			outputChannel := make(chan []byte, MAXMESSAGE)

			//go MainProgress(p[i], inputChannel, outputChannel)
			//实现的主要逻辑
			go RBCMain(p[i], nil, outputChannel, int(F), int(i))

			for e := 1; e <= testEpochs; e++ {
				value := <-outputChannel
				result[i] = append(result[i], value...)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	for i := uint32(1); i < N; i++ {
		if !bytes.Equal(result[i], result[i-1]) {
			t.Error()
		}
	}
}
