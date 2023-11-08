package aab

import (
	"Dory/internal/party"
	"Dory/pkg/protobuf"
	"fmt"
	"sync"
)

//var mm map[uint32]chan bool

func int2bytes(i uint32) []byte {
	b1 := byte(i)
	b2 := byte(i >> 8)
	b3 := byte(i >> 16)
	b4 := byte(i >> 24)

	return []byte{b4, b3, b2, b1}
}

func RBCMain(p *party.HonestParty, inputChannel chan []byte, outputChannel chan []byte, f int, pid int) {
	var idx uint32
	idx = 0
	mm := make(map[uint32]chan bool)
	mm[0] = make(chan bool, 1)
	mm[0] <- true

	for {
		idx++
		id := int2bytes(idx)
		//data := <-inputChannel
		//fmt.Println(pid, " get data: ", data)
		mm[idx] = make(chan bool, 1)
		// 接收最初的消息
		data, _ := p.GetMessageWithType("PROPOSE", []byte{0b11111111})

		if data.Data[0] != id[0] {
			fmt.Errorf("===========================!error!=======================================")
		}
		// 实现的主要逻辑（上面的并发并不是很重要）
		go rbc(p, data.Data, id, outputChannel, idx, mm)
	}
}

func rbc(p *party.HonestParty, data []byte, id []byte, outputChannel chan []byte, idx uint32, mm map[uint32]chan bool) {

	f := int(p.F)

	sendReady := false
	ready1 := make(map[string]int)
	ready2 := make(map[string]int)
	mu := new(sync.Mutex)

	var wg sync.WaitGroup
	wg.Add(2)

	// 第一轮 接收echo
	go func() {
		for {
			m, _ := p.GetMessageWithType("ECHO", id)
			//m, _ := p.GetMessage("ECHO",)
			mu.Lock()
			if !sendReady {
				if m != nil {
					key := string(m.Data)
					v, ok := ready1[key]
					if !ok {
						ready1[key] = 1
					} else {
						ready1[key] = v + 1
					}
					if ready1[key] >= 2*f+1 {
						p.Broadcast(&protobuf.Message{
							Data: m.Data,
							Type: "READY",
							Id:   id,
						})
						sendReady = true
						//fmt.Println(id, " sned READ1\t", string(m.Data))
						break
					}
				}
			} else {
				break
			}
			mu.Unlock()
		}
		mu.Unlock()
		wg.Done()
	}()

	// 接收ready并输出最终结果
	go func() {
		for {
			m, _ := p.GetMessageWithType("READY", id)
			mu.Lock()
			if m != nil {
				key := string(m.Data)
				v, ok := ready2[key]
				if !ok {
					ready2[key] = 1
				} else {
					ready2[key] = v + 1
				}
				if ready2[key] >= f+1 && !sendReady {
					p.Broadcast(&protobuf.Message{
						Data: m.Data,
						Type: "READY",
						Id:   id,
					})
					sendReady = true
					//fmt.Println(id, " sned READ2\t", string(m.Data))
					mu.Unlock()
					continue
				}

				if ready2[key] >= 2*f+1 {

					<-mm[idx-1]

					outputChannel <- m.Data

					mm[idx] <- true
					mu.Unlock()
					break
				}
			}
			mu.Unlock()
		}

		wg.Done()
	}()

	// 接收到数据v后，广播echo消息（有f个恶意节点）
	if p.PID > 2*p.F {
		// 敌手
		data[0] = data[0] + 1
		p.Broadcast(&protobuf.Message{
			Data: data,
			Type: "ECHO",
			Id:   id,
		})
	} else {
		p.Broadcast(&protobuf.Message{
			Data: data,
			Type: "ECHO",
			Id:   id,
		})
	}

	wg.Wait()
}
