package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/gpmgo/gopm/log"
	"github.com/hhq163/rabbitmq_channel_pool/impl"
	"github.com/streadway/amqp"
)

type PlayerData struct {
	Uid       int32
	UserName  string
	AgentName string
}

func main() {
	channelPool := new(impl.ChannelPool)
	channelPool.InitPool("amqp://test:testpassword@192.168.31.230:5672/test")

	userData := &PlayerData{
		Uid:       1,
		UserName:  "testUser",
		AgentName: "baoshan",
	}
	start := time.Now().UnixNano()

	sendCount := 0
	for i := 1; i < 10000; i++ {
		userData.Uid = i

		datas := new(bytes.Buffer)
		enc := gob.NewEncoder(datas)
		err := enc.Encode(userData)
		if err != nil {
			log.Error("Encode error")
			continue
		}
		msg := amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            datas.Bytes(),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		}

		errs := channelPool.Publish("TestExchange", "testKey", false, false, false, msg)
		if errs != nil {
			log.Error("Failed to publish a message grid:", tf.grid, "errinfo:", err.Error())
		}
		sendCount++
	}
	dur := time.Now().UnixNano() - start
	speed := 1000 / dur
	fmt.Println("Publish speed is", speed)

}
