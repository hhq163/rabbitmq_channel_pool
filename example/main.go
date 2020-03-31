package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/hhq163/rabbitmq_channel_pool/base"
	"github.com/hhq163/rabbitmq_channel_pool/impl"
	"github.com/streadway/amqp"
)

type PlayerData struct {
	Uid       int32
	UserName  string
	AgentName string
}

func main() {
	base.LogInit("Info", 1000)

	channelPool := new(impl.ChannelPool)
	channelPool.InitPool("amqp://admin:2626hhq@192.168.1.29:5672/test")

	userData := &PlayerData{
		Uid:       1,
		UserName:  "testUser",
		AgentName: "baoshan",
	}
	start := time.Now().UnixNano()

	sendCount := 0
	for i := 1; i < 10000; i++ {
		userData.Uid = int32(i)

		datas := new(bytes.Buffer)
		enc := gob.NewEncoder(datas)
		err := enc.Encode(userData)
		if err != nil {
			base.Log.Fatal("Encode error")
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

		errs := channelPool.Publish("test", "testKey", false, false, false, msg)
		if errs != nil {
			base.Log.Error("Failed to publish a message i:", i, "errinfo:", errs.Error())
		}
		sendCount++
	}
	dur := time.Now().UnixNano() - start
	speed := 1000 / dur
	fmt.Println("Publish speed is", speed)

}
