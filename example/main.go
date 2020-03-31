package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"os/signal"

	"github.com/hhq163/rabbitmq_channel_pool/base"
	"github.com/hhq163/rabbitmq_channel_pool/impl"
	"github.com/hhq163/rabbitmq_channel_pool/util"
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
	channelPool.InitPool("amqp://admin:2626hhq@192.168.1.29:5672/")

	userData := &PlayerData{
		Uid:       1,
		UserName:  "testUser",
		AgentName: "baoshan",
	}

	util.StartWorks(10, 10000)

	sendCount := 0
	for i := 1; i < 100000; i++ {
		index := i
		util.PushJob(func() {
			userData.Uid = int32(index)

			datas := new(bytes.Buffer)
			enc := gob.NewEncoder(datas)
			err := enc.Encode(userData)
			if err != nil {
				base.Log.Fatal("Encode error")
				return
			}
			msg := amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				Body:            datas.Bytes(),
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
			}

			errs := channelPool.Publish("LogicToWork", "testKey", false, false, false, msg)
			if errs != nil {
				base.Log.Error("Failed to publish a message i:", i, "errinfo:", err.Error())
			}
		})

		sendCount++
	}

	fmt.Println("Publish finished")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	sig := <-c
	base.Log.Info("Process closing down signal:", sig)

	util.StopWorkers()

}
