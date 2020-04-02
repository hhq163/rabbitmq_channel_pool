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
	Uid        int32
	UserName   string
	AgentName  string
	agentCode  string
	nickName   string
	imageIndex uint8
	rank       uint8
	hallType   uint8
	loginType  uint8
	ipInfo     string
}

func main() {
	base.LogInit("Info", 1000)
	base.BreakerInit()
	cpBreaker := base.GetBreaker(1000)

	channelPool := new(impl.ChannelPool)
	channelPool.InitPool("amqp://admin:2626hhq@192.168.1.29:5672/")

	userData := &PlayerData{
		Uid:        1,
		UserName:   "testUsertestUsertestUsertestUsertestUsertestUsertestUsertest",
		AgentName:  "baoshantestUsertestUsertestUsertestUsertestUserbaoshantestUsertestUsertestUsertestUsertestUser",
		agentCode:  "",
		nickName:   "testUser",
		imageIndex: 16,
		rank:       2,
		hallType:   3,
		loginType:  3,
		ipInfo:     "192.168.0.32",
	}

	util.StartWorks(10, 10000)

	for i := 1; i < 1000000; i++ {
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
			// base.Log.Info("datas.len=", datas.Len())
			msg := amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				Body:            datas.Bytes(),
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
			}
			if cpBreaker.IsAllowed() { //是否被熔断
				errs := channelPool.Publish("LogicToWork", "testKey", false, false, false, msg)
				if errs != nil {
					cpBreaker.Fail()
					base.Log.Error("Failed to publish a message i:", i, "errinfo:", err.Error())
				} else {
					cpBreaker.Succeed()
				}
			}

		})

	}

	fmt.Println("Publish finished")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	sig := <-c
	base.Log.Info("Process closing down signal:", sig)

	util.StopWorkers()

}
