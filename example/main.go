package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/url"
	"os"
	"os/signal"

	"github.com/hhq163/kk_core/base"
	"github.com/hhq163/kk_core/util"
	"github.com/hhq163/logger"
	"github.com/hhq163/rabbitmq_channel_pool"
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

var Log logger.Logger

var Workers *util.OrderWorkers

func main() {

	cfg := logger.NewDevelopmentConfig()
	cfg.Filename = "./logs/access_log.txt"
	cfg.MaxSize = 500 //单位为M
	Log = logger.NewCuttingLogger(cfg)

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

	Workers = util.NewOrderWorkers(8, 200, &Log)

	user := url.QueryEscape("gouser")
	password := url.QueryEscape("DEIro34KE@#$")
	urlStr := fmt.Sprintf("amqp://%s:%s@172.18.2.85:5672/Y", user, password)
	Log.Debug("urlStr=", urlStr)

	mqcfg := &rabbitmq_channel_pool.MqConfig{
		AmqpUrl:    urlStr,
		ChannelNum: 10,
	}

	mqClient, err := rabbitmq_channel_pool.NewMQClient(mqcfg, nil, &Log)
	if err != nil {
		Log.Debug("err=", err.Error())
	}

	// now := time.Now().Unix()
	// ticker := time.NewTicker(1 * time.Second)
	// for {
	// 	<-ticker.C

	// 	for i := 0; i < 1000; i++ {
	// 		t.Log("SendOneMsg i=", i)
	// 		SendOneMsg(CONSUME_EXCHANGE, "hequnKey", now+int64(i))
	// 	}

	// }

	for i := 1; i < 1000000; i++ {
		index := i
		util.Push(1, func() {
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
			errs := mqClient.Publish("LogicToWork", "testKey", false, false, false, msg)
			if errs != nil {
				base.Log.Error("Failed to publish a message i:", i, "errinfo:", err.Error())
			}

			base.Log.Info("Publish success")

		})

	}

	fmt.Println("Publish finished")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	sig := <-c
	base.Log.Info("Process closing down signal:", sig)

}
