package rabbitmq_channel_pool

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hhq163/kk_core/base"
	"github.com/hhq163/logger"
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

const (
	CONSUME_EXCHANGE = "testExchange"
)

var ActReqCountAll uint64    //总操作数
var ActReqCount uint64       //操作数
var ActRspSucessCount uint64 //操作成功数
var ActRspFailCount uint64   //操作失败数
var ActReqTimePoint int64    //开始时间
var ActLastTimePoint int64   //最后响应时间
var QpsMax float64           //qps最大值

var mqClient *RabbitMQClient
var clientPool *RabbitMQClientPool
var Log logger.Logger

//发布测试
func TestPuslish(t *testing.T) {
	var err error
	cfg := logger.NewDevelopmentConfig()
	cfg.Encoding = "json"
	cfg.OutputPaths = append(cfg.OutputPaths, "access_log.txt")
	Log = logger.NewMyLogger(cfg)

	user := url.QueryEscape("gouser")
	password := url.QueryEscape("DEIro34KE@#$")
	urlStr := fmt.Sprintf("amqp://%s:%s@172.18.2.85:5672/q1autoops", user, password)
	Log.Debug("urlStr=", urlStr)

	mqcfg := &MqConfig{
		AmqpUrl:    urlStr,
		ChannelNum: 10,
	}

	mqClient, err = NewMQClient(mqcfg, nil, &Log)
	if err != nil {
		Log.Debug("err=", err.Error())
	}

	now := time.Now().Unix()
	ticker := time.NewTicker(1 * time.Second)
	for {
		<-ticker.C

		for i := 0; i < 10; i++ {
			t.Log("SendOneMsg i=", i)
			SendOneMsg("AutoOpsEvent", "75f3942cd7719cfa0f4d9ab271e638ab1c61ab83f0a30095d3a526be240778c5", now+int64(i))
		}

	}

}

//channel池消费测试
func TestConsumer(t *testing.T) {
	var err error
	cfg := logger.NewDevelopmentConfig()

	cfg.OutputPaths = append(cfg.OutputPaths, "channepool_consumer.txt")
	cfg.MaxSize = 500 * 1024 * 1024 //单位为字节

	Log = logger.NewMyLogger(cfg)

	var mqHander MqTestHandler
	user := url.QueryEscape("gouser")
	password := url.QueryEscape("DEIro34KE@#$")
	urlStr := fmt.Sprintf("amqp://%s:%s@172.18.2.85:5672/Y", user, password)
	Log.Debug("urlStr=", urlStr)

	queueName1 := "WebToGameHequn1"
	queueName2 := "WebToGameHequn2"
	mqcfg := &MqConfig{
		AmqpUrl:       urlStr,
		PrefetchCount: 10,
		ChannelNum:    10,
		ConsumerList:  make([]*Consumer, 0),
	}

	mqcfg.ConsumerList = append(mqcfg.ConsumerList, &Consumer{
		ConsumeMqExchange:   CONSUME_EXCHANGE,
		ConsumeExchangeType: amqp.ExchangeDirect,
		ConsumeMqQueue:      queueName1,
		RoutingKey:          "hequnKey",
		AutoAck:             false,
	})

	mqcfg.ConsumerList = append(mqcfg.ConsumerList, &Consumer{
		ConsumeMqExchange:   CONSUME_EXCHANGE,
		ConsumeExchangeType: amqp.ExchangeDirect,
		ConsumeMqQueue:      queueName2,
		RoutingKey:          "hequnKey",
		AutoAck:             false,
	})

	ActReqTimePoint = time.Now().UnixNano()
	go printQpsInfo()

	mqClient, err = NewMQClient(mqcfg, &mqHander, &Log)
	if err != nil {
		Log.Debug("err=", err.Error())
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGPIPE)
LOOP:
	sig := <-signalCh
	if sig == syscall.SIGPIPE { //往终端打印消息过多，终端发送SIGPIPE信号导致进程退出
		goto LOOP
	}

	if sig == syscall.SIGTERM { //正常结束程序
		os.Exit(0)
	}
}

//连接池消费测试
func TestPoolConsumer(t *testing.T) {
	var err error
	cfg := logger.NewDevelopmentConfig()

	cfg.OutputPaths = append(cfg.OutputPaths, "connpool_consumer.txt")
	cfg.MaxSize = 500 * 1024 * 1024 //单位为字节

	Log = logger.NewMyLogger(cfg)

	var mqHander MqTestHandler
	user := url.QueryEscape("gouser")
	password := url.QueryEscape("DEIro34KE@#$")
	urlStr := fmt.Sprintf("amqp://%s:%s@172.16.124.61:5672/Y", user, password)
	Log.Debug("urlStr=", urlStr)

	queueName1 := "WebToGameHequn1"
	queueName2 := "WebToGameHequn2"
	mqcfg := &MqConfig{
		AmqpUrl:       urlStr,
		PrefetchCount: 10,
		ChannelNum:    1,
		ConnPoolNum:   10, //10个连接
		ConsumerList:  make([]*Consumer, 0),
	}

	mqcfg.ConsumerList = append(mqcfg.ConsumerList, &Consumer{
		ConsumeMqExchange:   CONSUME_EXCHANGE,
		ConsumeExchangeType: amqp.ExchangeDirect,
		ConsumeMqQueue:      queueName1,
		RoutingKey:          "hequnKey",
		AutoAck:             false,
	})

	mqcfg.ConsumerList = append(mqcfg.ConsumerList, &Consumer{
		ConsumeMqExchange:   CONSUME_EXCHANGE,
		ConsumeExchangeType: amqp.ExchangeDirect,
		ConsumeMqQueue:      queueName2,
		RoutingKey:          "hequnKey",
		AutoAck:             false,
	})

	ActReqTimePoint = time.Now().UnixNano()
	go printQpsInfo()

	clientPool, err = NewMQClientPool(mqcfg, &mqHander, &Log)
	if err != nil {
		Log.Debug("err=", err.Error())
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGPIPE)
LOOP:
	sig := <-signalCh
	if sig == syscall.SIGPIPE { //往终端打印消息过多，终端发送SIGPIPE信号导致进程退出
		goto LOOP
	}

	if sig == syscall.SIGTERM { //正常结束程序
		os.Exit(0)
	}
}

type MqTestHandler struct {
}

//MQ业务处理
func (h *MqTestHandler) HandleMessage(p interface{}) (ret int) {
	if msg, ok := p.(amqp.Delivery); ok {
		log.Println("HandleMessage(), len(msg.Body)=", len(msg.Body))

		atomic.AddUint64(&ActRspSucessCount, 1)
		atomic.StoreInt64(&(ActLastTimePoint), time.Now().UnixNano())
	}
	return
}

//发送一条消息
func SendOneMsg(exchange, routingKey string, index int64) {
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

	MqSendMsg(exchange, routingKey, userData)

}

//发送消息
func MqSendMsg(exchange, routingKey string, m *PlayerData) error {
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

	err = mqClient.Publish(exchange, routingKey, false, false, false, msg)
	if err != nil {
		Log.Error("Failed to publish a message err=", err.Error())
		return err
	} else {
		Log.Debug("send success")
	}
	return nil
}

func printQpsInfo() {
	for {
		fmt.Printf("-----------------------------\n")
		{
			reqCount := atomic.LoadUint64(&ActReqCount)
			rspSuccCount := atomic.LoadUint64(&ActRspSucessCount)
			rspFailCount := atomic.LoadUint64(&ActRspFailCount)
			atomic.AddUint64(&ActReqCountAll, ActReqCount)

			timeCount := float64(ActLastTimePoint-ActReqTimePoint) / 1000000000
			qps := 1000000000 * (float64(rspSuccCount) / float64(ActLastTimePoint-ActReqTimePoint))
			if qps > QpsMax {
				QpsMax = qps
			}
			res := fmt.Sprintf("请求数:%d，成功数:%d，失败数：%d，时间:%f，qps:%f， qps最大值=%f，总请求数=%d/s\n", reqCount, rspSuccCount, rspFailCount, timeCount, qps, QpsMax, ActReqCountAll)
			log.Println(res)
			fmt.Println(res)

		}
		atomic.StoreUint64(&ActReqCount, 0)
		atomic.StoreInt64(&ActReqTimePoint, time.Now().UnixNano())
		atomic.StoreUint64(&ActRspSucessCount, 0)
		atomic.StoreUint64(&ActRspFailCount, 0)

		time.Sleep(1 * time.Second)
	}
}

func InitStatic() {
	atomic.StoreUint64(&ActReqCount, 0)
	atomic.StoreUint64(&ActRspSucessCount, 0)
	atomic.StoreUint64(&ActRspFailCount, 0)
	atomic.StoreInt64(&ActReqTimePoint, 0)
	atomic.StoreInt64(&ActLastTimePoint, 0)
}
