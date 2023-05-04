package DawnMicroHub

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"skygitlab.q1oa.com/root/DawnCore/util"
	"skygitlab.q1oa.com/root/DawnMicroHub/proto/netmsg"
)

const (
	CONSUME_EXCHANGE = "GameToWebH"
)

var ActReqCountAll uint64    //总操作数
var ActReqCount uint64       //操作数
var ActRspSucessCount uint64 //操作成功数
var ActRspFailCount uint64   //操作失败数
var ActReqTimePoint int64    //开始时间
var ActLastTimePoint int64   //最后响应时间
var QpsMax float64           //qps最大值

var cfg *util.LogConfig

var mqClient *RabbitMQClient
var clientPool *RabbitMQClientPool
var Log *util.Logger
var srcSSID SSrvID
var dstsrvID SSrvID

//发布测试
func TestPuslish(t *testing.T) {
	var err error
	cfg = util.NewDevelopmentConfig(5, 1)

	cfg.FileName = util.GetExecpath() + "/logs/test.html"
	cfg.MaxSize = 500 * 1024 * 1024 //单位为字节

	Log, err = util.NewLog(cfg)
	if err != nil {
		log.Fatal("util.NewLog err=", err.Error())
	}
	srcSSID = SSrvID{
		SvrNo:     1,
		SvrType:   5,
		ClusterID: 1,
	}
	dstsrvID = SSrvID{
		SvrNo:     1,
		SvrType:   0,
		ClusterID: 1,
	}

	user := url.QueryEscape("gouser")
	password := url.QueryEscape("DEIro34KE@#$")
	urlStr := fmt.Sprintf("amqp://%s:%s@172.16.124.61:5672/Y", user, password)
	Log.Debug("urlStr=", urlStr)

	mqcfg := &MqConfig{
		AmqpUrl:    urlStr,
		ChannelNum: 10,
	}

	mqClient, err = NewMQClient(mqcfg, nil, Log)
	if err != nil {
		Log.Debug("err=", err.Error())
	}

	now := time.Now().Unix()
	ticker := time.NewTicker(1 * time.Second)
	for {
		<-ticker.C

		for i := 0; i < 1000; i++ {
			t.Log("SendOneMsg i=", i)
			SendOneMsg(CONSUME_EXCHANGE, "hequnKey", now+int64(i))
		}

	}

}

//channel池消费测试
func TestConsumer(t *testing.T) {
	var err error
	cfg = util.NewDevelopmentConfig(5, 1)

	cfg.FileName = util.GetExecpath() + "/logs/test.html"
	cfg.MaxSize = 500 * 1024 * 1024 //单位为字节

	Log, err = util.NewLog(cfg)
	if err != nil {
		log.Fatal("util.NewLog err=", err.Error())
	}
	srcSSID = SSrvID{
		SvrNo:     1,
		SvrType:   5,
		ClusterID: 1,
	}
	dstsrvID = SSrvID{
		SvrNo:     1,
		SvrType:   0,
		ClusterID: 1,
	}

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

	mqClient, err = NewMQClient(mqcfg, &mqHander, Log)
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
	cfg = util.NewDevelopmentConfig(5, 1)

	cfg.FileName = util.GetExecpath() + "/logs/test.html"
	cfg.MaxSize = 500 * 1024 * 1024 //单位为字节

	Log, err = util.NewLog(cfg)
	if err != nil {
		log.Fatal("util.NewLog err=", err.Error())
	}
	srcSSID = SSrvID{
		SvrNo:     1,
		SvrType:   5,
		ClusterID: 1,
	}
	dstsrvID = SSrvID{
		SvrNo:     1,
		SvrType:   0,
		ClusterID: 1,
	}

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

	clientPool, err = NewMQClientPool(mqcfg, &mqHander, Log)
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
		m, err := DecodeTcp(msg.Body)
		if err != nil {
			log.Print("DecodeTcp err=", err.Error())
			return
		}

		// log.Println("HandleMessage(), m=", m.MsgID)

		rsp := &netmsg.TMSG_AGENT_RUNCMD_RSP{}
		err = proto.Unmarshal(m.Data, rsp)
		if err != nil {
			log.Println("err=", err.Error())
			return
		}
		// log.Println("rsp.CmdRecordSn=", rsp.CmdRecordSn)
		atomic.AddUint64(&ActRspSucessCount, 1)
		atomic.StoreInt64(&(ActLastTimePoint), time.Now().UnixNano())

		// now := time.Now().Unix()
		// SendOneMsg("GameToWebH", "hequnKey", now)
	}
	return
}

//发送一条消息
func SendOneMsg(exchange, routingKey string, index int64) {

	rsp := &netmsg.TMSG_AGENT_RUNCMD_RSP{
		Ret:         uint32(netmsg.ERRCODE_ERR_NOERROR),
		Message:     "doing",
		CmdTarget:   fmt.Sprintf("%d:%d:%d", dstsrvID.ClusterID, dstsrvID.SvrType, dstsrvID.SvrNo), //clusterID:svrType:svrNo
		CmdSn:       123456,
		CmdName:     "Proc doning",
		CmdRecordSn: uint64(index),
		CmdObj:      []string{fmt.Sprintf("is a test index=%d", index)},
		StatusCode:  netmsg.CMDSTATE_CODE_EXECING,
	}

	MqSendMsg(exchange, routingKey, rsp, uint16(netmsg.NETMSGID_MSG_AGENT_RUNCMD_RSP))

}

//发送消息
func MqSendMsg(exchange, routingKey string, m protoreflect.ProtoMessage, msgID uint16) error {
	p := genMsg(srcSSID, dstsrvID, msgID, m)
	data, err := EncodeTcp(p)
	if err != nil {
		// log.Println("EncodeTcp(), err=", err.Error())
		Log.Error("EncodeTcp(), err=", err.Error())
		return err
	}

	msg := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "text/plain",
		ContentEncoding: "",
		Body:            data,
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

func genMsg(srcSvr, dstSvr SSrvID, msgID uint16, m protoreflect.ProtoMessage) *Packet {
	data, err := proto.Marshal(m)
	if err != nil {
		Log.Error("proto.Marshal err=", err.Error())
		return nil
	}

	p := &Packet{}
	p.SrcSvr = srcSvr
	p.DestSvr = dstSvr
	p.MsgID = msgID
	p.Data = data

	return p
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
