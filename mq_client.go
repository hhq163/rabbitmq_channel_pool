package rabbitmq_channel_pool

import (
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/hhq163/kk_core/util"
	"github.com/streadway/amqp"

	"github.com/hhq163/logger"
)

const (
	RECONNECT_TIME_MIN = 100 //单位,Millisecond
	RECONNECT_TIME_MAX = 10  //单位，Second
	RECONNECT_TIME     = 10  //增加等待时间的次数
)

//业务处理接口
type IMqHandler interface {
	HandleMessage(p interface{}) (ret int)
}

type MqConfig struct {
	AmqpUrl       string //连接URL
	PrefetchCount int    //每个消费者的预取数量
	ChannelNum    int    //channel池大小
	ConnPoolNum   int    //连接池大小

	ConsumerList []*Consumer //消费者信息
	ConsumerNum  int         //消费者数量
	ConnId       int         //连接id，用于多个连接时区别
}

//消费者信息
type Consumer struct {
	ConsumeMqExchange   string //交换机名称
	ConsumeExchangeType string //消费者交换机模式
	ConsumeMqQueue      string //消费者队列名
	RoutingKey          string //消费者routingKey
	AutoAck             bool   //是否自动ACK
}

//MQ客户端对象
type RabbitMQClient struct {
	Config  *MqConfig
	handler IMqHandler

	conn        *amqp.Connection //连接
	channelPool []*amqp.Channel  //channel池

	rLock             sync.Mutex
	chanReConnectFlag []bool //channel重连标识

	connNotify    chan *amqp.Error
	channelNotify chan *amqp.Error //channel产生的错误

	log *logger.Logger
}

/**
 * @description:创建一个MQ客户端对象，支持channl池
 * @param {*MqConfig} config 配置信息
 * @param {IMqHandler} h 业务处理逻辑
 * @param {*logger.Logger} log 日志组件
 * @return {*}
 */
func NewMQClient(cfg *MqConfig, h IMqHandler, log *logger.Logger) (*RabbitMQClient, error) {
	rand.Seed(time.Now().Unix())
	if cfg.AmqpUrl == "" || log == nil {
		return nil, fmt.Errorf("param is illegal cfg.AmqpUrl=%s,log == nil", cfg.AmqpUrl)
	}
	if cfg.ChannelNum == 0 { //	默认是1
		cfg.ChannelNum = 1
	}
	if cfg.ConsumerList == nil {
		cfg.ConsumerList = make([]*Consumer, 0)
	}
	cfg.ConsumerNum = len(cfg.ConsumerList)
	cPoolNum := cfg.ChannelNum

	if cfg.ConsumerNum != 0 {
		cPoolNum = cfg.ChannelNum * cfg.ConsumerNum
	}

	c := &RabbitMQClient{
		Config:            cfg,
		handler:           h,
		log:               log,
		channelPool:       make([]*amqp.Channel, cPoolNum),
		chanReConnectFlag: make([]bool, cPoolNum),
		channelNotify:     make(chan *amqp.Error, cPoolNum),
	}

	return c, c.Init()
}

//客户端初始化
func (c *RabbitMQClient) Init() error {
	var err error
	tryNum := 0
	for {
		err = c.Connect()
		tryNum++
		if err != nil {
			c.log.Error("Connect() err=", err.Error())
			if tryNum > 10 {
				return err
			} else {
				time.Sleep(time.Duration(1) * time.Second)
			}
		} else {
			break
		}
	}

	err = c.Declare()
	if err != nil {
		c.log.Error("Declare() err =", err.Error())
		return err
	}
	err = c.Subscribe()
	if err != nil {
		c.log.Error("Init() err=", err.Error())
		return err
	}
	util.SafeGo(c.NotifyHandle, c.log)

	return nil
}

func (c *RabbitMQClient) Connect() error {
	c.log.Debug("Connect() in")
	if c.conn != nil {
		c.CloseConn()
		c.conn = nil
	}
	var err error
	c.conn, err = amqp.Dial(c.Config.AmqpUrl)
	if err != nil {
		c.log.Error("amqp.Dial err=", err.Error(), ",c.Config.AmqpUrl=", c.Config.AmqpUrl)
		return err
	}

	for i := range c.channelPool {
		c.channelPool[i], err = c.conn.Channel()
		if err != nil {
			for j := 0; j < i; j++ {
				c.channelPool[j].Close()
			}
			c.log.Error("conn.Channel() err=", err.Error(), "i=", i, ",c.Config.AmqpUrl=", c.Config.AmqpUrl)
			return err
		}
	}

	c.connNotify = c.conn.NotifyClose(make(chan *amqp.Error))

	for i := range c.channelPool {
		if c.channelPool[i] == nil {
			c.log.Error("RabbitMQClient.Connect c.channelPool[i] is nil, i=", i)
			continue
		}
		util.SafeGo(c.ChannelNotifyClose, c.log, i)
	}

	return nil
}

//channel错误处理协程
func (c *RabbitMQClient) ChannelNotifyClose(index int) {
	if index >= len(c.channelPool) || c.channelPool[index] == nil {
		c.log.Error("channelNotifyClose err, index=", index, "len(c.channelPool) =", len(c.channelPool))
	}
	err := <-c.channelPool[index].NotifyClose(make(chan *amqp.Error))
	if err == nil { //主动关闭
		c.log.Info("ChannelNotifyClose closed index=", index, ",ConnId=", c.Config.ConnId)
	} else { //异常关闭
		c.log.Info("ChannelNotifyClose abnormal index=", index, ",ConnId=", c.Config.ConnId)
		c.rLock.Lock()
		defer c.rLock.Unlock()

		c.chanReConnectFlag[index] = true
		c.channelNotify <- err
	}
}

//交换机和队列声明
func (c *RabbitMQClient) Declare() (err error) {
	for _, consumer := range c.Config.ConsumerList {

		for i := range c.channelPool {
			if c.channelPool[i] == nil {
				c.log.Error("Declare channel is nil index=", i)
				continue
			}

			err = c.channelPool[i].ExchangeDeclare(consumer.ConsumeMqExchange, consumer.ConsumeExchangeType, true, false, false, false, nil)
			if err != nil {
				c.log.Error("ExchangeDeclare() err=", err.Error(), ",consumer.ConsumeMqExchange=", consumer.ConsumeMqExchange)
				return err
			}
			_, err = c.channelPool[i].QueueDeclare(consumer.ConsumeMqQueue, true, false, false, false, nil)
			if err != nil {
				c.log.Error("QueueDeclare() err=", err.Error(), ",consumer.ConsumeMqExchange=", consumer.ConsumeMqExchange, ",consumer.ConsumeMqQueue=", consumer.ConsumeMqQueue)
				return err
			}
			err = c.channelPool[i].QueueBind(consumer.ConsumeMqQueue, consumer.RoutingKey, consumer.ConsumeMqExchange, false, nil)
			if err != nil {
				c.log.Error("QueueBind() err=", err.Error(), ",consumer.ConsumeMqExchange=", consumer.ConsumeMqExchange, ",consumer.ConsumeMqQueue=", consumer.ConsumeMqQueue, ",consumer.RoutingKey=", consumer.RoutingKey)
				return err
			}

		}
	}

	return
}

//订阅消费
func (c *RabbitMQClient) Subscribe() (err error) {

	if c.Config.ConsumerList != nil && len(c.Config.ConsumerList) > 0 {
		for i, consumer := range c.Config.ConsumerList {
			for j := i * c.Config.ChannelNum; j < (i+1)*c.Config.ChannelNum; j++ {
				if c.channelPool[j] == nil {
					c.channelPool[j], err = c.conn.Channel()
					if err != nil {
						c.log.Error("create channel err=", err.Error())
						return fmt.Errorf("create channel err=%s", err.Error())
					}
				}
				if c.Config.PrefetchCount > 0 {
					err = c.channelPool[j].Qos(c.Config.PrefetchCount, 0, false)
					if err != nil {
						c.log.Error("channel.Qos err=", err.Error())
						return err
					}
				}

				msgs, err := c.channelPool[j].Consume(consumer.ConsumeMqQueue, consumer.RoutingKey, consumer.AutoAck, false, false, false, nil)
				if err != nil {
					c.log.Error("err=", err.Error())
					return err
				}
				util.SafeGo(c.Handle, c.log, msgs)
				c.log.Info("Subscribe channel j=", j, " ,queueName=", consumer.ConsumeMqQueue, ",routeKey=", consumer.RoutingKey)

			}
		}

	}

	return nil
}

//发布消息
func (c *RabbitMQClient) Publish(exchange, key string, mandatory, immediate, reliable bool, msg amqp.Publishing) (err error) {

	var channlTotal int
	if c.Config.ConsumerNum > 0 {
		channlTotal = c.Config.ChannelNum * c.Config.ConsumerNum
	} else {
		channlTotal = c.Config.ChannelNum
	}
	index := rand.Intn(channlTotal)
	channel := c.channelPool[index]
	if channel == nil {
		return fmt.Errorf("Publish() channel is nil, index=%d", index)
	}
	if reliable {
		if err := channel.Confirm(false); err != nil {
			c.log.Error("Channel could not be put into confirm mode: err=", err)
		}

		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		err = channel.Publish(exchange, key, mandatory, immediate, msg)
		if err != nil {
			c.log.Error("Publish err=", err.Error())
			return err
		}
		defer c.ConfirmOne(confirms)

		return nil
	} else {
		err = channel.Publish(exchange, key, mandatory, immediate, msg)
		if err != nil {
			c.log.Error("Publish err=", err.Error())
			return err
		}
	}
	return nil
}

func (c *RabbitMQClient) ConfirmOne(confirms <-chan amqp.Confirmation) error {
	c.log.Debug("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		c.log.Info("confirmed delivery with delivery tag:", confirmed.DeliveryTag)
		return nil
	} else {
		c.log.Error("failed delivery of delivery tag:", confirmed.DeliveryTag)
		return fmt.Errorf("failed delivery of delivery tag:%d", confirmed.DeliveryTag)
	}
}

//消息ACK
func (c *RabbitMQClient) Ack(index int, tag uint64, multiple bool) error {
	if index >= len(c.channelPool) {
		c.log.Error("RabbitMQClient.Ack index err! index=", index, " len=", len(c.channelPool), " id=", c.Config.ConnId)
		return nil
	}
	channel := c.channelPool[index]
	if channel == nil {
		c.log.Error("RabbitMQClient.Ack channel nil! index=", index, " len=", len(c.channelPool), " id=", c.Config.ConnId)
		return nil
	}
	err := channel.Ack(tag, multiple)
	if err != nil {
		c.log.Error("RabbitMQClient Ack err=", err.Error(), " index=", index, " id=", c.Config.ConnId)
		return err
	}
	return nil
}

//消息NACK
func (c *RabbitMQClient) NAck(index int, tag uint64, multiple bool, requeue bool) error {
	if index >= len(c.channelPool) {
		c.log.Error("RabbitMQClient.NAck index err! index=", index, " len=", len(c.channelPool), " id=", c.Config.ConnId)
		return nil
	}
	channel := c.channelPool[index]
	if channel == nil {
		c.log.Error("RabbitMQClient.NAck channel nil! index=", index, " len=", len(c.channelPool), " id=", c.Config.ConnId)
		return nil
	}
	err := channel.Nack(tag, multiple, requeue)
	if err != nil {
		c.log.Error("RabbitMQClient NAck err=", err.Error(), " index=", index, " id=", c.Config.ConnId)
		return err
	}
	return nil
}

//异常处理及重连逻辑
func (c *RabbitMQClient) NotifyHandle() {
	defer func() {
		if p := recover(); p != nil {
			c.log.Info("stack=", string(debug.Stack()))
			c.log.Error("socket read panic err: ", p)
		}
	}()

	for {
		select {
		case err := <-c.connNotify:
			if err == nil { //主动关闭
				c.log.Info("mq connection closed")
			} else { //异常关闭
				c.log.Error("channelNotify err=" + err.Error())
				c.CloseConn()
				c.tryReConnect()
			}
		case err := <-c.channelNotify:
			if err == nil { //主动关闭
				c.log.Info("mq channel closed")
			} else {
				c.log.Error("channelNotify err=" + err.Error())

				c.log.Debug("channelNotify 111")
				if c.conn.IsClosed() { //重连
					c.log.Debug("c.conn.IsClosed")
					c.tryReConnect()
				} else {
					c.rLock.Lock()
					for i := range c.chanReConnectFlag {
						if c.chanReConnectFlag[i] {
							if c.ReOpenChannel(i) {
								c.chanReConnectFlag[i] = false
							}
						}
					}
					c.rLock.Unlock()
				}

			}
		}
		//重新订阅队列
		err := c.Subscribe()
		if err != nil {
			c.log.Error(err)
		}
	}
}

//尝试重连
func (c *RabbitMQClient) tryReConnect() error {
	tryNum := 0
	for {
		err := c.Connect()
		tryNum++
		if err != nil { //连接失败
			c.log.Info("tryReConnect failed ")
			if tryNum > RECONNECT_TIME {
				time.Sleep(RECONNECT_TIME_MAX * time.Second)
			} else {
				time.Sleep(RECONNECT_TIME_MIN * time.Millisecond)
			}
		} else {
			c.log.Info("tryReConnect success")
			break
		}
	}

	c.rLock.Lock()
	defer c.rLock.Unlock()

	for i := range c.chanReConnectFlag {
		c.chanReConnectFlag[i] = false
	}

	return nil
}

//取消息
func (c *RabbitMQClient) Handle(delivery <-chan amqp.Delivery) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Info("stack=", string(debug.Stack()))
			c.log.Error("socket read panic err: ", p)
		}
	}()

	for msg := range delivery {
		if c.handler != nil {
			c.handler.HandleMessage(msg)
			msg.Ack(false)
		}
	}

}

//重新创建通道
func (c *RabbitMQClient) ReOpenChannel(index int) bool {
	err := c.CloseChannel(index)
	if err != nil {
		c.log.Debug("ReOpenChannel close channel fail ", err.Error())
		return false
	}
	if c.conn == nil {
		c.log.Debug("c.conn is nil")
		return false
	}

	tryNum := 0
	for {
		c.channelPool[index], err = c.conn.Channel()
		tryNum++
		if err != nil {
			c.log.Error("ReOpenChannel Channel err=", err.Error())
			if tryNum > RECONNECT_TIME {
				time.Sleep(RECONNECT_TIME_MAX * time.Second)
			} else {
				time.Sleep(RECONNECT_TIME_MIN * time.Millisecond)
			}
		} else {
			break
		}
	}

	return true
}

func (c *RabbitMQClient) CloseChannel(index int) error {
	if c.channelPool[index] == nil {
		return nil
	}

	return c.channelPool[index].Close()
}

//关闭连接
func (c *RabbitMQClient) CloseConn() (err error) {
	for i := range c.channelPool {
		c.channelPool[i].Close()
		c.channelPool[i] = nil
	}

	if c.conn != nil {
		err = c.conn.Close()
	}

	c.conn = nil
	return
}

//MQ连接池对象
type RabbitMQClientPool struct {
	cfg        *MqConfig
	clientPool []*RabbitMQClient
}

/**
 * @description:创建一个MQ连接池对象
 * @param {*MqConfig} config 配置信息
 * @param {IMqHandler} h 业务处理逻辑
 * @param {*logger.Logger} log 日志组件
 * @return {*}
 */

func NewMQClientPool(cfg *MqConfig, h IMqHandler, log *logger.Logger) (*RabbitMQClientPool, error) {
	rand.Seed(time.Now().Unix())
	var err error
	if cfg.AmqpUrl == "" || log == nil {
		log.Error("param is illegal cfg.AmqpUrl=", cfg.AmqpUrl, ",log == nil")
		return nil, fmt.Errorf("param is illegal cfg.AmqpUrl=%s,log == nil || h == nil", cfg.AmqpUrl)
	}
	if cfg.ChannelNum == 0 { //	默认是1
		cfg.ChannelNum = 1
	}
	if cfg.ConnPoolNum == 0 {
		cfg.ConnPoolNum = 1
	}
	if cfg.ConsumerList == nil {
		cfg.ConsumerList = make([]*Consumer, 0)
	}
	cfg.ConsumerNum = len(cfg.ConsumerList)

	p := &RabbitMQClientPool{
		cfg:        cfg,
		clientPool: make([]*RabbitMQClient, cfg.ConnPoolNum),
	}
	for i := 0; i < cfg.ConnPoolNum; i++ {
		p.clientPool[i], err = NewMQClient(cfg, h, log)
		if err != nil {
			log.Error("NewMQClient err=", err.Error(), ", i=", i)
			return p, err
		}
		p.clientPool[i].Init()
	}

	return p, nil
}

//发布消息
func (p *RabbitMQClientPool) Publish(exchange, key string, mandatory, immediate, reliable bool, msg amqp.Publishing) (err error) {

	clientNum := len(p.clientPool)
	index := rand.Intn(clientNum)
	cli := p.clientPool[index]
	if cli == nil {
		return fmt.Errorf("Publish() cli is nil, index=%d", index)
	}

	return cli.Publish(exchange, key, mandatory, immediate, reliable, msg)
}
