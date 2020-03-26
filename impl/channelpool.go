package impl

import (
	"fmt"
	"sync"

	"github.com/hhq163/rabbitmq_channel_pool/base"
	"github.com/streadway/amqp"
)

//ChannelPool amqp 通道池，提供通道复用
type ChannelPool struct {
	url      string
	mutex    sync.Mutex
	conn     *amqp.Connection
	channels []*amqp.Channel
}

//InitPool 初始化通道池
func (cp *ChannelPool) InitPool(url string) {
	cp.url = url
	cp.channels = make([]*amqp.Channel, 0, 1000)
}

func (cp *ChannelPool) getChannel() (*amqp.Channel, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	size := len(cp.channels)
	if size > 0 {
		ch := cp.channels[size-1]
		cp.channels[size-1] = nil
		cp.channels = cp.channels[:size-1]
		return ch, nil
	}
	if cp.conn == nil {
		conn, err := amqp.Dial(cp.url)
		if err != nil {
			return nil, err
		}
		cp.conn = conn
	}
	ch, err := cp.conn.Channel()
	if err != nil {
		cp.conn.Close()
		cp.conn = nil
		return nil, err
	}
	return ch, nil
}

func (cp *ChannelPool) returnChannel(ch *amqp.Channel) {
	cp.mutex.Lock()
	cp.channels = append(cp.channels, ch)
	cp.mutex.Unlock()
}

func (cp *ChannelPool) checkConnect() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	if cp.conn == nil {
		return
	}
	ch, err := cp.conn.Channel()
	if err != nil {
		for i := 0; i < len(cp.channels); i++ {
			cp.channels[i].Close()
			cp.channels[i] = nil
		}
		cp.channels = cp.channels[0:0]
		cp.conn.Close()
		cp.conn = nil
		return
	}
	cp.channels = append(cp.channels, ch)
}

//Publish 发布消息
func (cp *ChannelPool) Publish(exchange, key string, mandatory, immediate, reliable bool, msg amqp.Publishing) error {
	ch, err := cp.getChannel()
	if err != nil {
		return err
	}
	if reliable {
		if err := ch.Confirm(false); err != nil {
			base.Log.Error("Channel could not be put into confirm mode: %s", err)
		}

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		err = ch.Publish(exchange, key, mandatory, immediate, msg)
		if err != nil {
			ch.Close()
			cp.checkConnect()
			return err
		}
		defer cp.ConfirmOne(confirms)

		return nil
	} else {
		err = ch.Publish(exchange, key, mandatory, immediate, msg)
		if err != nil {
			ch.Close()
			cp.checkConnect()
			return err
		}
	}

	cp.returnChannel(ch)
	return nil
}

func (cp *ChannelPool) ConfirmOne(confirms <-chan amqp.Confirmation) error {
	base.Log.Info("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		base.Log.Info("confirmed delivery with delivery tag:", confirmed.DeliveryTag)
		return nil
	} else {
		base.Log.Error("failed delivery of delivery tag:", confirmed.DeliveryTag)
		return fmt.Errorf("failed delivery of delivery tag:", confirmed.DeliveryTag)
	}
}
