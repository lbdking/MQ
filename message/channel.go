package message

import (
	"MQ/utils"
	"errors"
	"log"
	"time"
)

//TODO
/*
1.其实感觉使用切片存储consumer并不是很合适，因为后续删除客户端的时候，需要进行查询，但是silence查询复杂度为o（n），使用map感觉会更好，后续进行修改。
2.并发安全不能保障，如果同时进行添加或者删除客户端，有可能破坏切片或者map的内存结构导致崩溃
*/

type Consumer interface {
	Close()
}
type Channel struct {
	name string
	//添加客户端的请求的通道
	addChannel chan utils.ChanReq
	//删除
	removeChannel chan utils.ChanReq
	clients       []Consumer

	//接收生产者的消息
	incomingMessageChan chan *Message
	//暂存消息，有缓冲区的channel，超出长度会丢失
	msgChan chan *Message
	//消费者读取管道
	clientMessageChan chan *Message

	//接收关闭信号的该管道，接收到信息之后关闭转发消息以及消费者的连接
	exitChan chan utils.ChanReq

	//读取写入存储已经发送的消息
	inFlightMessageChan chan *Message
	//用来存储已发送的消息
	inFlightMessages map[string]*Message

	//接收完成消息处理的请求
	finishMessageChan chan utils.ChanReq

	//接收消息重新入队的请求
	requeueMessageChan chan utils.ChanReq
}

func NewChannel(name string, inMeSize int) *Channel {
	channel := &Channel{
		name:                name,
		addChannel:          make(chan utils.ChanReq),
		removeChannel:       make(chan utils.ChanReq),
		clients:             make([]Consumer, inMeSize, 5),
		incomingMessageChan: make(chan *Message, 5),
		msgChan:             make(chan *Message, inMeSize),
		clientMessageChan:   make(chan *Message),
		exitChan:            make(chan utils.ChanReq),
		inFlightMessageChan: make(chan *Message),
		inFlightMessages:    make(map[string]*Message),
		finishMessageChan:   make(chan utils.ChanReq),
		requeueMessageChan:  make(chan utils.ChanReq),
	}
	go channel.Router()
	return channel
}

// AddClient 使用无缓冲的channel将client传递给服务端。
func (c *Channel) AddClient(client Consumer) {
	log.Printf("Channel(%s):AddClient ", client)
	doneChannel := make(chan interface{})
	c.addChannel <- utils.ChanReq{
		Variable: client,
		RetChan:  doneChannel,
	}
	//阻塞协程，doneChan通道接收信号，添加操作完成。
	<-doneChannel
}

func (c *Channel) RemoveClient(client Consumer) {
	log.Printf("Channel(%s):RemoveClient ", client)
	doneChannel := make(chan interface{})
	c.removeChannel <- utils.ChanReq{
		Variable: client,
		RetChan:  doneChannel,
	}
	<-doneChannel
}

func (c *Channel) Router() {
	var clientReq utils.ChanReq
	var closeChan chan struct{}

	//实时转发消息
	go c.MessagePump(closeChan)

	go c.RequeueRouter(closeChan)

	for {
		select {
		case clientReq = <-c.addChannel:
			//读取客户端
			client := clientReq.Variable.(Consumer)
			c.clients = append(c.clients, client)

			log.Printf("Channel:%s Add New Client:%s\n", c.name, client)
			clientReq.RetChan <- struct{}{}

		case clientReq = <-c.removeChannel:
			client := clientReq.Variable.(Consumer)
			index := -1
			//寻找要删除的客户端
			for i, c := range c.clients {
				if c == client {
					index = i
					break
				}
			}
			if index == -1 {
				log.Printf("Could not find client:%v in clients(%v)\n", client, c.clients)
			} else {
				c.clients = append(c.clients[:index], c.clients[index+1:]...)
				log.Printf("Channel:%s Remove Client:%s\n", c.name, client)
			}
			clientReq.RetChan <- struct{}{}

		case msg := <-c.incomingMessageChan:
			//将消息生产者的消息存储到channel的缓存区channel中，default超出缓冲区会丢失消息
			select {
			case c.msgChan <- msg:
				log.Printf("Channel:%s Receive Message:%v\n", c.name, msg)
			default:
			}
		}
	}
}

// PutMessage 生产者向channel中发送消息
func (c *Channel) PutMessage(msg *Message) {
	c.incomingMessageChan <- msg
}

// PullMessage 消费者拉取消息
func (c *Channel) PullMessage() *Message {
	return <-c.incomingMessageChan
}

// MessagePump 消息转发，将消息转发给客户端
func (c *Channel) MessagePump(closeChan chan struct{}) {
	var msg *Message

	for {
		select {
		case msg = <-c.msgChan:
		}
		c.clientMessageChan <- msg
		if msg != nil {
			c.inFlightMessageChan <- msg
		}
		c.clientMessageChan <- msg
	}
}

func (c *Channel) Close() error {
	errChan := make(chan interface{})
	c.exitChan <- utils.ChanReq{
		RetChan: errChan,
	}

	err := (<-errChan).(error)
	return err
}

func (c *Channel) pushInFlightMessage(msg *Message) {
	c.inFlightMessages[utils.UuidToString(msg.Uuid())] = msg
}

func (c *Channel) popInFlightMessageChan(uuid string) (*Message, error) {
	msg, ok := c.inFlightMessages[uuid]
	if ok {
		return nil, errors.New("UUID not Find")
	}
	delete(c.inFlightMessages, uuid)
	return msg, nil
}

// FinishMessage 标记指定消息的已经完成处理，并接收错误信息
func (c *Channel) FinishMessage(uuid string) error {
	errChan := make(chan interface{})
	c.finishMessageChan <- utils.ChanReq{
		Variable: uuid,
		RetChan:  errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}

// RequeueMessage 重新入队请求，同上一样接收错误信息
func (c *Channel) RequeueMessage(uuid string) error {
	errChan := make(chan interface{})
	c.requeueMessageChan <- utils.ChanReq{
		Variable: uuid,
		RetChan:  errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}

func (c *Channel) RequeueRouter(closeChan chan struct{}) {
	for {
		select {
		case msg := <-c.inFlightMessageChan:
			go func(msg *Message) {
				select {
				//一分钟之内如果没有消费者消费,重新将消息入队
				case <-time.After(1 * time.Second):
					log.Printf("Channel(%s): requeue message(%v)\n", c.name, msg)
				case <-msg.timeChan:
					return
				}
				err := c.RequeueMessage(utils.UuidToString(msg.Uuid()))
				if err != nil {
					log.Printf("Channel(%s): requeue message(%v) Error\n", c.name, msg)
				}
			}(msg)
			c.pushInFlightMessage(msg)
		case <-closeChan:
			return
		//检测到重入队列的信号就把消息读取进来，然后循环写入incomingMessageChan
		case requeueReq := <-c.requeueMessageChan:
			uuid := requeueReq.Variable.(string)
			msg, err := c.popInFlightMessageChan(uuid)
			if err != nil {
				log.Printf("Channel(%s):RequeueMessage:%s\n", c.name, uuid)
			} else {
				go func(msg *Message) {
					c.PutMessage(msg)
				}(msg)
			}
		case finishReq := <-c.finishMessageChan:
			uuid := finishReq.Variable.(string)
			_, err := c.popInFlightMessageChan(uuid)
			if err != nil {
				log.Printf("Error: Failed to finish menssage:uuid:%s----%s", uuid, err)
			}
			finishReq.RetChan <- err
		case <-closeChan:
			return
		}
	}
}
