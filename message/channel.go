package message

import (
	"MQ/utils"
	"log"
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

	//实时转发消息
	go c.MessagePump()

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
				log.Printf("Channel:%s Receive Message:%s\n", c.name, msg)
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
func (c *Channel) MessagePump() {
	var msg *Message

	for {
		select {
		case msg = <-c.msgChan:
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
