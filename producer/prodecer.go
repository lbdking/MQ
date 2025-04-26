package main

import (
	"MQ/message"
	"MQ/utils"
	"fmt"
	"log"
	"time"
)

func main() {
	// 启动 UUID 工厂
	go utils.UuidFactory()

	// 启动 Topic 工厂
	inMemSize := 10
	go message.TopicFactory(inMemSize)

	// 获取 Topic
	topicName := "testTopic"
	topic := message.GetTopic(topicName)

	// 生产者发送消息
	for i := 0; i < 5; i++ {
		msgData := []byte(fmt.Sprintf("Message %d", i))
		msg := message.NewMessage(msgData)
		topic.PutMessage(msg)
		log.Printf("Producer sent message: %s", string(msg.Body()))
		time.Sleep(1 * time.Second)
	}
	for {
		select {}
	}
}
