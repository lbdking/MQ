package main

import (
	"MQ/message"
	"MQ/utils"
	"log"
)

// MockConsumer 模拟消费者
type MockConsumer struct {
	Name string
}

func (m *MockConsumer) Close() {
	log.Printf("Consumer %s is closing", m.Name)
}

func main() {
	// 启动 Topic 工厂
	inMemSize := 10
	go message.TopicFactory(inMemSize)

	// 获取 Topic
	topicName := "testTopic"
	topic := message.GetTopic(topicName)

	// 获取 Channel
	channelName := "testChannel"
	channel := topic.GetChannel(channelName)

	// 创建两个消费者
	consumer1 := &MockConsumer{Name: "Consumer1"}
	consumer2 := &MockConsumer{Name: "Consumer2"}

	// 向 Channel 添加消费者
	channel.AddClient(consumer1)
	channel.AddClient(consumer2)

	// 消费者消费消息
	go func() {
		for {
			msg := channel.PullMessage()
			if msg != nil {
				log.Printf("%s received message: %s", consumer1.Name, string(msg.Body()))
				channel.FinishMessage(utils.UuidToString(msg.Uuid()))
			}
		}
	}()

	go func() {
		for {
			msg := channel.PullMessage()
			if msg != nil {
				log.Printf("%s received message: %s", consumer2.Name, string(msg.Body()))
				channel.FinishMessage(utils.UuidToString(msg.Uuid()))
			}
		}
	}()

	// 保持程序运行
	select {}
}
