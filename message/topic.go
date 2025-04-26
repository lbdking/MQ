package message

import (
	"MQ/utils"
	"log"
)

type Topic struct {
	name                string              //name topic名称
	newChannelChan      chan utils.ChanReq  //新增channel集合
	channelMap          map[string]*Channel //订阅本topic的channel
	incomingMessageChan chan *Message       //接收消息的chan
	msgChan             chan *Message       //有缓冲的的chan，消息的队列
	readSyncChan        chan struct{}       //和routerSyncChan一起保证channelMap并发安全
	routerSyncChan      chan struct{}
	exitChan            chan utils.ChanReq //接收退出信息的管道
	channelWriteStarted bool               //是否向channel发送信息
}

var (
	TopicMap     = make(map[string]*Topic)
	newTopicChan = make(chan utils.ChanReq)
)

// NewTopic inMemSize消息队列的缓冲区大小
func NewTopic(name string, inMemSize int) *Topic {
	topic := &Topic{
		name:                name,
		newChannelChan:      make(chan utils.ChanReq),
		channelMap:          make(map[string]*Channel),
		incomingMessageChan: make(chan *Message),
		msgChan:             make(chan *Message, inMemSize),
		readSyncChan:        make(chan struct{}),
		routerSyncChan:      make(chan struct{}),
		exitChan:            make(chan utils.ChanReq),
	}
	go topic.Router(inMemSize)
	return topic
}

func GetTopic(name string) *Topic {
	topicChan := make(chan interface{})
	newTopicChan <- utils.ChanReq{
		Variable: name,
		RetChan:  topicChan,
	}
	return (<-topicChan).(*Topic)
}

func TopicFactory(inMemSize int) {
	var (
		topicReq utils.ChanReq
		name     string
		topic    *Topic
		ok       bool
	)
	for {
		topicReq = <-newTopicChan
		name = topicReq.Variable.(string)
		if topic, ok = TopicMap[name]; !ok {
			topic = NewTopic(name, inMemSize)
			TopicMap[name] = topic
			log.Printf("Topic %s created.\n", name)
		}
		topicReq.RetChan <- topic
	}
}

func (t *Topic) GetChannel(name string) *Channel {
	channelRet := make(chan interface{})
	t.newChannelChan <- utils.ChanReq{
		Variable: name,
		RetChan:  channelRet,
	}
	return (<-channelRet).(*Channel)
}

func (t Topic) Router(inMemSize int) {
	var (
		msg       *Message
		closeChan = make(chan struct{})
	)
	for {
		select {
		case channelReq := <-t.newChannelChan:
			channelName := channelReq.Variable.(string)
			channel, ok := t.channelMap[channelName]
			if !ok {
				channel := NewChannel(channelName, inMemSize)
				t.channelMap[channelName] = channel
				log.Printf("Topic(%s)new a Channel %s\n", t.name, channelName)
			}
			channelReq.RetChan <- channel
			if !t.channelWriteStarted {
				go t.MessagePum(closeChan)
				t.channelWriteStarted = true
			}
		case msg = <-t.incomingMessageChan:
			select {
			case t.msgChan <- msg:
				log.Printf("Topic(%s) write a Message\n", t.name)
			default:
			}
		case <-t.readSyncChan:
			<-t.routerSyncChan
		case closeReq := <-t.exitChan:
			log.Printf("Topic(%s) exit", t.name)

			for _, channel := range t.channelMap {
				err := channel.Close()
				if err != nil {
					log.Printf("Topic(%s)'s Channel(%s) close error: %v\n", t.name, channel.name, err)
				}
			}
			close(closeChan)

			closeReq.RetChan <- nil
		}
	}
}

func (t *Topic) PutMessage(msg *Message) {
	t.incomingMessageChan <- msg
}

func (t *Topic) MessagePum(closeChan <-chan struct{}) {
	var msg *Message
	for {
		select {
		case msg = <-t.msgChan:
			t.readSyncChan <- struct{}{}

			for _, channel := range t.channelMap {
				go func(channel *Channel) {
					channel.PutMessage(msg)
				}(channel)
			}

			t.routerSyncChan <- struct{}{}
		case <-closeChan:
			return

		}
	}
}

func (t *Topic) Close() error {
	errChan := make(chan interface{})
	t.exitChan <- utils.ChanReq{
		RetChan: errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}
