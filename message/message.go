package message

type Message struct {
	//定义消息,前十六位为uid，标识唯一消息，后面内容为消息本身
	data []byte
	//用于终止计时逻辑
	timeChan chan struct{}
}

func NewMessage(data []byte) *Message {
	return &Message{data: data}
}

func (m *Message) Uuid() []byte {
	return m.data[:16]
}

func (m *Message) Body() []byte {
	return m.data[16:]
}

func (m *Message) Data() []byte {
	return m.data
}

func (m *Message) EndTime() {
	select {
	case m.timeChan <- struct{}{}:
	default:
	}
}
