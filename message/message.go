package message

type Message struct {
	data []byte
	//定义消息,前十六位为uid，标识唯一消息，后面内容为消息本身
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
