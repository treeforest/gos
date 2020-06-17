package transport

type message struct {
	msgID   uint32 //消息ID
	dataLen uint32 // 消息长度
	data    []byte //消息内容
}

func NewMessage(id uint32, data []byte) Message {
	return &message{
		msgID:   id,
		dataLen: uint32(len(data)),
		data:    data,
	}
}

// 获取消息的ID
func (m *message) GetID() uint32 {
	return m.msgID
}

// 获取消息的长度
func (m *message) GetLen() uint32 {
	return m.dataLen
}

//获取消息的内容
func (m *message) GetData() []byte {
	return m.data
}

// 设置消息ID
func (m *message) SetID(id uint32) {
	m.msgID = id
}

//设置消息的长度
func (m *message) SetLen(nLen uint32) {
	m.dataLen = nLen
}

// 设置消息的内容
func (m *message) SetData(data []byte) {
	m.data = data
}
