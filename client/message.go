package client

type message struct {
	dataLen   uint32 // 消息长度
	serviceID uint32 // 服务ID
	methodID  uint32 // 方法ID
	data      []byte // 消息内容
}

func (m *message)GetServiceID() uint32 {
	return m.serviceID
}

func (m *message)GetMethodID() uint32 {
	return m.methodID
}

func (m *message)GetData() []byte {
	return m.data
}

func NewMessage(serviceID, methodID uint32, data []byte) *message {
	return &message{
		dataLen:   uint32(len(data)),
		serviceID: serviceID,
		methodID:  methodID,
		data:      data,
	}
}
