package transport

type message struct {
	dataLen   uint32 // 消息长度
	serviceID uint32 //消息ID
	methodID  uint32 // 方法ID
	data      []byte //消息内容
}

func NewMessage(serviceID, methodID uint32, data []byte) Message {
	return &message{
		dataLen:   uint32(len(data)),
		serviceID: serviceID,
		methodID:  methodID,
		data:      data,
	}
}

// 获取消息的长度
func (m *message) GetLen() uint32 {
	return m.dataLen
}

// 获取服务的ID
func (m *message) GetServiceID() uint32 {
	return m.serviceID
}

// 获取服务对应方法的ID
func (m *message) GetMethodID() uint32 {
	return m.methodID
}

//获取消息的内容
func (m *message) GetData() []byte {
	return m.data
}

//设置消息的长度
func (m *message) SetLen(nLen uint32) {
	m.dataLen = nLen
}

// 设置服务的ID
func (m *message) SetServiceID(serviceID uint32) {
	m.serviceID = serviceID
}

// 设置服务对应方法的ID
func (m *message) SetMethodID(methodID uint32) {
	m.methodID = methodID
}

// 设置消息的内容
func (m *message) SetData(data []byte) {
	m.data = data
}
