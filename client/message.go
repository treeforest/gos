package client

import (
	"github.com/golang/protobuf/proto"
	"github.com/treeforest/gos/transport/context"
	"hash/crc32"
	"sync"
)

type message struct {
	DataLen  uint32 // 消息长度
	CheckSum uint32 // 检验和
	Data     []byte // 消息数据
	ctx      *context.Context
	once 	 sync.Once
}

func (m *message) GetServiceID() uint32 {
	return m.ctx.GetServiceId()
}

func (m *message) GetMethodID() uint32 {
	return m.ctx.GetMethodId()
}

func (m *message) GetData() []byte {
	return m.ctx.GetData()
}

func (m *message) GetContext() *context.Context {
	m.once.Do(func() {
		m.ctx = new(context.Context)
		proto.Unmarshal(m.Data, m.ctx)
	})
	return m.ctx
}

func (m *message) SetData(data []byte) {
	m.Data = data
}

func NewMessage(ctx *context.Context) *message {
	data, _ := proto.Marshal(ctx)
	return &message{
		DataLen:  uint32(len(data)),
		CheckSum: crc32.ChecksumIEEE(data),
		Data:     data,
	}
}

func NewMessage2(serviceID, methodID uint32, msg []byte) *message {
	ctx := new(context.Context)
	ctx.ServiceId = serviceID
	ctx.MethodId = methodID
	ctx.Data = msg
	data, _ := proto.Marshal(ctx)
	return &message{
		DataLen:  uint32(len(data)),
		CheckSum: crc32.ChecksumIEEE(data),
		Data:     data,
	}
}
