package transport

import (
	"github.com/golang/protobuf/proto"
	"github.com/treeforest/gos/transport/context"
	"hash/crc32"
)

type message struct {
	dataLen  uint32 // 消息长度
	checkSum uint32 // 校验和
	data     []byte // 消息内容
}

func (m *message) Reset(ctx *context.Context) {
	data, _ := proto.Marshal(ctx)
	m.dataLen = uint32(len(data))
	m.checkSum = crc32.ChecksumIEEE(data)
	m.data = data
}

// 获取消息的长度
func (m *message) GetLen() uint32 {
	return m.dataLen
}

// 获取校验码
func (m *message) GetCheckSum() uint32 {
	return m.checkSum
}

//获取消息的内容
func (m *message) GetData() []byte {
	return m.data
}

//设置消息的长度
func (m *message) SetLen(nLen uint32) {
	m.dataLen = nLen
}

// 设置校验码
func (m *message) SetCheckSum(checkSum uint32) {
	m.checkSum = checkSum
}

// 设置消息的内容
func (m *message) SetData(data []byte) {
	m.data = data
}

// 检测检验和
func (m *message) ChecksumIEEE() bool {
	return m.checkSum == crc32.ChecksumIEEE(m.data)
}
