package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"github.com/treeforest/logger"
	"github.com/treeforest/gos/config"
)

// 封包、拆包的具体模块
type dataPack struct{}

var pack_once sync.Once
var global_pack DataPacker

func NewDataPack() DataPacker {
	pack_once.Do(func() {
		global_pack = new(dataPack)
	})
	return global_pack
}

// 获取数据包长度
func (p dataPack) GetHeadLen() uint32 {
	// DataLen uint32 (4字节) + serviceID uint32 （4 字节） + methodID uint32 (4字节)
	return 12
}

// 封包方法
func (p dataPack) Pack(msg Message) ([]byte, error) {
	dataBuff := bytes.NewBuffer([]byte{})

	// 将数据包长度写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetLen()); err != nil {
		return nil, err
	}

	// 将服务ID写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetServiceID()); err != nil {
		return nil, err
	}

	// 将方法ID写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetMethodID()); err != nil {
		return nil, err
	}

	// 将消息内容写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetData()); err != nil {
		return nil, err
	}

	return dataBuff.Bytes(), nil
}

// 拆包方法
func (p dataPack) Unpack(binaryData []byte) (Message, error) {
	dataBuff := bytes.NewReader(binaryData)

	// 解压head信息，得到dataLen和messageID
	msg := &message{}

	// dataLen
	if err := binary.Read(dataBuff, binary.LittleEndian, msg.GetLen()); err != nil {
		return nil, err
	}

	// serviceID
	if err := binary.Read(dataBuff, binary.LittleEndian, msg.GetServiceID()); err != nil {
		return nil, err
	}

	// methodID
	if err := binary.Read(dataBuff, binary.LittleEndian, msg.GetMethodID()); err != nil {
		return nil, err
	}

	// 判断dataLen是否符合要求的最大包长度
	if config.ServerConfig.MaxPackageSize < msg.GetLen() {
		log.Warnf("MaxPackageSize: %d , msg: %v\n", config.ServerConfig.MaxPackageSize, msg)
		return nil, errors.New("too large msg data recv!")
	}

	return msg, nil
}
