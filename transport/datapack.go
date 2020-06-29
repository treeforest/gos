package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/treeforest/gos/config"
	"github.com/treeforest/logger"
	"sync"
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
	// dataLen uint32 (4字节) + checkSum uint32 (4字节)
	return 8
}

// 封包方法
func (p dataPack) Pack(msg Message) ([]byte, error) {
	dataBuff := bytes.NewBuffer([]byte{})

	// 将数据包长度写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetLen()); err != nil {
		return nil, err
	}

	// 将校验码写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetCheckSum()); err != nil {
		return nil, err
	}

	// 将消息内容写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.GetData()); err != nil {
		return nil, err
	}

	return dataBuff.Bytes(), nil
}

// 拆包方法
func (p dataPack) Unpack(binaryData []byte, m Message) error {
	dataBuff := bytes.NewReader(binaryData)

	// 解压head信息，得到dataLen和messageID
	msg := m.(*message)

	// dataLen
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.dataLen); err != nil {
		log.Warnf("Unpack dataLen error.")
		return err
	}

	// checkSum
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.checkSum); err != nil {
		log.Warnf("Unpack serviceID error.")
		return err
	}

	// 判断dataLen是否符合要求的最大包长度
	if config.ServerConfig.MaxPackageSize < msg.GetLen() {
		log.Warnf("MaxPackageSize: %d , msg: %v\n", config.ServerConfig.MaxPackageSize, msg)
		return errors.New("too large msg data recv!")
	}

	return nil
}
