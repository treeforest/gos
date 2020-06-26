package client

import (
	"encoding/binary"
	"bytes"
)

func Pack(msg *message) ([]byte, error) {
	dataBuff := bytes.NewBuffer([]byte{})

	// 将数据包长度写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.dataLen); err != nil {
		return nil, err
	}

	// 将服务ID写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.serviceID); err != nil {
		return nil, err
	}

	// 将方法ID写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.methodID); err != nil {
		return nil, err
	}

	// 将消息内容写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.data); err != nil {
		return nil, err
	}

	return dataBuff.Bytes(), nil
}

func UnpackHead(binaryData []byte) (*message, error) {
	dataBuff := bytes.NewReader(binaryData)

	// 解压head信息，得到dataLen和messageID
	msg := &message{}

	// dataLen
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.dataLen); err != nil {
		return nil, err
	}

	// serviceID
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.serviceID); err != nil {
		return nil, err
	}

	// methodID
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.methodID); err != nil {
		return nil, err
	}

	return msg, nil
}