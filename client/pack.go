package client

import (
	"bytes"
	"encoding/binary"
)

func Pack(msg *message) ([]byte, error) {
	dataBuff := bytes.NewBuffer([]byte{})

	// 将数据包长度写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.DataLen); err != nil {
		return nil, err
	}

	// 将校验码写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.CheckSum); err != nil {
		return nil, err
	}

	// 将消息内容写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.Data); err != nil {
		return nil, err
	}

	return dataBuff.Bytes(), nil
}

func UnpackHead(binaryData []byte) (*message, error) {
	dataBuff := bytes.NewReader(binaryData)

	// 解压head信息，得到dataLen和messageID
	msg := &message{}

	// dataLen
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.DataLen); err != nil {
		return nil, err
	}

	// checkSum
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.CheckSum); err != nil {
		return nil, err
	}

	return msg, nil
}
