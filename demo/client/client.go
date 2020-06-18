package main

import (
	"fmt"
	"io"
	"net"
	"time"
	"bytes"
	"encoding/binary"
	)

func main() {
	fmt.Println("client0 start...")

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		panic(fmt.Errorf("dial error: %v", err))
	}

	for {
		// 对消息封包
		binaryMsg, _ := Pack(NewMessage(0, []byte("Hello World!")))

		_, err := conn.Write(binaryMsg)
		if err != nil {
			fmt.Printf("write error: %v\n", err)
			break
		}

		headData := make([]byte, 8)
		_, err = io.ReadFull(conn, headData)
		if err != nil {
			fmt.Println("read head error")
			break
		}

		msg, err := Unpack(headData)
		if err != nil {
			fmt.Println("transport unpack head error:", err)
			break
		}

		if msg.dataLen > 0 {
			// msg 有数据
			// 2、根据dataLen将data读出来
			data := make([]byte, msg.dataLen)

			_, err := io.ReadFull(conn, data)
			if err != nil {
				fmt.Println("transport unpack data error:", err)
			}

			msg.SetData(data)

			// 读取数据完毕
			fmt.Println("--->Recv MsgID:", msg.msgID, ", dataLen:", msg.dataLen, ", data:", string(msg.data))
		}

		time.Sleep(time.Second * 4)
	}
}

type message struct {
	msgID   uint32 //消息ID
	dataLen uint32 // 消息长度
	data    []byte //消息内容
}

func (m *message) SetData(data []byte) {
	m.data = data
}

func NewMessage(msgID uint32, data []byte) *message {
	return &message{
		msgID:msgID,
		dataLen:uint32(len(data)),
		data:data,
	}
}

func Pack(msg *message) ([]byte, error) {
	dataBuff := bytes.NewBuffer([]byte{})

	// 将数据包长度写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.dataLen); err != nil {
		return nil, err
	}

	// 将消息ID写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.msgID); err != nil {
		return nil, err
	}

	// 将消息内容写入数据包
	if err := binary.Write(dataBuff, binary.LittleEndian, msg.data); err != nil {
		return nil, err
	}

	return dataBuff.Bytes(), nil
}

func Unpack(binaryData []byte) (*message, error) {
	dataBuff := bytes.NewReader(binaryData)

	// 解压head信息，得到dataLen和messageID
	msg := &message{}

	// dataLen
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.dataLen); err != nil {
		return nil, err
	}

	// messageID
	if err := binary.Read(dataBuff, binary.LittleEndian, &msg.msgID); err != nil {
		return nil, err
	}

	return msg, nil
}