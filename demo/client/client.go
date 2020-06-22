package main

import (
	"fmt"
	"io"
	"net"
		"bytes"
	"encoding/binary"
	"encoding/json"
	"time"
)

type SayRequest struct {
	Name string `json:"Name"`
}

type PlayRequest struct {
	Ball string `json:"Ball"`
}

type Response struct {
	Res string `json:"Result"`
}

// 服务ID
const CODE_HELLO = 101

// 服务方法对应的ID
const (
	EVENT_SAY = iota
	EVENT_PLAY
)

func main() {
	fmt.Println("client start...")

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		panic(fmt.Errorf("dial error: %v", err))
	}

	var ok bool = true
	var cnt int = 0

	for {
		cnt++
		var binaryMsg []byte
		if ok {
			r := SayRequest{}
			r.Name = "Tony"
			data, _ := json.Marshal(r)
			binaryMsg, _ = Pack(NewMessage(CODE_HELLO, EVENT_SAY, data))// 对消息封包
		} else {
			r := PlayRequest{}
			r.Ball = "badminton"
			data, _ := json.Marshal(r)
			binaryMsg, _ = Pack(NewMessage(CODE_HELLO, EVENT_PLAY, data))// 对消息封包
		}
		ok = !ok

		_, err := conn.Write(binaryMsg)
		if err != nil {
			fmt.Printf("write error: %v\n", err)
			break
		}

		headData := make([]byte, 12)
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

			resp := &Response{}
			json.Unmarshal(data, resp)

			// 读取数据完毕
			fmt.Println(cnt, "--->Recv serviceID:", msg.serviceID, " methodID:", msg.methodID, ", dataLen:", msg.dataLen, ", resp:", resp)
		}

		time.Sleep(time.Second * 4)
	}
}

type message struct {
	dataLen   uint32 // 消息长度
	serviceID uint32 // 服务ID
	methodID  uint32 // 方法ID
	data      []byte // 消息内容
}

func (m *message) SetData(data []byte) {
	m.data = data
}

func NewMessage(serviceID, methodID uint32, data []byte) *message {
	return &message{
		dataLen:   uint32(len(data)),
		serviceID: serviceID,
		methodID:  methodID,
		data:      data,
	}
}

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

func Unpack(binaryData []byte) (*message, error) {
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