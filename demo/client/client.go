package main

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/treeforest/gos/client"
	"github.com/treeforest/gos/demo/pb"
	"github.com/treeforest/logger"
	"hash/crc32"
	"io"
	"net"
	"time"
)

func write(conn net.Conn) {
	for {
		req := new(demo.HelloRequest)
		req.Name = "tony"
		msg, _ := proto.Marshal(req)
		binaryMsg, _ := client.Pack(client.NewMessage2(uint32(demo.ServiceID_demo), uint32(demo.Event_Hello), msg))

		_, err := conn.Write(binaryMsg)
		if err != nil {
			fmt.Printf("write error: %v\n", err)
			break
		}

		time.Sleep(time.Second * 4)
	}
}

func read(conn net.Conn) {
	var cnt int32 = 0
	for {
		cnt++

		headData := make([]byte, 8)
		_, err := io.ReadFull(conn, headData)
		if err != nil {
			fmt.Println("read head error")
			break
		}

		msg, err := client.UnpackHead(headData)
		if err != nil {
			fmt.Println("transport unpack head error:", err)
			break
		}

		if msg.DataLen > 0 {
			// msg 有数据
			// 根据dataLen将data读出来
			data := make([]byte, msg.DataLen)
			_, err := io.ReadFull(conn, data)
			if err != nil {
				fmt.Println("unpack data error:", err)
				break
			}
			msg.SetData(data)

			// 校验和检测
			if msg.CheckSum != crc32.ChecksumIEEE(msg.Data) {
				log.Errorf("Checksum error.")
				continue
			}

			resp := new(demo.HelloResponse)
			json.Unmarshal(msg.GetContext().GetData(), resp)

			// 读取数据完毕
			fmt.Println(cnt, "--->Recv serviceID:", msg.GetServiceID(), " methodID:", msg.GetMethodID(), ", dataLen:", msg.DataLen, ", resp:", resp)
		}
	}

}

func main() {
	fmt.Println("client start...")

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		panic(fmt.Errorf("dial error: %v", err))
	}

	go read(conn)

	write(conn)
}