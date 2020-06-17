package main

import (
	"fmt"
	"github.com/treeforest/gos/transport"
	"io"
	"net"
	"time"
)

func main() {
	fmt.Println("client0 start...")

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		panic(fmt.Errorf("dial error: %v", err))
	}

	for {
		// 对消息封包
		pack := transport.NewDataPack()
		binaryMsg, _ := pack.Pack(transport.NewMessage(0, []byte("Hello World!")))

		_, err := conn.Write(binaryMsg)
		if err != nil {
			fmt.Printf("write error: %v\n", err)
			break
		}

		headData := make([]byte, pack.GetHeadLen())
		_, err = io.ReadFull(conn, headData)
		if err != nil {
			fmt.Println("read head error")
			break
		}

		msg, err := pack.Unpack(headData)
		if err != nil {
			fmt.Println("transport unpack head error:", err)
			break
		}

		if msg.GetLen() > 0 {
			// msg 有数据
			// 2、根据dataLen将data读出来
			data := make([]byte, msg.GetLen())

			_, err := io.ReadFull(conn, data)
			if err != nil {
				fmt.Println("transport unpack data error:", err)
			}

			msg.SetData(data)

			// 读取数据完毕
			fmt.Println("--->Recv MsgID:", msg.GetID(), ", dataLen:", msg.GetLen(), ", data:", string(msg.GetData()))
		}

		time.Sleep(time.Second * 4)
	}
}
