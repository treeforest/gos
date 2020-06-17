package transport

import (
	"testing"
	"net"
	"fmt"
	"io"
	"time"
)

// 测试封包、拆包
func TestDataPack(t *testing.T) {
	/*
		模拟服务端
	 */
	listener, err := net.Listen("tcp", "127.0.0.1:7777")
	if err != nil {
		fmt.Println("transport listen error:", err)
		return
	}

	go func() {
		for  {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("transport accept error", err)
				continue
			}

			go func(conn net.Conn) {
				// 拆包过程
				pack := NewDataPack()
				for {
					// 1、将包的head读出来
					headData := make([]byte, pack.GetHeadLen())
					_, err := io.ReadFull(conn, headData)
					if err != nil {
						fmt.Println("read head error")
						break
					}

					msgHead, err := pack.Unpack(headData)
					if err != nil {
						fmt.Println("transport unpack head error:", err)
						return
					}

					if msgHead.GetLen() > 0 {
						// msg 有数据
						// 2、根据dataLen将data读出来
						msg := msgHead.(*message)
						msg.data = make([]byte, msg.GetLen())

						_, err := io.ReadFull(conn, msg.data)
						if err != nil {
							fmt.Println("transport unpack data error:", err)
						}

						// 读取数据完毕
						fmt.Println("--->Recv MsgID:", msg.msgID, ", dataLen:", msg.dataLen, ", data:", string(msg.data))
					}
				}
			}(conn)
		}
	}()

	time.Sleep(time.Second * 2)
	/*
		模拟客户端
	 */
	conn, err := net.Dial("tcp", "127.0.0.1:7777")
	if err != nil {
	fmt.Println("client dial error:", err)
	 return
	}

	pack := NewDataPack()

	// 模拟粘包过程,封装两个msg一同发送
	msg1 := &message{
		msgID:1,
		dataLen:5,
		data:[]byte{'h','e','l','l','o'},
	}
	data, err := pack.Pack(msg1)
	if err != nil {
		fmt.Println("client pack msg1 error:", err)
		return
	}

	msg2 := &message{
		msgID:2,
		dataLen:5,
		data:[]byte{'w','o','r','l','d'},
	}
	data2, err := pack.Pack(msg2)
	if err != nil {
		fmt.Println("client pack msg2 error:", err)
		return
	}

	// 模拟粘包
	data = append(data, data2...)

	// 一次性写
	conn.Write(data)

	// 阻塞
	select {}
}