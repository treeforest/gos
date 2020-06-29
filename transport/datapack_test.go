package transport

import (
	"github.com/golang/protobuf/proto"
	"github.com/treeforest/gos/transport/context"
	"hash/crc32"
	"io"
	"net"
	"testing"
	"time"
)

// 测试封包、拆包
func TestDataPack(t *testing.T) {
	/*
		模拟服务端
	*/
	listener, err := net.Listen("tcp", "127.0.0.1:7777")
	if err != nil {
		t.Errorf("transport listen error: %v", err)
		return
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				t.Errorf("transport accept error: %v", err)
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
						t.Error("read head error")
						break
					}

					msg := new(message)
					err = pack.Unpack(headData, msg)
					if err != nil {
						t.Errorf("transport unpack head error: %v", err)
						return
					}

					if msg.GetLen() > 0 {
						// msg 有数据
						// 2、根据dataLen将data读出来
						msg.data = make([]byte, msg.GetLen())

						_, err := io.ReadFull(conn, msg.data)
						if err != nil {
							t.Errorf("transport unpack data error: %v", err)
						}

						if !msg.ChecksumIEEE() {
							t.Error("Checksum error.")
						}

						ctx := new(context.Context)
						proto.Unmarshal(msg.GetData(), ctx)

						// 读取数据完毕
						t.Logf("--->Recv context: %v", ctx)
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
		t.Errorf("client dial error: %v", err)
		return
	}

	pack := NewDataPack()

	// 模拟粘包过程,封装两个msg一同发送
	ctx1 := new(context.Context)
	ctx1.ServiceId = 1
	ctx1.MethodId = 2
	ctx1.Data = []byte{'h', 'e', 'l', 'l', 'o'}
	data1, _ := proto.Marshal(ctx1)
	msg1 := &message{
		dataLen:  uint32(len(data1)),
		checkSum: crc32.ChecksumIEEE(data1),
		data:     data1,
	}
	buf1, err := pack.Pack(msg1)
	if err != nil {
		t.Errorf("client pack msg1 error: %v", err)
		return
	}

	ctx2 := new(context.Context)
	ctx2.ServiceId = 2
	ctx2.MethodId = 12
	ctx2.Data = []byte{'w', 'o', 'r', 'l', 'd'}
	data2, _ := proto.Marshal(ctx2)
	msg2 := &message{
		dataLen:  uint32(len(data2)),
		checkSum: crc32.ChecksumIEEE(data2),
		data:     data2,
	}
	buf2, err := pack.Pack(msg2)
	if err != nil {
		t.Errorf("client pack msg2 error: %v", err)
		return
	}

	// 模拟粘包
	buf := append(buf1, buf2...)

	// 一次性写
	conn.Write(buf)

	// 阻塞
	select {}
}
