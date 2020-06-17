package transport

import (
	"log"
	"net"
	"errors"
	"io"
	"fmt"
)

/*
	链接模块
*/
type connection struct {
	// 当前链接的套接字
	conn *net.TCPConn

	// 链接的ID
	connID uint32

	// 当前链接的状态
	closed bool

	// 告知当前链接已经退出/停止的channel
	existChan chan bool

	// msgID和对应的处理业务的API关系
	msgHandler MessageHandler
}

func NewConnection(conn *net.TCPConn, connID uint32, msgHandler MessageHandler) Connection {
	c := &connection{
		conn:      conn,
		connID:    connID,
		closed:    false,
		msgHandler:    msgHandler,
		existChan: make(chan bool),
	}
	return c
}

func (c *connection) Start() {
	log.Printf("Conn Start()... ConnID = %d", c.connID)
	// 启动从当前链接读数据的业务
	go c.startReader()
	// TODO：启动从当前写数据的业务
}

func (c *connection) Stop() {
	log.Printf("Conn Stop()... ConnID = %d", c.connID)
	if c.closed {
		return
	}
	c.closed = true

	// 关闭连接
	c.conn.Close()

	// 回收资源
	close(c.existChan)
}

func (c *connection) GetTCPConnection() *net.TCPConn {
	return c.conn
}

func (c *connection) GetConnID() uint32 {
	return c.connID
}

func (c *connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *connection) Send(msgID uint32, data []byte) error {
	if c.closed {
		return errors.New("Send error: connection closed when send message.")
	}

	// 封包处理
	pack := NewDataPack()
	binaryMsg, err := pack.Pack(NewMessage(msgID, data))
	if err != nil {
		return fmt.Errorf("Send error: pack failed, %v", err)
	}

	if _, err := c.conn.Write(binaryMsg); err != nil {
		return fmt.Errorf("Send error: conn write failed, %v", err)
	}

	return nil
}

// 链接的读业务方法
func (c *connection) startReader() {
	log.Println("Reader goroutine is running...")
	defer func() {
		log.Printf("connID=%d Reader is exist, remote addr is %s", c.connID, c.RemoteAddr().String())
		c.Stop()
	}()

	for {
		// 1、将包的head读出来
		pack := NewDataPack()

		headData := make([]byte, pack.GetHeadLen())
		_, err := io.ReadFull(c.GetTCPConnection(), headData)
		if err != nil {
			log.Fatalf("read head error: %v", err)
			break
		}

		msg, err := pack.Unpack(headData)
		if err != nil {
			log.Fatalf("transport unpack head error: %v", err)
			break
		}

		if msg.GetLen() > 0 {
			// msg 有数据
			// 2、根据dataLen将data读出来
			data := make([]byte, msg.GetLen())

			if _, err := io.ReadFull(c.GetTCPConnection(), data); err != nil {
				log.Fatalf("transport unpack data error: %v", err)
				break
			}

			msg.SetData(data)

			// 读取数据完毕, 交路由器处理
			req := NewRequest(c, msg)

			// 根据绑定好的msgID进行对应的处理
			go c.msgHandler.Do(req)
		}
	}
}