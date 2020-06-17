package transport

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"github.com/treeforest/gos/utils"
	"sync"
)

/*
	链接模块
*/
type connection struct {
	// 当前链接隶属于那个server
	tcpServer Server

	// 当前链接的套接字
	conn *net.TCPConn

	// 链接的ID
	connID uint32

	// 当前链接的状态
	closed bool

	// 告知当前链接已经退出/停止的channel(由reader告知writer)
	existChan chan bool

	// 无缓冲管道，用于读、写goroutine之间的消息通信
	msgChan chan []byte

	// msgID和对应的处理业务的API关系
	msgHandler MessageHandler

	// 扩展的链接属性集合
	propertyMap sync.Map
}

func NewConnection(tcpServer Server, conn *net.TCPConn, connID uint32, msgHandler MessageHandler) Connection {
	c := &connection{
		tcpServer:  tcpServer,
		conn:       conn,
		connID:     connID,
		closed:     false,
		msgHandler: msgHandler,
		existChan:  make(chan bool),
		msgChan:    make(chan []byte),
	}

	// 将conn加入到connManager中
	c.tcpServer.GetConnManager().Add(c)

	return c
}

func (c *connection) Start() {
	log.Printf("[Conn Start] ConnID = %d\n", c.connID)

	// 启动从当前链接读数据的业务
	go c.startReader()

	// 启动从当前写数据的业务
	go c.startWriter()

	// 链接之前执行的HOOk
	c.tcpServer.CallOnConnStart(c)
}

func (c *connection) Stop() {
	log.Printf("[Conn Stop] ConnID = %d\n", c.connID)
	if c.closed {
		return
	}
	c.closed = true

	// 链接结束之前调用HOOK
	c.tcpServer.CallOnConnStop(c)

	// 关闭连接
	c.conn.Close()

	// 通知writer关闭
	c.existChan <- true

	// 将当前链接从connManager中移除
	c.tcpServer.GetConnManager().Remove(c)

	// 回收资源
	close(c.existChan)
	close(c.msgChan)
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

	// 发送数据给客户端
	c.msgChan <- binaryMsg

	return nil
}

// 设置链接属性
func (c *connection) SetProperty(key string, value interface{}) {
	c.propertyMap.Store(key, value)
}

// 获取链接属性
func (c *connection) GetProperty(key string) (value interface{}, ok bool) {
	return c.propertyMap.Load(key)
}

// 移除链接属性
func (c *connection) RemoveProperty(key string) {
	c.propertyMap.Delete(key)
}


/*
	读消息的goroutine
*/
func (c *connection) startReader() {
	log.Println("[Reader goroutine is running]")
	defer func() {
		log.Printf("[Reader is exit!] connID=%d\n", c.connID)
		c.Stop()
	}()

	for {
		// 1、将包的head读出来
		pack := NewDataPack()

		headData := make([]byte, pack.GetHeadLen())
		_, err := io.ReadFull(c.GetTCPConnection(), headData)
		if err != nil {
			log.Printf("read head error: %v\n", err)
			break
		}

		msg, err := pack.Unpack(headData)
		if err != nil {
			log.Printf("transport unpack head error: %v\n", err)
			break
		}

		if msg.GetLen() > 0 {
			// msg 有数据
			// 2、根据dataLen将data读出来
			data := make([]byte, msg.GetLen())

			if _, err := io.ReadFull(c.GetTCPConnection(), data); err != nil {
				log.Printf("transport unpack data error: %v\n", err)
				break
			}

			msg.SetData(data)

			// 读取数据完毕, 交路由器处理
			req := NewRequest(c, msg)

			// 进行消息处理
			if utils.GlobalObject.WorkerPoolSize > 0 {
				// 若开启了工作池
				c.msgHandler.SendMsgToTaskQueue(req)
			} else {
				// 未开启工作池，直接一个协程进行处理
				go c.msgHandler.HandleRequest(req)
			}
		}
	}
}

/*
	写消息的goroutine
*/
func (c *connection) startWriter() {
	log.Println("[Writer goroutine is running]")
	defer func() {
		log.Printf("[Writer is exit!] connID=%d\n", c.connID)
	}()

	// 阻塞等待channel的消息，进行写给客户端
	for {
		select {
		case data := <-c.msgChan:
			// 有写数据
			if _, err := c.conn.Write(data); err != nil {
				log.Printf("Send data error: %v\n", err)
				return
			}
		case <-c.existChan:
			// 表示reader已经退出，此时writer同时结束
			return
		}
	}
}