package server

import (
	"github.com/treeforest/gos/utils"
	"log"
	"net"
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

	// 该链接处理的方法
	router Router
}

func NewConnection(conn *net.TCPConn, connID uint32, router Router) Connection {
	c := &connection{
		conn:      conn,
		connID:    connID,
		closed:    false,
		router:    router,
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

func (c *connection) Send(data []byte) error {
	_, err := c.conn.Write(data)
	return err
}

// 链接的读业务方法
func (c *connection) startReader() {
	log.Println("Reader goroutine is running...")
	defer func() {
		log.Printf("connID=%d Reader is exist, remote addr is %s", c.connID, c.RemoteAddr().String())
		c.Stop()
	}()

	buf := make([]byte, utils.GlobalObject.MaxConn)

	for {
		_, err := c.conn.Read(buf)
		if err != nil {
			log.Fatalf("recv buf error: %v", err)
			continue
		}

		// 得到当前conn数据的Request请求数据
		req := NewRequest(c, buf)

		// 执行注册的路由方法
		go func(r Request) {
			c.router.PreHandle(r)
			c.router.Handle(r)
			c.router.PostHandle(r)
		}(req)
	}
}
