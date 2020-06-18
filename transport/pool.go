package transport

import (
	"sync"
	"net"
)

/*
	TCP 链接套接字临时对象池
 */
var GlobalTCPConnPool *tcpConnPool = &tcpConnPool{
	pool: sync.Pool{
		New: func() interface{} {
			return new(net.TCPConn)
		},
	},
}

type tcpConnPool struct {
	pool sync.Pool
}

func (p *tcpConnPool) Get() *net.TCPConn {
	return p.pool.Get().(*net.TCPConn)
}

func (p *tcpConnPool) Put(c *net.TCPConn) {
	p.pool.Put(c)
}

/*
	链接临时对象池
 */
var GlobalConnectionPool *connectionPool = &connectionPool{
	pool:sync.Pool {
		New: func() interface{} {
			return new(connection)
		},
	},
}

type connectionPool struct {
	pool sync.Pool
}

func (p *connectionPool) Get() *connection {
	return p.pool.Get().(*connection)
}

func (p *connectionPool) Put(c *connection) {
	p.pool.Put(c)
}

/*
	请求模块临时对象池
 */
var GlobalRequestPool *requestPool = &requestPool {
	pool:sync.Pool {
		New: func() interface{} {
			return new(request)
		},
	},
}

type requestPool struct {
	pool sync.Pool
}

func (p *requestPool) Get() *request {
	return p.pool.Get().(*request)
}

func (p *requestPool) Put(c *request) {
	p.pool.Put(c)
}