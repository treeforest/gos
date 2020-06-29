package transport

import (
	"github.com/treeforest/gos/transport/context"
	"net"
	"sync"
)

/*
 * 全局临时对象池
 */
var globalPool *pool = newPool()

type pool struct {
	tcpConnPool sync.Pool //TCP 链接套接字临时对象池
	connPool    sync.Pool //链接临时对象池
	requestPool sync.Pool //请求临时对象池
	contextPool sync.Pool //上下文临时对象池
	messagePool sync.Pool //消息临时对象池
}

func newPool() *pool {
	p := new(pool)
	p.tcpConnPool = sync.Pool{
		New: func() interface{} {
			return new(net.TCPConn)
		},
	}
	p.connPool = sync.Pool{
		New: func() interface{} {
			return new(connection)
		},
	}
	p.requestPool = sync.Pool{
		New: func() interface{} {
			return new(request)
		},
	}
	p.contextPool = sync.Pool{
		New: func() interface{} {
			return new(context.Context)
		},
	}
	p.messagePool = sync.Pool{
		New: func() interface{} {
			return new(message)
		},
	}
	return p
}

func (p *pool) GetTCPConn() *net.TCPConn {
	return p.tcpConnPool.Get().(*net.TCPConn)
}

func (p *pool) PutTCPConn(c *net.TCPConn) {
	p.tcpConnPool.Put(c)
}

func (p *pool) GetConnection() *connection {
	return p.connPool.Get().(*connection)
}

func (p *pool) PutConnection(c *connection) {
	p.connPool.Put(c)
}

func (p *pool) GetRequest() *request {
	return p.requestPool.Get().(*request)
}

func (p *pool) PutRequest(r *request) {
	p.requestPool.Put(r)
}

func (p *pool) GetContext() *context.Context {
	return p.contextPool.Get().(*context.Context)
}

func (p *pool) PutContext(c *context.Context) {
	p.contextPool.Put(c)
}

func (p *pool) GetMessage() *message {
	return p.messagePool.Get().(*message)
}

func (p *pool) PutMessage(m *message) {
	p.messagePool.Put(m)
}
