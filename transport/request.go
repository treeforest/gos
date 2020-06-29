package transport

import (
	"github.com/golang/protobuf/proto"
	"github.com/treeforest/gos/transport/context"
	"github.com/treeforest/logger"
)

type request struct {
	// 已经和客户建立好的链接
	conn Connection

	// 请求上下文
	ctx *context.Context
}

func (r *request) SetRequest(conn Connection, data []byte) (req Request, err error) {
	r.conn = conn

	r.ctx = globalPool.GetContext()
	if err := proto.Unmarshal(data, r.ctx); err != nil {
		log.Errorf("proto Unmarshal context error: %v", err)
		return nil, err
	}

	return r, nil
}

// 得到当前链接
func (r *request) GetConnection() Connection {
	return r.conn
}

// 得到请求的上下文
func (r *request) GetContext() *context.Context {
	return r.ctx
}

// 获取服务ID
func (r *request) GetServiceID() uint32 {
	return r.ctx.GetServiceId()
}

// 获取方法ID
func (r *request) GetMethodID() uint32 {
	return r.ctx.GetMethodId()
}

// 获取session
func (r *request) GetSession() uint32 {
	return r.ctx.GetSession()
}
