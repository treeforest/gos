package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/treeforest/gos/demo/pb"
	"github.com/treeforest/gos/transport"
	"github.com/treeforest/logger"
	"encoding/json"
	)

// 逻辑实现
type Logic struct {}

// Test Handle
func (l *Logic) Hello(req *demo.HelloRequest) *demo.HelloResponse{
	log.Debugf("Name: %s", req.Name)
	resp := new(demo.HelloResponse)
	resp.Ret = fmt.Sprintf("Hello %s.", req.Name)
	return resp
}

// 实例句柄
var m_handle = &Logic{}

// 路由实现
type HelloRouter struct {
	transport.BaseRouter
}

func (r *HelloRouter) PreHandle(req transport.Request) {
	log.Debug("PreHandle")
}

// Test Handle
func (r *HelloRouter) Handle(req transport.Request) {
	// 读取客户端的数据
	log.Debugf("serviceID=%d, methodID=%d", req.GetServiceID(), req.GetMethodID())

	switch req.GetMethodID() {
	case uint32(demo.Event_Hello):
		var resp *demo.HelloResponse
		r := new(demo.HelloRequest)

		if err := proto.Unmarshal(req.GetContext().GetData(), r); err != nil {
			log.Error("Say request unmarshal error: ", err)
			resp = new(demo.HelloResponse)
			resp.Ret = "SayRequest unmarshal error."
		} else {
			resp = m_handle.Hello(r)
		}

		data, _ := json.Marshal(resp)
		req.GetConnection().Send(req.GetContext(), data)
	}
}

func (r *HelloRouter) PostHandle(req transport.Request) {
	log.Debug("PostHandle")
}

func OnConnStart(c transport.Connection) {
	log.Debug("OnConnStart")
}

func OnConnStop(c transport.Connection) {
	log.Debug("OnConnStop")
}

func main() {
	log.SetFileLogger()

	s := transport.NewServer("[Demo]")

	s.SetOnConnStartFunc(OnConnStart)
	s.SetOnConnStopFunc(OnConnStop)

	// 添加router
	s.RegisterRouter(uint32(demo.ServiceID_demo), &HelloRouter{})

	s.Serve()
}
