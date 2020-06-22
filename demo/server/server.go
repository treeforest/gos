package main

import (
	"fmt"
	"github.com/treeforest/gos/transport"
	"github.com/treeforest/logger"
	"encoding/json"
	)

type SayRequest struct {
	Name string `json:"Name"`
}

type PlayRequest struct {
	Ball string `json:"Ball"`
}

type Response struct {
	Res string `json:"Result"`
}

// 逻辑实现
type Logic struct {}

// Test Handle
func (l *Logic) Say(req *SayRequest) *Response{
	log.Debug("Name: ", req.Name)

	resp := new(Response)
	resp.Res = fmt.Sprintf("Hello %s.", req.Name)
	return resp
}

func (l *Logic) Play(req *PlayRequest) *Response {
	log.Debug("Ball: ", req.Ball)

	resp := new(Response)
	resp.Res = fmt.Sprintf("Play %s.", req.Ball)
	return resp
}

// 服务ID
const CODE_HELLO = 101

// 服务方法对应的ID
const (
	EVENT_SAY = iota
	EVENT_PLAY
)

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
	log.Debug("serviceID=%d, methodID=%d, data=%s\n", req.GetServiceID(), req.GetMethodID(), string(req.GetData()))

	switch req.GetMethodID() {
	case EVENT_SAY:
		var resp *Response
		sayReq := &SayRequest{}

		if err := json.Unmarshal(req.GetData(), sayReq); err != nil {
			log.Error("Say request unmarshal error: ", err)
			resp = new(Response)
			resp.Res = "SayRequest unmarshal error."
		} else {
			resp = m_handle.Say(sayReq)
		}

		data, _ := json.Marshal(resp)
		req.GetConnection().Send(req.GetServiceID(), req.GetMethodID(), data)

	case EVENT_PLAY:
		var resp *Response
		playReq := &PlayRequest{}

		if err := json.Unmarshal(req.GetData(), playReq); err != nil {
			log.Error("PlayRequest unmarshal error: ", err)
			resp = new(Response)
			resp.Res = "PlayRequest unmarshal error."
		} else {
			resp = m_handle.Play(playReq)
		}

		data, _ := json.Marshal(resp)
		req.GetConnection().Send(req.GetServiceID(), req.GetMethodID(), data)
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

	s := transport.NewServer("[lsgo V0.1]")

	s.SetOnConnStartFunc(OnConnStart)
	s.SetOnConnStopFunc(OnConnStop)

	// 添加router
	s.RegisterRouter(CODE_HELLO, &HelloRouter{})

	s.Serve()
}
