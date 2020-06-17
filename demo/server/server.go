package main

import (
	"fmt"
	"github.com/treeforest/gos/transport"
)

type PingRouter struct {
	transport.BaseRouter
}

// Test Handle
func (r *PingRouter) Handle(req transport.Request) {
	fmt.Println("Call PingRouter Handle...")
	// 读取客户端的数据
	fmt.Printf("msgID=%d, data=%s\n", req.GetMsgID(), string(req.GetData()))

	err := req.GetConnection().Send(200, []byte("ping...ping..."))
	if err != nil {
		fmt.Println("call back ping error:", err)
	}
}

type HelloRouter struct {
	transport.BaseRouter
}

// Test Handle
func (r *HelloRouter) Handle(req transport.Request) {
	fmt.Println("Call HelloRouter Handle...")
	// 读取客户端的数据
	fmt.Printf("msgID=%d, data=%s\n", req.GetMsgID(), string(req.GetData()))

	err := req.GetConnection().Send(201, []byte("Hello world..."))
	if err != nil {
		fmt.Println("call back ping error:", err)
	}
}

func main() {
	s := transport.NewServer("[lsgo V0.1]")

	// 添加router
	s.RegisterRouter(0, &PingRouter{})
	s.RegisterRouter(1, &HelloRouter{})

	s.Serve()
}
