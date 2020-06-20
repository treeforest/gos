package main

import (
	"fmt"
	"github.com/treeforest/gos/transport"
	"github.com/treeforest/logger"
)

type PingRouter struct {
	transport.BaseRouter
}

// Test Handle
func (r *PingRouter) Handle(req transport.Request) {
	fmt.Println("Call PingRouter Handle...")
	// 读取客户端的数据
	fmt.Printf("msgID=%d, data=%s - %v\n", req.GetMsgID(), string(req.GetData()), req.GetData())

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

func OnConnStart(c transport.Connection) {
	fmt.Println("==> OnConnStart")
	if err := c.Send(202, []byte("OnConnStart Begin.")); err != nil {
		fmt.Println(err)
	}

	// 设置一些链接属性
	c.SetProperty("Name", "treeforest")
	c.SetProperty("Home", "github.com/treeforest")
}

func OnConnStop(c transport.Connection) {
	fmt.Println("==> OnConnStop")
	if err := c.Send(202, []byte("OnConnStop End.")); err != nil {
		fmt.Println(err)
	}

	// 获取链接属性
	v, _ := c.GetProperty("Name")
	fmt.Printf("Name = %s\n", v.(string))
	v, _ = c.GetProperty("Home")
	fmt.Printf("Name = %s\n", v.(string))
}

func main() {
	log.SetFileLogger()

	s := transport.NewServer("[lsgo V0.1]")

	s.SetOnConnStartFunc(OnConnStart)
	s.SetOnConnStopFunc(OnConnStop)

	// 添加router
	s.RegisterRouter(0, &PingRouter{})
	s.RegisterRouter(1, &HelloRouter{})

	s.Serve()
}
