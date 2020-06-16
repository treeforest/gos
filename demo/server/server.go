package main

import (
	"fmt"
	"github.com/treeforest/gos/server"
)

type PingRouter struct {
	server.BaseRouter
}

// Test PreHandle
func (r *PingRouter) PreHandle(req server.Request) {
	fmt.Println("Call Router PreHandle...")
	_, err := req.GetConnection().GetTCPConnection().Write([]byte("before ping..."))
	if err != nil {
		fmt.Println("call back before ping error:", err)
	}
}

// Test Handle
func (r *PingRouter) Handle(req server.Request) {
	fmt.Println("Call Router Handle...")
	_, err := req.GetConnection().GetTCPConnection().Write([]byte("ping..."))
	if err != nil {
		fmt.Println("call back ping error:", err)
	}
}

// Test PostHandle
func (r *PingRouter) PostHandle(req server.Request) {
	fmt.Println("Call Router PostHandle...")
	_, err := req.GetConnection().GetTCPConnection().Write([]byte("after ping..."))
	if err != nil {
		fmt.Println("call back after ping error:", err)
	}
}

func main() {
	s := server.NewServer("[lsgo V0.1]")

	// 添加router
	s.RegisterRouter(&PingRouter{})

	s.Serve()
}
