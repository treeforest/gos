package server

import (
	"fmt"
	"github.com/treeforest/gos/utils"
	"log"
	"net"
)

// 定义一个Server服务器模块
type server struct {
	// 服务器名称
	name string

	// 服务器绑定的IP版本
	tcpVersion string

	//服务器监听的IP
	ip string

	// 服务器监听的接口
	port int

	// 当前的Server注册的路由
	router Router
}

func (s *server) Serve() {
	// 启动server
	s.Start()

	// TODO 额外业务

	// 阻塞状态
	select {}
}

func (s *server) Start() {
	log.Printf("[Gos] Server[%s] listenner at IP[%s:%d] is starting.", s.name, s.ip, s.port)
	log.Printf("[Gos] Version:%s MaxConn:%d MaxPackageSize:%d",
		utils.GlobalObject.Version, utils.GlobalObject.MaxConn, utils.GlobalObject.MaxPackageSize)

	go func() {
		addr, err := net.ResolveTCPAddr(s.tcpVersion, fmt.Sprintf("%s:%d", s.ip, s.port))
		if err != nil {
			panic(fmt.Errorf("resolve tcp addr error: %v", err))
			return
		}

		listener, err := net.ListenTCP(s.tcpVersion, addr)
		if err != nil {
			panic(fmt.Errorf("listen %s error: %v", s.tcpVersion, err))
			return
		}

		log.Printf("start server %s success.", s.name)
		var cid uint32 = 0

		for {
			conn, err := listener.AcceptTCP()
			if err != nil {
				log.Fatalf("Accept tcp error: %v", err)
				continue
			}

			// 处理新链接的业务
			dealConn := NewConnection(conn, cid, s.router)
			cid++

			// 启动当前的链接业务处理
			go dealConn.Start()
		}
	}()
}

func (s *server) Stop() {

}

func (s *server) RegisterRouter(router Router) {
	s.router = router
	log.Println("Register router success.")
}

/*
	初始化Server
*/
func NewServer(serverName string) Server {
	s := &server{
		name:       utils.GlobalObject.Name,
		tcpVersion: "tcp4",
		ip:         utils.GlobalObject.Host,
		port:       utils.GlobalObject.TcpPort,
		router:     nil,
	}

	return s
}
