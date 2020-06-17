package transport

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
	port uint32

	// 当前的Server消息管理模块，绑定msgID与对应的业务api关系
	msgHandler MessageHandler

	// 该server的连接管理器
	connMgr ConnManager

	// 在Server创建链接之前调用
	onConnStart func(conn Connection)

	// 在Server销毁链接之后调用
	onConnStop func(conn Connection)
}

func (s *server) Serve() {
	// 启动server
	s.Start()

	// TODO 额外业务

	// 阻塞状态
	select {}
}

func (s *server) Start() {
	log.Printf("[START] Server[%s] listenner at IP[%s:%d] is starting.\n", s.name, s.ip, s.port)
	log.Printf("[START] Version:%s MaxConn:%d MaxPackageSize:%d\n",
		utils.GlobalObject.Version, utils.GlobalObject.MaxConn, utils.GlobalObject.MaxPackageSize)

	// 开启消息队列及工作池(WorkerPool)
	s.msgHandler.StartWorkerPool()

	go func() {

		addr, err := net.ResolveTCPAddr(s.tcpVersion, fmt.Sprintf("%s:%d", s.ip, s.port))
		if err != nil {
			panic(fmt.Errorf("resolve tcp addr error: %v\n", err))
			return
		}

		listener, err := net.ListenTCP(s.tcpVersion, addr)
		if err != nil {
			panic(fmt.Errorf("listen %s error: %v\n", s.tcpVersion, err))
			return
		}

		log.Printf("start transport %s success.\n", s.name)
		var cid uint32 = 0 // 连接ID

		for {
			conn, err := listener.AcceptTCP()
			if err != nil {
				log.Printf("Accept tcp error: %v\n", err)
				continue
			}

			// 判断已经连接的数量，若以达到最大连接数，则直接关闭连接
			if s.connMgr.Len() >= utils.GlobalObject.MaxConn {
				//TODO: 回执给客户端超出最大连接的错误包
				log.Println("=====> Connection overflow!")
				conn.Close()
				continue
			}

			// 处理新链接的业务
			dealConn := NewConnection(s, conn, cid, s.msgHandler)
			cid++

			log.Println("=====> [CONN] NowConn:", s.connMgr.Len(), " MaxConn:", utils.GlobalObject.MaxConn)

			// 启动当前的链接业务处理
			go dealConn.Start()
		}
	}()
}

func (s *server) Stop() {
	s.connMgr.ClearAllConn()
	log.Printf("[STOP] server name %s\n", s.name)
}

func (s *server) RegisterRouter(msgID uint32, router Router) {
	s.msgHandler.RegisterRouter(msgID, router)
}

func (s *server) GetConnManager() ConnManager {
	return s.connMgr
}

// 设置在Server创建链接之前自动调用的函数
func (s *server) SetOnConnStartFunc(f func(c Connection)) {
	s.onConnStart = f
}

// 设置在Server销毁链接之后自动调用的函数
func (s *server) SetOnConnStopFunc(f func(c Connection)) {
	s.onConnStop = f
}

// 在Server创建链接之前调用
func (s *server) CallOnConnStart(c Connection) {
	if s.onConnStop != nil {
		s.onConnStart(c)
	}
}

// 在Server销毁链接之前之后调用
func (s *server) CallOnConnStop(c Connection) {
	if s.onConnStop != nil {
		s.onConnStop(c)
	}
}

/*
	初始化Server
*/
func NewServer(serverName string) Server {
	return &server{
		name:       utils.GlobalObject.Name,
		tcpVersion: "tcp4",
		ip:         utils.GlobalObject.Host,
		port:       utils.GlobalObject.TcpPort,
		msgHandler: NewMessageHandler(),
		connMgr:	NewConnManager(),
	}
}
