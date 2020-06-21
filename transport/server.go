package transport

import (
	"fmt"
	"github.com/treeforest/logger"
	"net"
	"github.com/treeforest/gos/config"
	"sync"
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
	log.Infof("START Server[%s] listener at IP[%s:%d] is starting...", s.name, s.ip, s.port)
	log.Infof("START Version[%s] MaxConn[%d] MaxPackageSize[%d]",
		config.ServerConfig.Version, config.ServerConfig.MaxConn, config.ServerConfig.MaxPackageSize)

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

		log.Infof("START server[%s] success!!!\n", s.name)
		var cid uint32 = 0 // 连接ID

		for {
			conn := GlobalTCPConnPool.Get()

			conn, err = listener.AcceptTCP()
			if err != nil {
				log.Errorf("Accept TCP error: %v", err)
				continue
			}

			// 判断已经连接的数量，若以达到最大连接数，则直接关闭连接
			if s.connMgr.Len() >= config.ServerConfig.MaxConn {
				//TODO: 回执给客户端超出最大连接的错误包

				log.Warnf("Connection overflow!")
				GlobalTCPConnPool.Put(conn)
				continue
			}

			// 处理新链接的业务
			dealConn := NewConnection(s, conn, cid, s.msgHandler)
			cid++

			log.Debugf("New connection ConnCount:%d MaxConn:%d ", s.connMgr.Len(), config.ServerConfig.MaxConn)

			// 启动当前的链接业务处理
			go dealConn.Start()
		}
	}()
}

func (s *server) Stop() {
	s.connMgr.ClearAllConn()
	log.Infof("STOP server[%s]\n", s.name)
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
var server_once sync.Once
var global_server *server
func NewServer(serverName string) Server {
	server_once.Do(func() {
		global_server = &server{
			name:       config.ServerConfig.Name,
			tcpVersion: "tcp4",
			ip:         config.ServerConfig.Host,
			port:       config.ServerConfig.TcpPort,
			msgHandler: NewMessageHandler(),
			connMgr:    NewConnManager(),
		}
	})
	return global_server
}
