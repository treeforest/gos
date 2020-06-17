package transport

import "net"

/*
 服务接口
*/
type Server interface {
	// 启动服务器
	Start()

	// 停止服务器
	Stop()

	// 运行服务器
	Serve()

	// 给当前的服务注册路由
	RegisterRouter(msgID uint32, router Router)
}

/*
 链接接口
*/
type Connection interface {
	// 启动链接， 让当前的链接准备工作
	Start()

	// 停止链接，结束当前链接的工作
	Stop()

	// 获取当前链接的绑定
	GetTCPConnection() *net.TCPConn

	// 获取当前链接模块的链接ID
	GetConnID() uint32

	// 获取远程客户端的 TCP状态
	RemoteAddr() net.Addr

	// 发送数据，将数据发送给远程的客户端
	Send(msgID uint32, data []byte) error
}

// 处理链接业务的方法
type HandleFunc func(conn *net.TCPConn, data []byte, nLen int) error

/*
 Request 接口
 实际上是把客户端请求的链接信息与数据包装到一个Request中
*/
type Request interface {
	// 得到当前链接
	GetConnection() Connection

	// 得到请求的消息数据
	GetData() []byte

	// 得到当前请求的消息ID
	GetMsgID() uint32
}

/*
 路由抽象接口，路由里的数据都是Request
*/
type Router interface {
	// 处理业务之前的方法
	PreHandle(Request)

	// 处理业务的方法
	Handle(Request)

	// 处理业务后的方法
	PostHandle(Request)
}

/*
 将请求的消息封装到一个Message中，定义抽象的接口
*/
type Message interface {
	// 获取消息的ID
	GetID() uint32

	// 获取消息的长度
	GetLen() uint32

	//获取消息的内容
	GetData() []byte

	// 设置消息ID
	SetID(uint32)

	//设置消息的长度
	SetLen(uint32)

	// 设置消息的内容
	SetData([]byte)
}

/*
 数据的封包、拆包 模块
 直接连接TCP连接中的数据流，用于处理TCP粘包问题
*/
type DataPacker interface {
	// 获取数据包长度
	GetHeadLen() uint32

	// 封包方法
	Pack(Message) ([]byte, error)

	// 拆包方法
	Unpack([]byte) (Message, error)
}

/*
 消息管理抽象层
 */
type MessageHandler interface {
	// 调度/执行对应的Router消息处理方法
	Do(Request)

	// 为消息添加具体的处理逻辑
	RegisterRouter(msgID uint32, router Router)
}