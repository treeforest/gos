package transport

type request struct {
	// 已经和客户建立好的链接
	conn Connection

	// 客户端请求的数据
	msg Message
}

func NewRequest(conn Connection, msg Message) Request {
	return &request{
		conn: conn,
		msg:  msg,
	}
}

// 得到当前链接
func (r *request) GetConnection() Connection {
	return r.conn
}

// 得到请求的消息数据
func (r *request) GetData() []byte {
	return r.msg.GetData()
}

// 得到请求的服务ID
func (r *request) GetServiceID() uint32 {
	return r.msg.GetServiceID()
}

// 得到请求的服务接口ID
func (r *request) GetMethodID() uint32 {
	return r.msg.GetMethodID()
}