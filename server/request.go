package server

type request struct {
	// 已经和客户建立好的链接
	conn Connection

	// 客户端请求的数据
	data []byte
}

func NewRequest(conn Connection, data []byte) Request {
	return &request{
		conn: conn,
		data: data,
	}
}

// 得到当前链接
func (r *request) GetConnection() Connection {
	return r.conn
}

// 得到请求的消息数据
func (r *request) GetData() []byte {
	return r.data
}
