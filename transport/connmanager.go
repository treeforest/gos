package transport

import (
	"sync"
	"log"
	"errors"
)

type connManager struct {
	// 管理连接的map集合 map[uint32]Connection
	connMap sync.Map
}

func NewConnManager() ConnManager {
	return &connManager{}
}

// 添加链接
func (m *connManager) Add(conn Connection){
	m.connMap.Store(conn.GetConnID(), conn)
	log.Printf("connID = %d add to ConnManager success: conn num = %d \n", conn.GetConnID(), m.Len())
}

// 删除链接
func (m *connManager) Remove(conn Connection){
	m.connMap.Delete(conn.GetConnID())
	log.Printf("connID = %d remove to ConnManager success: conn num = %d \n", conn.GetConnID(), m.Len())
}

// 根据connID获取链接
func (m *connManager) Get(connID uint32) (Connection, error){
	if conn, ok := m.connMap.Load(connID); ok {
		return conn.(Connection), nil
	}
	return nil, errors.New("connection not FOUND!")
}

// 当前连接总数
func (m *connManager) Len() uint32{
	var nLen uint32 = 0 
	m.connMap.Range(func(key, value interface{}) bool {
		nLen++
		return true
	})
	return nLen
}

// 清除并终止所有连接
func (m *connManager) ClearAllConn(){
	m.connMap.Range(func(key, value interface{}) bool {
		// 主动停止链接
		conn := value.(Connection)
		conn.Stop()

		// 删除元素
		m.connMap.Delete(key)

		return true
	})
}
