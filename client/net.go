package client

import (
	"fmt"
	"io"
	"time"
	"net"
	"container/list"
	"github.com/treeforest/logger"
)

type client struct {
	conn      net.Conn
	recvQueue *list.List
	sendQueue *list.List
}

func NewClient() Client {
	return &client{
		recvQueue:list.New(),
		sendQueue:list.New(),
	}
}

func (c *client) Send(serviceID, methodID uint32, data []byte) {
	msg := NewMessage(serviceID, methodID, data)
	c.sendQueue.PushBack(msg)
}

func (c *client) Recv() Message {
	if c.recvQueue.Len() == 0 {
		return nil
	}

	e := c.recvQueue.Front()
	msg := e.Value.(*message)
	c.recvQueue.Remove(e)
	return msg
}

func (c *client) Dial(address string) {
	var err error
	c.conn, err = net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		panic(fmt.Errorf("dial error: %v", err))
	}

	// 开启读
	go func() {
		for {
			headData := make([]byte, 12)
			_, err = io.ReadFull(c.conn, headData)
			if err != nil {
				log.Warn("read head error")
				break
			}

			msg, err := UnpackHead(headData)
			if err != nil {
				log.Warn("transport unpack head error:", err)
				break
			}

			if msg.dataLen > 0 {
				msg.data = make([]byte, msg.dataLen)
				_, err := io.ReadFull(c.conn, msg.data)
				if err != nil {
					log.Warn("transport unpack data error:", err)
				}

				c.recvQueue.PushBack(msg)
			}
		}
	}()

	// 开启写
	go func() {
		for {
			if c.sendQueue.Len() == 0 {
				time.Sleep(time.Millisecond * 60)
				continue
			}

			elem := c.sendQueue.Front()
			msg := elem.Value.(*message)
			c.sendQueue.Remove(elem)

			buf, err := Pack(msg)
			if err != nil {
				log.Warn("Pack error")
				break
			}

			if _, err = c.conn.Write(buf); err != nil {
				log.Error("conn write error.")
				break
			}
		}
	}()
}