package client

import (
	"container/list"
	"fmt"
	"github.com/treeforest/gos/transport/context"
	"github.com/treeforest/logger"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
	"time"
)

type client struct {
	conn      net.Conn
	recvQueue *list.List
	sendQueue *list.List
}

func NewClient() Client {
	return &client{
		recvQueue: list.New(),
		sendQueue: list.New(),
	}
}

func (c *client) Send(serviceID, methodID uint32, data []byte) {
	ctx := new(context.Context)
	ctx.ServiceId = serviceID
	ctx.MethodId = methodID
	ctx.Data = data

	c.sendQueue.PushBack(NewMessage(ctx))
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
			headData := make([]byte, 8)
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

			if msg.DataLen > 0 {
				msg.Data = make([]byte, msg.DataLen)
				_, err := io.ReadFull(c.conn, msg.Data)
				if err != nil {
					log.Warn("transport unpack data error:", err)
					break
				}

				ctx := new(context.Context)
				proto.Unmarshal(msg.Data, ctx)
				msg.ctx = ctx

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
