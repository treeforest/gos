package transport

import (
	"fmt"
	"log"
)

/*
	消息处理模块的实现
*/
type messageHandle struct {
	// 存放每个msgID所对应的处理方法
	apis map[uint32]Router
}

func NewMessageHandler() MessageHandler {
	return &messageHandle{
		apis: make(map[uint32]Router),
	}
}

// 调度/执行对应的Router消息处理方法
func (h *messageHandle) Do(req Request) {
	handler, ok := h.apis[req.GetMsgID()]
	if !ok {
		log.Printf("api msgID = %d is not found!", req.GetMsgID())
		return
	}

	handler.PreHandle(req)
	handler.Handle(req)
	handler.PostHandle(req)
}

// 为消息添加具体的处理逻辑
func (h *messageHandle) RegisterRouter(msgID uint32, router Router) {
	//1 判断当前msgID
	if _, ok := h.apis[msgID]; ok {
		panic(fmt.Errorf("repeat router, msgID = %d", msgID))
	}

	//2 添加msgID与router的绑定
	h.apis[msgID] = router
	log.Printf("register router msgID = %d success.", msgID)
}
