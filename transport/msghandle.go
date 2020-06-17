package transport

import (
	"fmt"
	"log"
	"github.com/treeforest/gos/utils"
)

/*
	消息处理模块的实现
*/
type messageHandle struct {
	// 存放每个msgID所对应的处理方法
	apis map[uint32]Router

	// 负责Worker取任务的消息队列
	taskQueue []chan Request

	// 业务工作Worker池的Worker数量
	workerPoolSize uint32
}

func NewMessageHandler() MessageHandler {
	return &messageHandle{
		apis:           make(map[uint32]Router),
		taskQueue:      make([]chan Request, utils.GlobalObject.WorkerPoolSize),
		workerPoolSize: utils.GlobalObject.WorkerPoolSize,
	}
}

// 调度/执行对应的Router消息处理方法
func (h *messageHandle) HandleRequest(req Request) {
	handler, ok := h.apis[req.GetMsgID()]
	if !ok {
		log.Printf("api msgID = %d is not found!\n", req.GetMsgID())
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
	log.Printf("register router msgID = %d success.\n", msgID)
}

// 启动Worker Pool(该动作只能发生一次)
func (h *messageHandle) StartWorkerPool() {
	// 根据 h.workerPoolSize 分别开启Worker
	var i uint32
	for i = 0; i < h.workerPoolSize; i++ {
		// 初始化一个worker
		h.taskQueue[i] = make(chan Request, utils.GlobalObject.MaxWorkerTaskLen)
		// 启动当前worker， 阻塞等待消息从channel传递过来
		go h.startOneWorker(i, h.taskQueue[i])
	}
}

func (h *messageHandle) startOneWorker(workerID uint32, task chan Request) {
	fmt.Printf("WorkerID = %d is started.\n", workerID)

	// 阻塞等待对应消息队列的信息
	for {
		select {
		// 有消息到来，出列一个客户端消息
		case req := <- task:
			h.HandleRequest(req)
		}
	}
}

// 将消息交给taskQueue,由worker处理
func (h *messageHandle) SendMsgToTaskQueue(req Request) {
	// 1、将消息平均分配给不同的worker
	// 根据客户端连接的connID进行分配(轮询)
	workerID := req.GetConnection().GetConnID() % h.workerPoolSize
	log.Printf("Add ConnID = %d request msgID = %d to workerID = %d\n", req.GetConnection().GetConnID(), req.GetMsgID(), workerID)

	// 2、将消息发送给对应的worker的taskQueue即可
	h.taskQueue[workerID] <- req
}