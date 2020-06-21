package transport

import (
	"fmt"
	"github.com/treeforest/logger"
	"github.com/treeforest/gos/config"
)

/*
	消息处理模块的实现
*/
type messageHandle struct {
	// 存放每个msgID所对应的处理方法
	routerMap map[uint32]Router

	// 负责Worker取任务的消息队列
	taskQueue []chan Request

	// 业务工作Worker池的Worker数量
	workerPoolSize uint32
}

func NewMessageHandler() MessageHandler {
	return &messageHandle{
		routerMap:      make(map[uint32]Router),
		taskQueue:      make([]chan Request, config.ServerConfig.WorkerPoolSize),
		workerPoolSize: config.ServerConfig.WorkerPoolSize,
	}
}

// 调度/执行对应的Router消息处理方法
func (h *messageHandle) HandleRequest(req Request) {
	handler, ok := h.routerMap[req.GetMsgID()]
	if !ok {
		log.Errorf("HandleRequest msgID = %d is not found!", req.GetMsgID())
		return
	}

	handler.PreHandle(req)
	handler.Handle(req)
	handler.PostHandle(req)

	GlobalRequestPool.Put(req.(*request))
}

// 为消息添加具体的处理逻辑
func (h *messageHandle) RegisterRouter(msgID uint32, router Router) {
	//1 判断当前msgID
	if _, ok := h.routerMap[msgID]; ok {
		panic(fmt.Errorf("repeat router, msgID = %d", msgID))
	}

	//2 添加msgID与router的绑定
	h.routerMap[msgID] = router
	log.Infof("register router msgID = %d success!", msgID)
}

// 启动Worker Pool(该动作只能发生一次)
func (h *messageHandle) StartWorkerPool() {
	// 根据 h.workerPoolSize 分别开启Worker
	var i uint32
	for i = 0; i < h.workerPoolSize; i++ {
		// 初始化一个worker
		h.taskQueue[i] = make(chan Request, config.ServerConfig.MaxWorkerTaskLen)
		// 启动当前worker， 阻塞等待消息从channel传递过来
		go h.startOneWorker(i, h.taskQueue[i])
	}
}

func (h *messageHandle) startOneWorker(workerID uint32, task chan Request) {
	log.Debugf("Worker ID = %d is started!", workerID)

	// 阻塞等待对应消息队列的信息
	for {
		select {
		// 有消息到来，出列一个客户端消息
		case req := <-task:
			h.HandleRequest(req)
		}
	}
}

// 将消息交给taskQueue,由worker处理
func (h *messageHandle) SendMsgToTaskQueue(req Request) {
	// 1、将消息平均分配给不同的worker
	// 根据客户端连接的connID进行分配(轮询)
	workerID := req.GetConnection().GetConnID() % h.workerPoolSize

	log.Debugf("Add ConnID = %d request msgID = %d to workerID = %d\n", req.GetConnection().GetConnID(), req.GetMsgID(), workerID)

	// 2、将消息发送给对应的worker的taskQueue即可
	h.taskQueue[workerID] <- req
}
