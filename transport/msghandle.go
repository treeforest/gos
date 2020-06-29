package transport

import (
	"fmt"
	"github.com/treeforest/gos/config"
	"github.com/treeforest/logger"
)

/*
	消息处理模块的实现
*/
type messageHandle struct {
	// 存放每个msgID所对应的处理方法
	routerMap map[uint32]Router

	// 工作池的消息队列
	taskChan chan Request

	// 业务工作Worker池的Worker数量
	workerPoolSize uint32
}

func NewMessageHandler() MessageHandler {
	return &messageHandle{
		routerMap:      make(map[uint32]Router),
		taskChan:       make(chan Request, config.ServerConfig.WorkerPoolSize),
		workerPoolSize: config.ServerConfig.WorkerPoolSize,
	}
}

// 调度/执行对应的Router消息处理方法
func (h *messageHandle) HandleRequest(req Request) {
	handler, ok := h.routerMap[req.GetServiceID()]
	if !ok {
		log.Errorf("HandleRequest serviceID = %d is not found!", req.GetServiceID())
		return
	}

	handler.PreHandle(req)
	handler.Handle(req)
	handler.PostHandle(req)

	// 回收临时对象资源
	globalPool.PutContext(req.GetContext())
	globalPool.PutRequest(req.(*request))
}

// 为消息添加具体的处理逻辑
func (h *messageHandle) RegisterRouter(serviceID uint32, router Router) {
	//1 判断当前msgID
	if _, ok := h.routerMap[serviceID]; ok {
		panic(fmt.Errorf("repeat router, serviceID = %d", serviceID))
	}

	//2 添加msgID与router的绑定
	h.routerMap[serviceID] = router
	log.Infof("register router serviceID = %d success!", serviceID)
}

// 启动Worker Pool(该动作只能发生一次)
func (h *messageHandle) StartWorkerPool() {
	// 根据 h.workerPoolSize 分别开启Worker
	var i uint32
	for i = 0; i < h.workerPoolSize; i++ {
		// 启动一个worker， 阻塞等待消息从channel传递过来
		go h.startOneWorker(i)
	}
}

func (h *messageHandle) startOneWorker(workerID uint32) {
	log.Debugf("Worker ID = %d is started!", workerID)

	// 阻塞等待对应消息队列的任务
	for {
		select {
		// 取一个任务就行处理
		case req := <-h.taskChan:
			// TODO：根据优先级处理相关信息
			// log.Infof("Worker ID:%d", workerID)
			h.HandleRequest(req)
		}
	}
}

// 将执行的任务交给工作池处理
func (h *messageHandle) EntryTaskToWorkerPool(req Request) {
	// log.Debugf("Add ConnID = %d serviceID = %d to workerID = %d", req.GetConnection().GetConnID(), req.GetServiceID(), workerID)

	// 将消息发送给worker的任务队列即可
	h.taskChan <- req
}
