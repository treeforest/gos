package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	)

/*
	存储一切有关 gos 框架的全局参数，供其他模块使用
*/

type GlobalObj struct {
	/*
		Server
	*/
	Host    string
	TcpPort uint32
	Name    string

	/*
		gos
	*/
	Version          string // gos 的版本号
	MaxConn          uint32 // 最大连接数
	MaxPackageSize   uint32 // 数据包的最大大小
	WorkerPoolSize   uint32 // worker工作池大小
	MaxWorkerTaskLen uint32 // 每个worker对应的消息队列的最大数量
}

/*
	定义一个全局的对外GlobalObj
*/

var GlobalObject *GlobalObj

func (g *GlobalObj) Reload() {
	data, err := ioutil.ReadFile("conf/conf.json")
	if err != nil {
		panic(fmt.Errorf("Read conf file error: %v", err))
	}

	err = json.Unmarshal(data, &GlobalObject)
	if err != nil {
		panic(err)
	}

	if GlobalObject.WorkerPoolSize <= 0 {
		GlobalObject.WorkerPoolSize = 10
	}
}

/*
	初始化全局对象
*/
func init() {
	// 默认值
	GlobalObject = &GlobalObj{
		Name:             "Renju-ServerApp",
		Version:          "V1.0",
		TcpPort:          9999,
		Host:             "0.0.0.0",
		MaxConn:          1000,
		MaxPackageSize:   4096,
		WorkerPoolSize:   10,
		MaxWorkerTaskLen: 1024,
	}

	GlobalObject.Reload()
}
