package config

import (
	"github.com/treeforest/gos/utils/config"
	"github.com/treeforest/logger"
	"github.com/treeforest/gos/utils/config/source/file"
	"github.com/treeforest/gos/utils/config/encoder/yaml"
	"github.com/treeforest/gos/utils/config/source"
	"os"
	"path/filepath"
)

/*
	存储一切有关 服务端 框架的全局参数
*/

type serverConfig struct {
	Host             string // IP 地址
	TcpPort          uint32 // 端口号
	Name             string // 服务名
	Version          string // gos 的版本号
	MaxConn          uint32 // 最大连接数
	MaxPackageSize   uint32 // 数据包的最大大小
	WorkerPoolSize   uint32 // worker工作池大小
	MaxWorkerTaskLen uint32 // 每个worker对应的消息队列的最大数量
}

/*
	定义一个全局的对外GlobalObj
*/
var ServerConfig *serverConfig

func (c *serverConfig) load() {
	conf, err := config.NewConfig()
	if err != nil {
		log.Errorf("New config  error: %v ", err)
	}

	// 配置文件路径
	path := filepath.Join(os.Getenv("GoPath"), "src", "github.com", "treeforest", "gos", "config", "config.yaml")

	e := yaml.NewEncoder()
	if err = conf.Load(file.NewSource(
		file.WithPath(path),
		source.WithEncoder(e),
	)); err != nil {
		log.Errorf("config load error: %v", err)
	}

	Name := conf.Get("Name").String("GOS SERVER")
	Host := conf.Get("Host").String("0.0.0.0")
	TcpPort := conf.Get("TcpPort").Int(9999)
	MaxConn := conf.Get("MaxConn").Int(20000)
	Version := "V1.0" // 默认配置
	WorkerPoolSize := conf.Get("WorkerPoolSize").Int(20)
	MaxPackageSize := conf.Get("MaxPackageSize").Int(4096)
	MaxWorkerTaskLen := conf.Get("MaxWorkerTaskLen").Int(1024)

	// 初始化
	ServerConfig.Name = Name
	ServerConfig.Host = Host
	ServerConfig.TcpPort = uint32(TcpPort)
	ServerConfig.MaxConn = uint32(MaxConn)
	ServerConfig.Version = Version
	ServerConfig.WorkerPoolSize = uint32(WorkerPoolSize)
	ServerConfig.MaxPackageSize = uint32(MaxPackageSize)
	ServerConfig.MaxWorkerTaskLen = uint32(MaxWorkerTaskLen)
}

/*
	初始化全局对象
*/
func init() {
	ServerConfig = new(serverConfig)
	ServerConfig.load()
}