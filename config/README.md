# 服务端框架配置文件

注: 并非业务代码配置文件

### config.yaml
```yaml
Name: "GOS SERVER"      # 服务器名
Host: "0.0.0.0"         # 服务器地址
TcpPort: 9999           # 服务端端口
MaxConn: 20000          # 服务端最大连接数
WorkerPoolSize: 10      # 工作者池中的最大工作者数量
MaxPackageSize: 4096    # 传输的每个数据包的最大大小
MaxWorkerTaskLen: 1024  # 工作池任务队列长度
```