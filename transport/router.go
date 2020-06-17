package transport

// 使用Router时，先嵌入BaseRouter基类，然后根据需要对这个基类方法进行重写
// 则可以实现对应的方法，不必三个方法均实现
type BaseRouter struct {
}

// 处理业务之前的方法
func (r *BaseRouter) PreHandle(Request) {}

// 处理业务的方法
func (r *BaseRouter) Handle(Request) {}

// 处理业务后的方法
func (r *BaseRouter) PostHandle(Request) {}
