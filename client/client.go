package client

type Client interface {
	Dial(address string)
	Send(serviceID, methodID uint32, data []byte)
	Recv() Message
}

type Message interface {
	GetServiceID() uint32
	GetMethodID() uint32
	GetData() []byte
}