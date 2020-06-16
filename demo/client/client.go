package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	fmt.Println("client start...")

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		panic(fmt.Errorf("dial error: %v", err))
	}

	for {
		_, err := conn.Write([]byte("Hello World!"))
		if err != nil {
			fmt.Printf("write error: %v\n", err)
			continue
		}

		buf := make([]byte, 512)
		cnt, err := conn.Read(buf)
		if err != nil {

		}

		fmt.Printf("server call back: %s, cnt=%d\n", buf[:cnt], cnt)

		time.Sleep(time.Second * 2)
	}
}
