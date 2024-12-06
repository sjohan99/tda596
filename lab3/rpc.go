package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"runtime"
)

func (n *Node) StartServer(ip string, port string) {
	rpc.Register(n)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ip+":"+port)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)
}

func Call(rpcname, ip, port string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("tcp", ip+":"+port)
	if err != nil {
		log.Println("dialing:", err)
		printStackTrace()
		log.Fatal("Could not connect to node")
		return false
	}
	pingCall := client.Go(rpcname, args, reply, nil)
	res := <-pingCall.Done
	if res.Error != nil {
		log.Println("Error:", res.Error)
		return false
	}
	return true
}

func printStackTrace() {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	fmt.Printf("%s\n", buf[:n])
}
