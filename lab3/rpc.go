package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
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
