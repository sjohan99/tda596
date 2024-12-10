package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

const chordPath = "/_chord_"

func (n *Node) StartServer(ip string, port string, ctx *context.Context) {
	server := rpc.NewServer()
	server.Register(n)
	server.HandleHTTP(chordPath+port, "/chord/debug"+port)
	l, err := net.Listen("tcp", ip+":"+port)
	go func() {
		<-(*ctx).Done()
		log.Printf("Shutting down server")
		l.Close()
	}()
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)
}

func Call(rpcname, ip, port string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTPPath("tcp", ip+":"+port, chordPath+port)
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
