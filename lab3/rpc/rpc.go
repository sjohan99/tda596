package rpc

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type PingArgs struct{}

type PingReply struct {
	Message string
}

type RPCNode struct{}

func (t *RPCNode) Ping(args *PingArgs, reply *PingReply) error {
	fmt.Println("Ping!")
	reply.Message = "Pong!"
	return nil
}

func (t *RPCNode) StartServer(ip string, port string) {
	arith := new(RPCNode)
	rpc.Register(arith)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ip+":"+port)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	// TODO change to go http.Serve(l, nil)
	http.Serve(l, nil)
}

func (t *RPCNode) PingServer(ip string, port string) {
	client, err := rpc.DialHTTP("tcp", ip+":"+port)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	pingArgs := new(PingArgs)
	pingReply := new(PingReply)
	pingCall := client.Go("RPCNode.Ping", pingArgs, pingReply, nil)
	<-pingCall.Done
	fmt.Println(pingReply.Message)
}
