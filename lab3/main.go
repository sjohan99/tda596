package main

import (
	"chord/argparser"
	"chord/rpc"
	"strconv"
)

func main() {
	config := argparser.ParseArguments()
	rpcServer := rpc.RPCNode{}
	if config.Initialization == argparser.CREATE {
		rpcServer.StartServer(config.Address, strconv.Itoa(config.Port))
	} else {
		rpcServer.PingServer(config.JoinAddress, strconv.Itoa(config.JoinPort))
	}
}
