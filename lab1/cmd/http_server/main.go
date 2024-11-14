package main

import (
	"fmt"
	"lab1/httpserver"
	"os"
)

func readPortFromArgs() string {
	if len(os.Args) < 2 {
		fmt.Println("Usage: http_server <port>")
		os.Exit(1)
	}
	return os.Args[1]
}

func main() {
	port := readPortFromArgs()
	httpserver.CreateServer(port).Run()
}
