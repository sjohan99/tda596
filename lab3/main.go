package main

import (
	"bufio"
	"chord/argparser"
	"fmt"
	"log"
	"os"
)

func main() {
	config := argparser.ParseArguments()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	if config.Initialization == argparser.CREATE {
		n := createNode(config)
		go n.StartServer(n.IP, n.Port)
		for {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter text: ")
			reader.ReadString('\n')
			fmt.Printf("Node: %+v\n", n)
		}

	} else {
		n := joinNode(config)
		//go n.StartServer(n.IP, n.Port)
		for {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter text: ")
			reader.ReadString('\n')
			fmt.Printf("Node: %+v\n", n)
		}
	}
}
