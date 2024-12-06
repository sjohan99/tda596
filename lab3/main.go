package main

import (
	"bufio"
	"chord/argparser"
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	config := argparser.ParseArguments()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(io.Discard)
	var n Node
	if config.Initialization == argparser.CREATE {
		n = createNode(config)
	} else {
		n = joinNode(config)
	}
	n.Start(config)
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter text: ")
		reader.ReadString('\n')
		n.PrintState()
	}
}
