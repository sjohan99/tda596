package main

import (
	"bufio"
	"chord/argparser"
	"fmt"
	"log"
	"os"
	"strings"
)

type logWriter struct{}

func (writer logWriter) Write(bytes []byte) (int, error) {
	logEntry := string(bytes)
	parts := strings.SplitN(logEntry, ": ", 2)
	if len(parts) == 2 {
		fmt.Printf("\033[90m%s\033[0m: %s", parts[0], parts[1])
	} else {
		fmt.Print(logEntry)
	}
	return len(bytes), nil
}

func main() {
	config := argparser.ParseArguments()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(new(logWriter))
	var n Node
	if config.Initialization == argparser.CREATE {
		n = *createNode(config)
	} else {
		n = *joinNode(config)
	}
	n.Start(config)
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter text: ")
		reader.ReadString('\n')
		n.PrintState()
	}
}
