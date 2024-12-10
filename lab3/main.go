package main

import (
	"bufio"
	"chord/argparser"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type logWriter struct{}

// makes prefix (date, time, file) grey in terminal
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
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(new(logWriter))

	config := argparser.ParseArguments()
	var n Node
	if config.Initialization == argparser.CREATE {
		n = *CreateNode(config)
	} else {
		n = *JoinNode(config)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.Start(config, &ctx)
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter text: ")
		input, _ := reader.ReadString('\n')
		if strings.TrimSpace(input) == "exit" {
			cancel()
			time.Sleep(45 * time.Second)
			break
		} else {
			n.PrintState()
		}
	}
}
