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
	n.Start(config, nil)
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter command (-help): ")
		input, _ := reader.ReadString('\n')
		parts := strings.Split(input, " ")
		command := strings.ToLower(strings.TrimSpace(parts[0]))
		switch command {
		case "lookup":
			if len(parts) < 2 {
				fmt.Println("Error: Missing filename.")
				break
			}
			n.LookUpCmd(strings.TrimSpace(parts[1]))
		case "storefile":
			if len(parts) < 2 {
				fmt.Println("Error: Missing filename.")
				break
			}
			n.StoreFileCmd(strings.TrimSpace(parts[1]))
		case "printstate":
			n.PrintStateCmd()
		case "-help":
			fmt.Printf("Commands:\n\t-lookup <filename>\n\t-storefile <filename>\n\t-printstate\n\t-exit\n")
		case "exit":
			return
		}
	}
}
