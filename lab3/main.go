package main

import (
	"bufio"
	"chord/argparser"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
)

const helpMessage = "Commands:\n\tlookup <filename>\n\tstorefile <filename>\n\tprintstate\n\texit\n"

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

func parseCommand(reader *bufio.Reader) (string, string, error) {
	fmt.Print("Enter command (-help): ")
	input, _ := reader.ReadString('\n')
	parts := strings.Split(input, " ")
	command := strings.ToLower(strings.TrimSpace(parts[0]))

	if slices.Contains([]string{"lookup", "storefile"}, command) {
		if len(parts) == 2 {
			filepath := strings.TrimSpace(parts[1])
			return command, filepath, nil
		} else {
			return "", "", fmt.Errorf("missing filename argument")
		}
	}

	return command, "", nil
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

	reader := bufio.NewReader(os.Stdin)
	for {
		command, filepath, err := parseCommand(reader)
		if err != nil {
			fmt.Println(err)
			continue
		}
		switch command {
		case "lookup":
			n.LookUpCmd(filepath)
		case "storefile":
			n.StoreFileCmd(filepath)
		case "printstate":
			n.PrintStateCmd()
		case "-help":
			fmt.Print(helpMessage)
		case "exit":
			return
		default:
			fmt.Print(helpMessage)
		}
	}
}
