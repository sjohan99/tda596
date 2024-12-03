package main

import (
	"chord/cli"
	"fmt"
)

func main() {
	config := cli.ParseArguments()
	fmt.Printf("%+v\n", config)
}
