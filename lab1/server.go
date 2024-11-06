package main

import (
	"fmt"
	"io"
	"net/http"
)

func handle(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Hello world!")
}

// Replace with our http server impl
// This is just to make sure devcontainer works properly
func main() {
	portNumber := "8080"
	http.HandleFunc("/", handle)
	fmt.Println("Server listening on port", portNumber)
	http.ListenAndServe(":"+portNumber, nil)
}
