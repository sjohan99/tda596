package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

var extToContentType = map[string]string{
	".txt":  "text/plain",
	".html": "text/html",
}

func handleConnection(conn net.Conn) {
	fmt.Println("connected", conn)
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Print("bad request")
		return
	}
	switch req.Method {
	case "GET":
		getHandler(conn, req)
	case "POST":
		postHandler(conn, req)
	default:
		// TODO return 501 not implemented
		fmt.Println("Unsupported method:", req.Method)
	}
	defer conn.Close()
}

func createResponse(status int) http.Response {
	return http.Response{
		Status:     http.StatusText(status),
		StatusCode: status,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
	}
}

// handle get requests
func getHandler(conn net.Conn, req *http.Request) {
	file := req.URL.Query().Get("file")
	if file == "" {
		fmt.Println("Bad req -400-")
		// TODO: add message saying that query is missing
		httpResponse := createResponse(http.StatusBadRequest)
		httpResponse.Write(conn)
		return
	}

	ext := filepath.Ext(file)

	fileContent, err := os.ReadFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("File not found:", file)
			httpResponse := createResponse(http.StatusNotFound)
			httpResponse.Write(conn)
			return
		}
		fmt.Println("Error reading file:", err)
		httpResponse := createResponse(http.StatusInternalServerError)
		httpResponse.Write(conn)
		return
	}

	httpResponse := createResponse(http.StatusOK)
	httpResponse.Body = io.NopCloser(strings.NewReader(string(fileContent)))

	// todo
	httpResponse.Header.Set("Content-Type", "text/plain")
	httpResponse.Write(conn)
}

func postHandler(conn net.Conn, req *http.Request) {

}

// Replace with our http server impl
// This is just to make sure devcontainer works properly
func main() {
	ln, err := net.Listen("tcp", ":8080")
	//defer resp.Body.Close()
	if err != nil {
		// handle error
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
		}
		go handleConnection(conn)
	}
}
