package proxy

import (
	"bufio"
	"fmt"
	"lab1/httpserver"
	"net"
	"net/http"
)

func proxyHandler(conn net.Conn, _ httpserver.Opts) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		httpserver.RespondWithStatus(http.StatusBadRequest, conn)
		return
	}
	if req.Method != "GET" {
		httpserver.RespondWithStatus(http.StatusNotImplemented, conn)
		return
	}
	print(req.URL.String())
	resp, err := httpserver.Get(req.URL)
	if err != nil {
		httpserver.RespondWithStatus(http.StatusInternalServerError, conn)
		return
	}
	resp.Write(conn)
}

func Proxy(port string) {
	listener, err := net.Listen("tcp", ":" + port)
	if err != nil {
		fmt.Println("Error creating listener:", err)
		return
	}
	server := httpserver.HttpServer{
		Handler: proxyHandler,
		NumberOfConnectionHandlers: 10,
		Listener: listener,
	}
	server.Run()
}