package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
)

type HttpServer struct {
	numberOfConnectionHandlers int          // number of go routines to handle connections
	listener                   net.Listener // listener to accept connections
	opts                       Opts         // options for the server
	handler                    Handler
}

type Opts struct {
	ReadDirectory  string // directory in which files are allowed to be read from
	WriteDirectory string // directory in which files are allowed to be written to
}

const numberOfConnectionHandlers = 10

type Task = net.Conn

type Handler func(net.Conn, Opts)

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

var extToContentType = map[string]string{
	".txt":  "text/plain",
	".html": "text/html",
	".gif":  "image/gif",
	".jpeg": "image/jpeg",
	".jpg":  "image/jpg",
	".png":  "image/png",
	".css":  "text/css",
	"":      "test/html",
}

func handleConnection(conn net.Conn, opts Opts) {
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		respondWithStatus(http.StatusBadRequest, conn)
		return
	}

	switch req.Method {
	case "GET":
		getHandler(conn, req, opts)
	case "POST":
		postHandler(conn, req, opts)
	default:
		respondWithStatus(http.StatusNotImplemented, conn)
	}
	defer conn.Close()
}

// handle get requests
func getHandler(conn net.Conn, req *http.Request, opts Opts) {
	fileName, fileContentType, err := checkFileFormat(req, opts.ReadDirectory)
	if err != nil {
		respondWithErrorMessage(http.StatusBadRequest, err.Error(), conn)
	}
	file, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Println("File not found:", fileName)
			respondWithStatus(http.StatusNotFound, conn)
			return
		}
		logger.Println("Error reading file:", err)
		respondWithStatus(http.StatusInternalServerError, conn)
		return
	}

	file_stats, err := file.Stat()
	if err != nil {
		logger.Println("Error getting file stats:", err)
		respondWithStatus(http.StatusInternalServerError, conn)
	}
	httpResponse := createResponse(http.StatusOK, fileContentType, fmt.Sprint(file_stats.Size()), file)
	httpResponse.Write(conn)
}

func postHandler(conn net.Conn, req *http.Request, opts Opts) {
	fileName, _, err := checkFileFormat(req, opts.WriteDirectory)
	if err != nil {
		respondWithErrorMessage(http.StatusBadRequest, err.Error(), conn)
		return
	}
	if _, err := os.Stat(fileName); err == nil {
		respondWithErrorMessage(http.StatusConflict, "file already exists", conn)
		return
	}

	file, err := os.Create(fileName)
	if err != nil {
		logger.Println("Error creating file:", err)
		respondWithStatus(http.StatusInternalServerError, conn)
		return
	}
	defer file.Close()
	_, err = io.Copy(file, req.Body)
	if err != nil {
		logger.Println("Error copying file:", err)
		respondWithStatus(http.StatusInternalServerError, conn)
		return
	}
	respondWithStatus(http.StatusCreated, conn)
}

func startConnectionHandlers(tasks <-chan Task, numberOfTaskWorkers int, server *HttpServer) {
	for range numberOfTaskWorkers {
		go func() {
			for task := range tasks {
				server.handler(task, server.opts)
			}
		}()
	}
}

func (server *HttpServer) Run() {
	createDirectoryIfNotExists(server.opts.ReadDirectory)
	createDirectoryIfNotExists(server.opts.WriteDirectory)

	var tasks = make(chan Task)
	startConnectionHandlers(tasks, server.numberOfConnectionHandlers, server)
	logger.Println("Server starting, listening on", server.listener.Addr())

	for {
		conn, err := server.listener.Accept()
		if err != nil {
			if isClosedConnError(err) {
				logger.Println("Listener closed on ", server.listener.Addr())
				return
			}
			logger.Println("Listener error:", err)
		} else {
			tasks <- conn
		}
	}
}

func (s *HttpServer) Stop() {
	s.listener.Close()
}

func main() {
	port := readPortFromArgs()
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatalf("Error listening on port %s: %v", port, err)
	}
	server := HttpServer{
		opts:                       Opts{ReadDirectory: "public", WriteDirectory: "public"},
		listener:                   listener,
		numberOfConnectionHandlers: numberOfConnectionHandlers,
		handler:                    handleConnection,
	}
	server.Run()
}
