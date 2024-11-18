package httpserver

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
)

type HttpServer struct {
	NumberOfConnectionHandlers int          // number of go routines to handle connections
	Listener                   net.Listener // listener to accept connections
	Opts                       Opts         // options for the server
	Handler                    Handler      // function to handle connections
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

func Get(url *url.URL) (*http.Response, error) {
	// url library does not include port in the Host field if it is the default port (80)
	// but we need the port for net.Dial()
	if url.Port() == "" {
		url.Host = url.Host + ":80"
	}
	conn, err := net.Dial("tcp", url.Host)
	if err != nil {
		logger.Println("Error connecting to server:", err)
		return nil, err
	}
	req := http.Request{
		Method: "GET",
		URL:    url,
		Proto: "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	req.Write(conn)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		logger.Println("Error reading response:", err)
	}
	return resp, nil
}

func DefaultHandler(conn net.Conn, opts Opts) {
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		RespondWithStatus(http.StatusBadRequest, conn)
		return
	}

	switch req.Method {
	case "GET":
		getHandler(conn, req, opts)
	case "POST":
		postHandler(conn, req, opts)
	default:
		RespondWithStatus(http.StatusNotImplemented, conn)
	}
	defer conn.Close()
}

func getHandler(conn net.Conn, req *http.Request, opts Opts) {
	fileName, fileContentType, err := checkFileFormat(req, opts.ReadDirectory)
	if err != nil {
		respondWithErrorMessage(http.StatusBadRequest, err.Error(), conn)
		return
	}
	file, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Println("File not found:", fileName)
			RespondWithStatus(http.StatusNotFound, conn)
			return
		}
		logger.Println("Error reading file:", err)
		RespondWithStatus(http.StatusInternalServerError, conn)
		return
	}

	file_stats, err := file.Stat()
	if err != nil {
		logger.Println("Error getting file stats:", err)
		RespondWithStatus(http.StatusInternalServerError, conn)
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
		RespondWithStatus(http.StatusInternalServerError, conn)
		return
	}
	defer file.Close()
	_, err = io.Copy(file, req.Body)
	if err != nil {
		logger.Println("Error copying file:", err)
		RespondWithStatus(http.StatusInternalServerError, conn)
		return
	}
	RespondWithStatus(http.StatusCreated, conn)
}

func startConnectionHandlers(tasks <-chan Task, numberOfTaskWorkers int, server *HttpServer) {
	for range numberOfTaskWorkers {
		go func() {
			for task := range tasks {
				server.Handler(task, server.Opts)
			}
		}()
	}
}

func (server *HttpServer) Run() {
	createDirectoryIfNotExists(server.Opts.ReadDirectory)
	createDirectoryIfNotExists(server.Opts.WriteDirectory)

	var tasks = make(chan Task)
	startConnectionHandlers(tasks, server.NumberOfConnectionHandlers, server)
	logger.Println("Server starting, listening on", server.Listener.Addr())

	for {
		conn, err := server.Listener.Accept()
		if err != nil {
			if isClosedConnError(err) {
				logger.Println("Listener closed on ", server.Listener.Addr())
				return
			}
			logger.Println("Listener error:", err)
		} else {
			tasks <- conn
		}
	}
}

func (s *HttpServer) Stop() {
	s.Listener.Close()
}

func CreateServer(port string) *HttpServer {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatalf("Error listening on port %s: %v", port, err)
	}
	return &HttpServer{
		Opts:                       Opts{ReadDirectory: "public", WriteDirectory: "public"},
		Listener:                   listener,
		NumberOfConnectionHandlers: numberOfConnectionHandlers,
		Handler:                    DefaultHandler,
	}
}
