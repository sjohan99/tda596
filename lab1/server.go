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
	Port                       string
	ContentDir                 string
	NumberOfConnectionHandlers int
}

const numberOfConnectionHandlers = 10

type Task = net.Conn

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

var extToContentType = map[string]string{
	".txt":  "text/plain",
	".html": "text/html",
	".gif":  "image/gif",
	".jpeg": "image/jpeg",
	".jpg":  "image/jpg",
	".css":  "text/css",
}

func handleConnection(conn net.Conn, allowedDirectory string) {
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Print("bad request")
		return
	}

	// http pre processing validating file type
	fileName, fileContentType, err := checkFileFormat(req, allowedDirectory)
	if err != nil {
		respondWithErrorMessage(http.StatusBadRequest, err.Error(), conn)
	}

	switch req.Method {
	case "GET":
		getHandler(conn, fileName, fileContentType)
	case "POST":
		postHandler(conn, req, fileName)
	default:
		respondWithStatus(http.StatusNotImplemented, conn)
	}
	defer conn.Close()
}

// handle get requests
func getHandler(conn net.Conn, fileName string, fileContentType string) {
	file, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("File not found:", fileName)
			respondWithStatus(http.StatusNotFound, conn)
			return
		}
		fmt.Println("Error reading file:", err)
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

func postHandler(conn net.Conn, req *http.Request, fileName string) {
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

// Starts 10 go routines on standby
func startConnectionHandlers(tasks <-chan Task, numberOfTaskWorkers int, allowedDirectory string) {
	for range numberOfTaskWorkers {
		go func() {
			for task := range tasks {
				handleConnection(task, allowedDirectory)
			}
		}()
	}
}

func runServer(server HttpServer) {
	createDirectoryIfNotExists(server.ContentDir)
	var tasks = make(chan Task)
	ln, err := net.Listen("tcp", ":"+server.Port)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
	startConnectionHandlers(tasks, server.NumberOfConnectionHandlers, server.ContentDir)
	fmt.Println("Server started, listening on port", server.Port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
		} else {
			tasks <- conn
		}
	}
}

func main() {
	port := readPortFromArgs()
	server := HttpServer{
		Port:                       port,
		ContentDir:                 "public",
		NumberOfConnectionHandlers: numberOfConnectionHandlers,
	}
	runServer(server)
}
