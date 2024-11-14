package httpserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func createBaseResponse(status int) http.Response {
	return http.Response{
		Status:     http.StatusText(status),
		StatusCode: status,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
	}
}

func createResponse(status int, contentType, contentLength string, body io.ReadCloser) http.Response {
	response := createBaseResponse(status)
	response.Body = body
	response.Header.Set("Content-Type", contentType)
	response.Header.Set("Content-Length", contentLength)
	return response
}

func respondWithStatus(status int, conn net.Conn) {
	httpResponse := createBaseResponse(status)
	httpResponse.Write(conn)
}

func respondWithErrorMessage(status int, message string, conn net.Conn) {
	response := map[string]string{
		"error":   http.StatusText(status),
		"message": message,
	}
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		logger.Println("Error marshalling JSON:", err)
		respondWithStatus(http.StatusInternalServerError, conn)
		return
	}
	httpResponse := createResponse(status, "application/json", fmt.Sprint(len(jsonBytes)), io.NopCloser(bytes.NewReader(jsonBytes)))
	httpResponse.Write(conn)
}

// getSecureFilePath returns the path to the file in the public directory
// and ensures that the file is within the public directory
func getSecureFilePath(file_name string, allowedDirectory string) string {
	return filepath.Join(allowedDirectory, file_name)
}

func getFilePath(url *url.URL) string {
	if url.Path == "" || url.Path == "/" {
		return "index.html"
	}

	parts := strings.Split(url.Path, "/")
	for i, part := range parts {
		parts[i] = filepath.Base(strings.TrimSpace(part))
	}
	return filepath.Join(parts...)
}

func checkFileFormat(req *http.Request, allowedDirectory string) (string, string, error) {
	fileName := getFilePath(req.URL)

	file_ext := strings.ToLower(filepath.Ext(fileName))
	fileContentType, ok := extToContentType[file_ext]
	if !ok {
		return "", "", fmt.Errorf("unsupported media type")
	}

	fileName = getSecureFilePath(fileName, allowedDirectory)
	return fileName, fileContentType, nil
}

func createDirectoryIfNotExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		logger.Printf("Content directory %s does not exist, will create directory", dir)
		if err := os.MkdirAll(dir, os.ModeDir); err != nil {
			logger.Printf("Failed to create content directory %s: %v", dir, err)
		}
		return err
	}
	return nil
}

func isClosedConnError(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if opErr.Err.Error() == "use of closed network connection" {
			return true
		}
	}
	return false
}

func (s *HttpServer) Port() string {
	port := s.Listener.Addr().(*net.TCPAddr).Port
	return strconv.Itoa(port)
}
