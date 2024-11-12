package main

import (
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const defaultReadDir = "test_data"
const baseEndpoint = "http://localhost"

func endpoint(port string) string {
	return baseEndpoint + ":" + port
}

func query(file string) string {
	return "?file=" + file
}

// Creates a new HttpServer and runs it in a goroutine
// A random port is assigned by the OS in order to isolate tests
// from each other
func setupTest(t *testing.T, readDir, writeDir string) *HttpServer {
	if writeDir == "" {
		writeDir = t.TempDir()
	}
	listener, err := net.Listen("tcp", ":0") // 0 means os will choose a free port
	if err != nil {
		t.Fatalf("Failed to create listener for test: %v", err)
	}
	server := HttpServer{
		opts:                       Opts{ReadDirectory: readDir, WriteDirectory: writeDir},
		listener:                   listener,
		numberOfConnectionHandlers: 10,
	}

	go server.Run()
	return &server
}

func TestMain(m *testing.M) {
	for _, arg := range os.Args {
		if arg == "quiet" {
			logger.SetOutput(io.Discard)
			break
		}
	}
	m.Run()
}

func TestGetTextfileHasCorrectBody(t *testing.T) {
	server := setupTest(t, defaultReadDir, "")
	defer server.Stop()
	resp, err := http.Get(endpoint(server.Port()) + query("testfile.txt"))
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)
	if bodyStr != "this is text content\n" {
		t.Errorf("Expected body to be 'this is text content\n' but got %v", bodyStr)
	}
}

func TestGetExistingResourceRespondsWithStatusOK(t *testing.T) {
	server := setupTest(t, defaultReadDir, "")
	defer server.Stop()
	resp, err := http.Get(endpoint(server.Port()) + query("testfile.txt"))
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK but got %v", resp.StatusCode)
	}
}

func TestPostFile(t *testing.T) {
	writeDir := t.TempDir()
	server := setupTest(t, defaultReadDir, writeDir)
	defer server.Stop()

	expectedFileContent := "test content"
	testFile := "tmp.txt"

	resp, err := http.Post(endpoint(server.Port())+query(testFile), "text/plain", strings.NewReader(expectedFileContent))
	if err != nil {
		t.Errorf("Failed to make POST request: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Expected status Created but got %v", resp.StatusCode)
	}
	file, err := os.ReadFile(filepath.Join(writeDir, testFile))
	if err != nil {
		t.Errorf("Failed to read file: %v", err)
	}
	if string(file) != expectedFileContent {
		t.Errorf("Expected file content to be %v but got %v", expectedFileContent, string(file))
	}
}
