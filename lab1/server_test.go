package main

import (
	"bytes"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const defaultReadDir = "test_data"
const baseEndpoint = "http://localhost"

func endpoint(s *HttpServer) string {
	return baseEndpoint + ":" + s.Port()
}

// Creates a new HttpServer and runs it in a goroutine
// A random port is assigned by the OS in order to isolate tests
// from each other
func setupTest(t *testing.T, readDir, writeDir string, handler Handler) *HttpServer {
	if handler == nil {
		handler = handleConnection
	}
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
		numberOfConnectionHandlers: numberOfConnectionHandlers,
		handler:                    handler,
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
	server := setupTest(t, defaultReadDir, "", nil)
	defer server.Stop()
	resp, err := http.Get(endpoint(server) + "/testfile.txt")
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
	server := setupTest(t, defaultReadDir, "", nil)
	defer server.Stop()
	resp, err := http.Get(endpoint(server) + "/testfile.txt")
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK but got %v", resp.StatusCode)
	}
}

func TestGetNonExistentResourceRespondsWithStatusNotFound(t *testing.T) {
	server := setupTest(t, defaultReadDir, "", nil)
	defer server.Stop()
	resp, err := http.Get(endpoint(server) + "/this-file-should-not-exist.txt")
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status OK but got %v", resp.StatusCode)
	}
}

func TestGetUnsupportedMediaTypeResondsWithBadRequest(t *testing.T) {
	server := setupTest(t, defaultReadDir, "", nil)
	defer server.Stop()
	resp, err := http.Get(endpoint(server) + "/testfile.exe")
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status Bad Request but got %v", resp.StatusCode)
	}
}

func TestGetFileInSubdirectory(t *testing.T) {
	server := setupTest(t, defaultReadDir, "", nil)
	defer server.Stop()
	resp, err := http.Get(endpoint(server) + "/subdir/image.png")
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK but got %v", resp.StatusCode)
	}
	f, err := os.Open(filepath.Join(defaultReadDir, "subdir", "image.png"))
	if err != nil {
		t.Errorf("Failed to open file: %v", err)
	}
	defer f.Close()
	expectedContent, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("Failed to read file: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Failed to read response body: %v", err)
	}
	if !bytes.Equal(body, expectedContent) {
		t.Errorf("Expected body to be %v but got %v", string(expectedContent), string(body))
	}
}

func TestPostNonExistingFileShouldSucceed(t *testing.T) {
	writeDir := t.TempDir()
	server := setupTest(t, defaultReadDir, writeDir, nil)
	defer server.Stop()

	expectedFileContent := "test content"
	testFile := "tmp.txt"

	// localhost:port/?file=tmp.txt
	resp, err := http.Post(endpoint(server)+"/"+testFile, "text/plain", strings.NewReader(expectedFileContent))
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

func TestPostExistingFileShouldFail(t *testing.T) {
	tempDir := t.TempDir()
	server := setupTest(t, tempDir, tempDir, nil)
	defer server.Stop()

	os.WriteFile(filepath.Join(tempDir, "testfile.txt"), []byte("content"), fs.ModePerm)
	resp, err := http.Post(endpoint(server)+"/testfile.txt", "text/plain", strings.NewReader("new content"))
	if err != nil {
		t.Errorf("Failed to make POST request: %v", err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("Expected status Conflict but got %v", resp.StatusCode)
	}
}

func TestPostThenGetFile(t *testing.T) {
	tempDir := t.TempDir()
	server := setupTest(t, tempDir, tempDir, nil)
	defer server.Stop()

	content := "new content"

	_, err := http.Post(endpoint(server)+"/myfile.txt", "text/plain", strings.NewReader(content))
	if err != nil {
		t.Errorf("Failed to make POST request: %v", err)
	}
	resp, err := http.Get(endpoint(server) + "/myfile.txt")
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Failed to read response body: %v", err)
	}
	if string(body) != content {
		t.Errorf("Expected body to be '%v' but got '%v'", content, string(body))
	}

}

func TestDeleteMethodReturnsUnimplemented(t *testing.T) {
	server := setupTest(t, defaultReadDir, "", nil)
	defer server.Stop()

	req, err := http.NewRequest(http.MethodDelete, endpoint(server), nil)
	if err != nil {
		t.Errorf("Failed to create DELETE request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("Failed to make DELETE request: %v", err)
	}

	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected status Not Implemented but got %v", resp.StatusCode)
	}
}

func TestMaxConnections(t *testing.T) {
	inUse := int32(0)

	testHandler := func(conn net.Conn, opts Opts) {
		defer conn.Close()
		currentlyInUse := atomic.AddInt32(&inUse, 1)
		if currentlyInUse > numberOfConnectionHandlers {
			t.Fatalf("Expected inUse to be <= 10 but got %v", currentlyInUse)
		}
		time.Sleep(250 * time.Millisecond)
		atomic.AddInt32(&inUse, -1)
		resp := createBaseResponse(http.StatusOK)
		resp.Write(conn)
	}
	server := setupTest(t, defaultReadDir, "", testHandler)
	defer server.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := http.Get(endpoint(server) + "/testfile.txt")
			if err != nil {
				t.Errorf("Failed to make GET request: %v", err)
			}
		}()
	}
	wg.Wait()
}
