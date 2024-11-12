package main

import (
	"io"
	"net/http"
	"testing"
	"time"
)

const port = "8080"
const baseEndpoint = "http://localhost:" + port
const textFileEndpoint = baseEndpoint + "?file=testfile.txt"

// TestMain runs the server before running the tests
func TestMain(m *testing.M) {
	server := HttpServer{
		Port:                       port,
		ContentDir:                 "test_data",
		NumberOfConnectionHandlers: 10,
	}
	go runServer(server)
	time.Sleep(time.Millisecond * 250)
	m.Run()
}

func TestGetTextfileHasCorrectBody(t *testing.T) {
	resp, err := http.Get(textFileEndpoint)
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
	resp, err := http.Get(textFileEndpoint)
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK but got %v", resp.StatusCode)
	}
}
