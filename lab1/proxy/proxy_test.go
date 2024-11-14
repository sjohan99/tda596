package proxy

import (
	"io"
	"lab1/httpserver"
	"net"
	"net/http"
	"net/url"
	"testing"
)

const baseEndpoint = "http://localhost"
const numberOfConnectionHandlers = 10

func endpoint(s *httpserver.HttpServer) string {
	return baseEndpoint + ":" + s.Port()
}

func makeProxyRequest(endpoint, proxyEndpoint string, method string) (*http.Response, error) {
	proxyURL, err := url.Parse(proxyEndpoint)
    if err != nil {
        return nil, nil
    }

    transport := &http.Transport{
        Proxy: http.ProxyURL(proxyURL),
    }

    client := &http.Client{
        Transport: transport,
    }

    req, err := http.NewRequest(method, endpoint, nil)
    if err != nil {
        return nil, err
    }

    return client.Do(req)
}

// Creates a new HttpServer and runs it in a goroutine
// A random port is assigned by the OS in order to isolate tests
// from each other
func setupTest(t *testing.T, readDir, writeDir string, handler httpserver.Handler) *httpserver.HttpServer {
	if handler == nil {
		handler = httpserver.DefaultHandler
	}
	if writeDir == "" {
		writeDir = t.TempDir()
	}
	listener, err := net.Listen("tcp", ":0") // 0 means os will choose a free port
	if err != nil {
		t.Fatalf("Failed to create listener for test: %v", err)
	}
	server := httpserver.HttpServer{
		Opts:                       httpserver.Opts{ReadDirectory: readDir, WriteDirectory: writeDir},
		Listener:                   listener,
		NumberOfConnectionHandlers: numberOfConnectionHandlers,
		Handler:                    handler,
	}

	go server.Run()
	return &server
}

func TestGetReturnsContent(t *testing.T) {
	server := setupTest(t, "test_data", "", nil)
	defer server.Stop()
	proxy := setupTest(t, t.TempDir(), t.TempDir(), proxyHandler)
	defer proxy.Stop()

	serverEndpoint := endpoint(server)+"/testfile.txt"
	proxyEndpoint := endpoint(proxy)

	resp, err := makeProxyRequest(serverEndpoint, proxyEndpoint, "GET")
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Failed to read response body: %v", err)
	}
	if string(body) != "this is text content\n" {
		t.Errorf("Expected body to be 'this is text content\n' but got '%v'", string(body))
	}
}

func TestGetNonExistentResourceRespondsWithStatusNotFound(t *testing.T) {
	server := setupTest(t, "test_data", "", nil)
	defer server.Stop()
	proxy := setupTest(t, t.TempDir(), t.TempDir(), proxyHandler)
	defer proxy.Stop()

	serverEndpoint := endpoint(server)+"/nonexistingfile.txt"
	proxyEndpoint := endpoint(proxy)

	resp, err := makeProxyRequest(serverEndpoint, proxyEndpoint, "GET")
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status Not Found but got %v", resp.StatusCode)
	}
}

func TestPostShouldReturnNotImplemented(t *testing.T) {
	server := setupTest(t, "test_data", "", nil)
	defer server.Stop()
	proxy := setupTest(t, t.TempDir(), t.TempDir(), proxyHandler)
	defer proxy.Stop()

	serverEndpoint := endpoint(server)+"/testfile.txt"
	proxyEndpoint := endpoint(proxy)

	resp, err := makeProxyRequest(serverEndpoint, proxyEndpoint, "POST")
	if err != nil {
		t.Errorf("Failed to make GET request: %v", err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected status Not Implemented but got %v", resp.StatusCode)
	}
}

