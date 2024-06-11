// Package comm implements the communication layer of the benchmark server.
package comm

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"restrelay-bench/server/logic"
)

// globalCounter is a counter for creating unique correlation id. Format: globalPrefix + globalCounter
var globalCounter = logic.NewThreadSafeCounter()

// globalPrefix is a unique-per-server-instance prefix for correlation id generating
var globalPrefix = strconv.FormatInt(time.Now().UnixNano(), 10)

func newCorrelationID() string {
	return globalPrefix + strconv.FormatInt(globalCounter.Inc(), 10)
}

// HTTPServer is an interface for the server.
type HTTPServer interface {
	ServerListenAndServe(addr, nodeName, baseURL string) error
}

// RunServer runs the server with the specified parameters.
func RunServer(srv string, serverURL string, baseURL string, nodeName string) error {
	var httpServer HTTPServer
	switch srv {
	case "standard":
		httpServer = &standardHTTPServer{}
	case "fast":
		httpServer = &fastHTTPServer{}
	case "gin":
		httpServer = &ginHTTPServer{}
	default:
		return errors.New("unknown http-server")
	}

	fmt.Println(srv)

	return httpServer.ServerListenAndServe(serverURL, nodeName, baseURL) //nolint:wrapcheck
}
