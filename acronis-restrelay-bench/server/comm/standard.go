package comm

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"restrelay-bench/server/logic"
	"restrelay-bench/server/version"
)

var gzPool = sync.Pool{
	New: func() interface{} {
		w := gzip.NewWriter(io.Discard)

		return w
	},
}

type standardHTTPServer struct{}

func (srv *standardHTTPServer) ServerListenAndServe(addr, nodeName, baseURL string) error {
	http.HandleFunc("/api/restrelay_bench_server/", func(responseWriter http.ResponseWriter, request *http.Request) {
		if !strings.Contains(request.Header.Get("Accept-Encoding"), "gzip") {
			HandleRequestStandard(responseWriter, request, nodeName, baseURL)

			return
		}

		responseWriter.Header().Set("Content-Encoding", "gzip")

		gz := gzPool.Get().(*gzip.Writer)
		defer gzPool.Put(gz)

		gz.Reset(responseWriter)
		defer gz.Close() //nolint:errcheck

		HandleRequestStandard(&gzipResponseWriter{ResponseWriter: responseWriter, Writer: gz}, request, nodeName, baseURL)
	})
	http.HandleFunc("/api/restrelay_bench_server/version", func(responseWriter http.ResponseWriter, _ *http.Request) {
		if _, err := responseWriter.Write([]byte(version.Version)); err != nil {
			fmt.Printf("failed writing version: %v\n", err)
		}
	})
	http.HandleFunc("/api/restrelay_bench_server/type", func(responseWriter http.ResponseWriter, _ *http.Request) {
		if _, err := responseWriter.Write([]byte(version.Type)); err != nil {
			fmt.Printf("failed writing type: %v\n", err)
		}
	})

	return http.ListenAndServe(addr, nil) //nolint:wrapcheck
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b) //nolint:wrapcheck
}

func finishWithError(responseWriter http.ResponseWriter, message string) {
	responseWriter.WriteHeader(500)
	if _, err := responseWriter.Write([]byte(fmt.Sprintf("ERROR %s", message))); err != nil { //nolint:perfsprint
		fmt.Printf("failed writing error: %v\n", err)
	}
}

// StandardURIArgs is a wrapper for URL values
type StandardURIArgs struct {
	Values url.Values
}

// Get returns the values for a given key
func (args StandardURIArgs) Get(key string) []string {
	return args.Values[key]
}

// HandleRequestStandard handles all incoming HTTP requests
func HandleRequestStandard(responseWriter http.ResponseWriter, request *http.Request, nodeName string, baseURL string) {
	tb := logic.NewTimestampBuilder(nodeName)

	tb.AddTimestamp()

	correlationID := request.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = newCorrelationID()
	}

	var _, err = io.ReadAll(request.Body)
	if err != nil {
		finishWithError(responseWriter, err.Error())

		return
	}

	if err = request.Body.Close(); err != nil {
		finishWithError(responseWriter, err.Error())

		return
	}

	requestWrapper := logic.RequestWrapper{
		Args: StandardURIArgs{
			Values: request.URL.Query(),
		},
		URI:              request.URL.RequestURI(),
		ConnectionHeader: request.Header.Get("Connection"),
	}

	reader, headers, status, err := logic.PerformActions(baseURL, requestWrapper, nodeName, &tb, correlationID)
	if err != nil {
		finishWithError(responseWriter, err.Error())

		return
	}

	responseWriter.Header().Set("Server", "standard")

	for key, value := range headers {
		responseWriter.Header().Set(key, value)
	}

	responseWriter.WriteHeader(status)
	_, err = reader.WriteTo(responseWriter)

	if err != nil {
		finishWithError(responseWriter, err.Error())

		return
	}
}
