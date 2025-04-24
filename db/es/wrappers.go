package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/acronis/perfkit/db"
)

/*
Package es provides an implementation of the db.Database interface for Elasticsearch.

This file contains all wrapper implementations that handle:
- Query logging: logs all Elasticsearch queries and operations
- Performance measurements: tracks time spent in various database operations
*/

// accountTime adds elapsed time since the given time to the atomic counter
func accountTime(t *atomic.Int64, since time.Time) {
	t.Add(time.Since(since).Nanoseconds())
}

// wrappedRows is a struct for storing and logging ES rows results
type wrappedRows struct {
	rows *esRows

	logTime        bool
	readRowsLogger db.Logger
	printed        int
}

// Next advances the cursor to the next row, returning false if no more rows
func (r *wrappedRows) Next() bool {
	return r.rows.Next()
}

// Err returns any error that was encountered during iteration
func (r *wrappedRows) Err() error {
	return r.rows.Err()
}

const maxRowsToPrint = 10

func logRow(logger db.Logger, logTime bool, since time.Time, dest ...interface{}) {
	if logger == nil {
		return
	}

	var values = db.DumpRecursive(dest, " ")
	if logTime {
		var dur = time.Since(since)
		logger.Log("Row: %s -- %s", values, fmt.Sprintf("parse duration: %v", dur))
	} else {
		logger.Log("Row: %s", values)
	}
}

// Scan copies the columns in the current row into the values pointed at by dest.
// Logs the scanned values if readRowsLogger is configured, up to maxRowsToPrint rows.
func (r *wrappedRows) Scan(dest ...interface{}) error {
	var since = time.Now()
	var err = r.rows.Scan(dest...)

	if r.readRowsLogger != nil {
		if r.printed >= maxRowsToPrint {
			return err
		} else if r.printed == maxRowsToPrint {
			r.readRowsLogger.Log("... truncated ...")
			r.printed++
			return err
		}

		// Create a single log line with all columns
		logRow(r.readRowsLogger, r.logTime, since, dest...)
		r.printed++
	}

	return err
}

// Close closes the rows iterator
func (r *wrappedRows) Close() error {
	return r.rows.Close()
}

// wrappedQuerier implements the querier interface with additional functionality:
// - measuring time of queries
// - logging of queries
type wrappedQuerier struct {
	q querier

	execTime  *atomic.Int64 // Execution time counter
	queryTime *atomic.Int64 // Query time counter
}

func (wq wrappedQuerier) insert(ctx context.Context, idxName indexName, query *BulkIndexRequest) (*BulkIndexResult, int, error) {
	defer accountTime(wq.execTime, time.Now())

	return wq.q.insert(ctx, idxName, query)
}

func (wq wrappedQuerier) search(ctx context.Context, idxName indexName, request *SearchRequest) ([]map[string]interface{}, error) {
	defer accountTime(wq.queryTime, time.Now())

	return wq.q.search(ctx, idxName, request)
}

func (wq wrappedQuerier) count(ctx context.Context, idxName indexName, request *CountRequest) (int64, error) {
	defer accountTime(wq.queryTime, time.Now())

	return wq.q.count(ctx, idxName, request)
}

// httpLoggingTransport wraps a http.RoundTripper and logs the request and response
type httpWrapperTransport struct {
	transport   http.RoundTripper
	queryLogger db.Logger
	logTime     bool
}

// sanitizeHeaders removes sensitive information from request headers
func sanitizeHeaders(dump string) string {
	// This is a simplified implementation - we could make this more sophisticated
	// to properly handle sensitive headers like Authorization, etc.
	lines := strings.Split(dump, "\n")

	for i, line := range lines {
		if strings.HasPrefix(line, "Authorization:") {
			lines[i] = "Authorization: [REDACTED]"
		}
	}

	return strings.Join(lines, "\n")
}

// beautifyJSON formats JSON string with proper indentation
func beautifyJSON(input string) string {
	var out bytes.Buffer
	err := json.Indent(&out, []byte(input), "", "   ")
	if err != nil {
		return input
	}
	return out.String()
}

// beautifyRequestBody beautifies JSON in HTTP request body if Content-Type is application/json
func beautifyRequestBody(dump string) string {
	// Split the dump into headers and body
	parts := strings.Split(dump, "\r\n\r\n")
	if len(parts) < 2 {
		return dump
	}

	headers := parts[0]
	body := parts[1]

	// Check if the Content-Type is application/json
	if strings.Contains(headers, "Content-Type: application/json") {
		beautifiedBody := beautifyJSON(body)
		return headers + "\r\n\r\n" + beautifiedBody
	}

	return dump
}

// RoundTrip implements the http.RoundTripper interface
func (t *httpWrapperTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var startTime = time.Now()

	// Dump the request for logging
	var reqDump []byte
	var err error

	// Use DumpRequestOut to include headers but exclude sensitive auth information
	if req.Body != nil {
		// Save the body to be restored later
		var bodyBytes []byte
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body.Close()

		// Create two new readers: one for dumping, one for the actual request
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		bodyForDump := io.NopCloser(bytes.NewBuffer(bodyBytes))

		// Create a temporary request for dumping with the same body
		reqForDump := req.Clone(req.Context())
		reqForDump.Body = bodyForDump

		reqDump, err = httputil.DumpRequestOut(reqForDump, true)
		if err != nil {
			reqDump = []byte(fmt.Sprintf("Failed to dump request: %v", err))
		}
	} else {
		reqDump, err = httputil.DumpRequestOut(req, true)
		if err != nil {
			reqDump = []byte(fmt.Sprintf("Failed to dump request: %v", err))
		}
	}

	// Make the actual request
	resp, err := t.transport.RoundTrip(req)

	// Log the request and response if we have a logger
	if t.queryLogger != nil {
		defer func() {
			// Extract request method and path
			method := req.Method
			path := req.URL.Path

			// Sanitize dump by removing sensitive headers
			sanitizedDump := sanitizeHeaders(string(reqDump))

			// Beautify JSON if Content-Type is application/json
			sanitizedDump = beautifyRequestBody(sanitizedDump)

			// Format and log
			if t.logTime {
				duration := time.Since(startTime)
				t.queryLogger.Log("%s %s // %s\n%s", method, path,
					fmt.Sprintf("duration: %v", duration), sanitizedDump)
			} else {
				t.queryLogger.Log("%s %s\n%s", method, path, sanitizedDump)
			}
		}()
	}

	return resp, err
}
