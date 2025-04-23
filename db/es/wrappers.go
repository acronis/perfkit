package es

import (
	"context"
	"fmt"
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

func logQuery(logger db.Logger, logTime bool, since time.Time, request string, headers string, body string) {
	if logger == nil {
		return
	}

	if logTime {
		var dur = time.Since(since)
		logger.Log("%s // %s\n%s\n\n%s", request, fmt.Sprintf("duration: %v", dur), headers, body)
	} else {
		logger.Log("%s\n%s\n\n%s", request, headers, body)
	}
}

// accountTime adds elapsed time since the given time to the atomic counter
func accountTime(t *atomic.Int64, since time.Time) {
	t.Add(time.Since(since).Nanoseconds())
}

// wrappedQuerier implements the querier interface with additional functionality:
// - measuring time of queries
// - logging of queries
type wrappedQuerier struct {
	q querier

	execTime  *atomic.Int64 // Execution time counter
	queryTime *atomic.Int64 // Query time counter

	logTime     bool
	queryLogger db.Logger
}

func (wq wrappedQuerier) insert(ctx context.Context, idxName indexName, query *BulkIndexRequest) (*BulkIndexResult, int, error) {
	defer accountTime(wq.execTime, time.Now())

	if wq.queryLogger != nil {
		defer func(since time.Time) {
			logQuery(wq.queryLogger, wq.logTime, since,
				fmt.Sprintf("POST /%s/_bulk", idxName),
				"Content-Type: application/x-ndjson",
				fmt.Sprintf("%s", query.Reader()))
		}(time.Now())
	}

	return wq.q.insert(ctx, idxName, query)
}

func (wq wrappedQuerier) search(ctx context.Context, idxName indexName, request *SearchRequest) ([]map[string]interface{}, error) {
	defer accountTime(wq.queryTime, time.Now())

	if wq.queryLogger != nil {
		defer func(since time.Time) {
			logQuery(wq.queryLogger, wq.logTime, since,
				fmt.Sprintf("POST /%s/_search", idxName),
				"Content-Type: application/json",
				request.String())
		}(time.Now())
	}

	return wq.q.search(ctx, idxName, request)
}

func (wq wrappedQuerier) count(ctx context.Context, idxName indexName, request *CountRequest) (int64, error) {
	defer accountTime(wq.queryTime, time.Now())

	if wq.queryLogger != nil {
		defer func(since time.Time) {
			logQuery(wq.queryLogger, wq.logTime, since,
				fmt.Sprintf("POST /%s/_search", idxName),
				"Content-Type: application/json",
				request.String())
		}(time.Now())
	}

	return wq.q.count(ctx, idxName, request)
}
