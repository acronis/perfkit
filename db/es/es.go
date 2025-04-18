// Package es provides an implementation of the db.Database interface for Elasticsearch.
package es

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/atomic"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"
)

type querier interface {
	insert(ctx context.Context, idxName indexName, query *BulkIndexRequest) (*BulkIndexResult, int, error)
	search(ctx context.Context, idxName indexName, query *SearchRequest) ([]map[string]interface{}, error)
	count(ctx context.Context, idxName indexName, query *CountRequest) (int64, error)
}

type accessor interface {
	querier

	ping(ctx context.Context) error
	stats() db.Stats
	rawSession() interface{}
	close() error
}

type esGateway struct {
	q   querier
	ctx *db.Context
}

type esSession struct {
	esGateway
}

func (s *esSession) Transact(fn func(tx db.DatabaseAccessor) error) error {
	return fn(s)
}

type esDatabase struct {
	rw      accessor
	mig     migrator
	dialect dialect

	queryLogger logger.Logger
}

// Ping pings the DB
func (d *esDatabase) Ping(ctx context.Context) error {
	return d.rw.ping(ctx)
}

func (d *esDatabase) DialectName() db.DialectName {
	return db.ELASTICSEARCH
}

func (d *esDatabase) UseTruncate() bool {
	return false
}

func (d *esDatabase) GetVersion() (db.DialectName, string, error) {
	return getVersion(d.dialect)
}

func (d *esDatabase) GetInfo(version string) (ret []string, dbInfo *db.Info, err error) {
	return getInfo(d.rw, version)
}

func (d *esDatabase) ApplyMigrations(tableName, tableMigrationSQL string) error {
	return nil
}

func (d *esDatabase) TableExists(tableName string) (bool, error) {
	return indexExists(d.mig, tableName)
}

func (d *esDatabase) CreateTable(tableName string, tableDefinition *db.TableDefinition, tableMigrationDDL string) error {
	return createIndex(d.dialect, d.mig, tableName, tableDefinition, tableMigrationDDL)
}

func (d *esDatabase) DropTable(name string) error {
	return dropIndex(d.mig, name)
}

func (d *esDatabase) IndexExists(indexName string, tableName string) (bool, error) {
	return true, nil
}

func (d *esDatabase) CreateIndex(indexName string, tableName string, columns []string, indexType db.IndexType) error {
	return nil
}

func (d *esDatabase) DropIndex(indexName string, tableName string) error {
	return nil
}

func (d *esDatabase) ReadConstraints() ([]db.Constraint, error) {
	return nil, nil
}

func (d *esDatabase) AddConstraints(constraints []db.Constraint) error {
	return nil
}

func (d *esDatabase) DropConstraints(constraints []db.Constraint) error {
	return nil
}

func (d *esDatabase) CreateSequence(sequenceName string) error {
	return nil
}

func (d *esDatabase) DropSequence(sequenceName string) error {
	return nil
}

func (d *esDatabase) GetTablesSchemaInfo(tableNames []string) ([]string, error) {
	return getTablesSchemaInfo(d.rw, tableNames)
}

func (d *esDatabase) GetTablesVolumeInfo(tableNames []string) ([]string, error) {
	return getTablesVolumeInfo(d.rw, tableNames)
}

func (d *esDatabase) Context(ctx context.Context, explain bool) *db.Context {
	return &db.Context{
		Ctx:         ctx,
		Explain:     explain,
		BeginTime:   atomic.NewInt64(0),
		PrepareTime: atomic.NewInt64(0),
		ExecTime:    atomic.NewInt64(0),
		QueryTime:   atomic.NewInt64(0),
		DeallocTime: atomic.NewInt64(0),
		CommitTime:  atomic.NewInt64(0),
	}
}

func (d *esDatabase) Session(c *db.Context) db.Session {
	return &esSession{
		esGateway: esGateway{
			q: timedQuerier{q: d.rw,
				execTime:    c.ExecTime,
				queryTime:   c.QueryTime,
				queryLogger: d.queryLogger,
			},
			ctx: c,
		},
	}
}

func (d *esDatabase) RawSession() interface{} {
	return d.rw.rawSession()
}

func (d *esDatabase) Stats() *db.Stats {
	return nil
}

func (d *esDatabase) Close() error {
	var err = d.rw.close()
	if err != nil {
		return fmt.Errorf("close failed: %w", err)
	}

	return nil
}

type timedQuerier struct {
	q querier

	execTime  *atomic.Int64 // *time.Duration
	queryTime *atomic.Int64 // *time.Duration

	queryLogger logger.Logger
}

func accountTime(t *atomic.Int64, since time.Time) {
	t.Add(time.Since(since).Nanoseconds())
}

func (tq timedQuerier) insert(ctx context.Context, idxName indexName, query *BulkIndexRequest) (*BulkIndexResult, int, error) {
	defer accountTime(tq.execTime, time.Now())

	if tq.queryLogger != nil {
		tq.queryLogger.Info("bulk insert:\n%v", query.Reader())
	}

	return tq.q.insert(ctx, idxName, query)
}

func (tq timedQuerier) search(ctx context.Context, idxName indexName, request *SearchRequest) ([]map[string]interface{}, error) {
	defer accountTime(tq.queryTime, time.Now())

	if tq.queryLogger != nil {
		tq.queryLogger.Info("search:\n%s", request.String())
	}

	return tq.q.search(ctx, idxName, request)
}

func (tq timedQuerier) count(ctx context.Context, idxName indexName, request *CountRequest) (int64, error) {
	defer accountTime(tq.queryTime, time.Now())

	if tq.queryLogger != nil {
		tq.queryLogger.Info("count:\n%s", request.String())
	}

	return tq.q.count(ctx, idxName, request)
}

type dialect interface {
	name() db.DialectName
	getVectorType() fieldType
}
