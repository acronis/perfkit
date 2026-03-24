// Package ms provides an implementation of the db.Database interface for Meilisearch.
// @cpt-perfkit-db-component-ms-adapter
package ms

import (
	"context"
	"fmt"
	"net/url"

	"github.com/meilisearch/meilisearch-go"
	"go.uber.org/atomic"

	"github.com/acronis/perfkit/db"
)

// @cpt-perfkit-db-algo-ms-adapter-register
// nolint: gochecknoinits // remove init() when we will have a better way to register connectors
func init() {
	for _, msNameStyle := range []string{"ms", "meilisearch"} {
		if err := db.Register(msNameStyle, &msConnector{}); err != nil {
			panic(err)
		}
	}
}

type msConnector struct{}

// @cpt-perfkit-db-algo-ms-adapter-connect
func (c *msConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	host, apiKey, err := parseMeilisearchURI(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("db: meilisearch: %v", err)
	}

	var opts []meilisearch.Option
	if apiKey != "" {
		opts = append(opts, meilisearch.WithAPIKey(apiKey))
	}

	client := meilisearch.New(host, opts...)

	if _, err := client.Health(); err != nil {
		return nil, fmt.Errorf("db: failed ping meilisearch at %v, err: %v", host, err)
	}

	return &msDatabase{
		client:         client,
		host:           host,
		logTime:        cfg.LogOperationsTime,
		readRowsLogger: cfg.ReadRowsLogger,
	}, nil
}

func (c *msConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.MEILISEARCH, nil
}

// parseMeilisearchURI parses a Meilisearch connection URI.
// Supported formats:
//   - ms://host:port?apiKey=KEY
//   - meilisearch://host:port?apiKey=KEY
func parseMeilisearchURI(cs string) (host string, apiKey string, err error) {
	u, err := url.Parse(cs)
	if err != nil {
		return "", "", fmt.Errorf("cannot parse connection url %v, err: %v", cs, err)
	}

	scheme := "http"
	if u.Query().Get("tls") == "true" {
		scheme = "https"
	}

	host = fmt.Sprintf("%s://%s", scheme, u.Host)
	apiKey = u.Query().Get("apiKey")

	return host, apiKey, nil
}

type msGateway struct {
	client  meilisearch.ServiceManager
	ctx     *db.Context
	logTime bool

	readRowsLogger db.Logger
}

type msSession struct {
	msGateway
}

// @cpt-perfkit-db-algo-ms-adapter-passthrough-tx
func (s *msSession) Transact(fn func(tx db.DatabaseAccessor) error) error {
	return fn(s)
}

type msDatabase struct {
	client         meilisearch.ServiceManager
	host           string
	logTime        bool
	readRowsLogger db.Logger
}

// Ping pings the Meilisearch server
func (d *msDatabase) Ping(ctx context.Context) error {
	if _, err := d.client.Health(); err != nil {
		return fmt.Errorf("db: failed ping meilisearch, err: %v", err)
	}
	return nil
}

func (d *msDatabase) DialectName() db.DialectName {
	return db.MEILISEARCH
}

func (d *msDatabase) UseTruncate() bool {
	return false
}

func (d *msDatabase) GetVersion() (db.DialectName, string, error) {
	v, err := d.client.Version()
	if err != nil {
		return db.MEILISEARCH, "", fmt.Errorf("db: meilisearch: failed to get version: %v", err)
	}
	return db.MEILISEARCH, v.PkgVersion, nil
}

func (d *msDatabase) GetInfo(version string) (ret []string, dbInfo *db.Info, err error) {
	ret = append(ret, "Meilisearch")
	return ret, nil, nil
}

func (d *msDatabase) ApplyMigrations(tableName, tableMigrationSQL string) error {
	return nil
}

func (d *msDatabase) IndexExists(indexName string, tableName string) (bool, error) {
	return true, nil
}

func (d *msDatabase) CreateIndex(indexName string, tableName string, columns []string, indexType db.IndexType) error {
	return nil
}

func (d *msDatabase) DropIndex(indexName string, tableName string) error {
	return nil
}

func (d *msDatabase) ReadConstraints() ([]db.Constraint, error) {
	return nil, nil
}

func (d *msDatabase) AddConstraints(constraints []db.Constraint) error {
	return nil
}

func (d *msDatabase) DropConstraints(constraints []db.Constraint) error {
	return nil
}

func (d *msDatabase) CreateSequence(sequenceName string) error {
	return nil
}

func (d *msDatabase) DropSequence(sequenceName string) error {
	return nil
}

func (d *msDatabase) GetTablesSchemaInfo(tableNames []string) ([]string, error) {
	return nil, nil
}

func (d *msDatabase) GetTablesVolumeInfo(tableNames []string) ([]string, error) {
	return nil, nil
}

func (d *msDatabase) Context(ctx context.Context, explain bool) *db.Context {
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

func (d *msDatabase) Session(c *db.Context) db.Session {
	return &msSession{
		msGateway: msGateway{
			client:         d.client,
			ctx:            c,
			logTime:        d.logTime,
			readRowsLogger: d.readRowsLogger,
		},
	}
}

func (d *msDatabase) RawSession() interface{} {
	return d.client
}

func (d *msDatabase) Stats() *db.Stats {
	return nil
}

func (d *msDatabase) Close() error {
	return nil
}
