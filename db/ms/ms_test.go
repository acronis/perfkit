package ms

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/acronis/perfkit/db"
)

const msConnString = "ms://localhost:7700"

type TestingSuite struct {
	suite.Suite
	ConnString string
}

func TestDatabaseSuiteMeilisearch(t *testing.T) {
	suite.Run(t, &TestingSuite{ConnString: msConnString})
}

type testLogger struct {
	t *testing.T
}

func newTestLogger(t *testing.T) db.Logger {
	return &testLogger{t: t}
}

func (l *testLogger) Log(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func testTableDefinition() *db.TableDefinition {
	return &db.TableDefinition{
		TableRows: []db.TableRow{
			db.TableRowItem{Name: "id", Type: db.DataTypeId, PrimaryKey: true, Indexed: true},
			db.TableRowItem{Name: "uuid", Type: db.DataTypeUUID, Indexed: true},
			db.TableRowItem{Name: "type", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRowItem{Name: "policy_name", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRowItem{Name: "resource_name", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRowItem{Name: "score", Type: db.DataTypeInt, Indexed: true},
		},
	}
}

func (suite *TestingSuite) makeTestSession() (db.Database, db.Session, *db.Context) {
	var logger = newTestLogger(suite.T())

	dbo, err := db.Open(db.Config{
		ConnString:        suite.ConnString,
		MaxOpenConns:      16,
		MaxConnLifetime:   1000 * time.Millisecond,
		QueryLogger:       logger,
		ReadRowsLogger:    logger,
		LogOperationsTime: true,
	})

	require.NoError(suite.T(), err, "making test msSession")

	var tableSpec = testTableDefinition()

	// Clean up if left over from a previous run
	if exists, _ := dbo.TableExists("ms_perf_table"); exists {
		_ = dbo.DropTable("ms_perf_table")
	}

	if err = dbo.CreateTable("ms_perf_table", tableSpec, ""); err != nil {
		require.NoError(suite.T(), err, "init scheme")
	}

	var c = dbo.Context(context.Background(), false)
	s := dbo.Session(c)

	return dbo, s, c
}

func logDbTime(t *testing.T, c *db.Context) {
	t.Helper()
	db.DumpExecutionTime(newTestLogger(t), c)
}

func cleanup(t *testing.T, dbo db.Database) {
	t.Helper()

	exists, err := dbo.TableExists("ms_perf_table")
	if err != nil {
		t.Error("check table exists", err)
		return
	}

	if !exists {
		return
	}

	if err := dbo.DropTable("ms_perf_table"); err != nil {
		t.Error("drop table", err)
		return
	}
}

func TestParseMeilisearchURI(t *testing.T) {
	tests := []struct {
		name       string
		uri        string
		wantHost   string
		wantAPIKey string
		wantErr    bool
	}{
		{
			name:       "basic uri",
			uri:        "ms://localhost:7700",
			wantHost:   "http://localhost:7700",
			wantAPIKey: "",
		},
		{
			name:       "with api key",
			uri:        "ms://localhost:7700?apiKey=masterKey123",
			wantHost:   "http://localhost:7700",
			wantAPIKey: "masterKey123",
		},
		{
			name:       "with tls",
			uri:        "meilisearch://meili.example.com:443?tls=true&apiKey=secret",
			wantHost:   "https://meili.example.com:443",
			wantAPIKey: "secret",
		},
		{
			name:       "meilisearch scheme",
			uri:        "meilisearch://127.0.0.1:7700",
			wantHost:   "http://127.0.0.1:7700",
			wantAPIKey: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, apiKey, err := parseMeilisearchURI(tt.uri)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantHost, host)
				require.Equal(t, tt.wantAPIKey, apiKey)
			}
		})
	}
}
