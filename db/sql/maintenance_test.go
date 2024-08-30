package sql

import (
	"time"

	"github.com/acronis/perfkit/db"
)

func (suite *TestingSuite) TestSqlSchemaInit() {
	dbo, err := db.Open(db.Config{
		ConnString:      suite.ConnString,
		MaxOpenConns:    16,
		MaxConnLifetime: 100 * time.Millisecond,
	})

	if err != nil {
		suite.T().Error("db create", err)
		return
	}

	var exists bool
	if exists, err = dbo.TableExists("perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else if exists {
		suite.T().Error("table exists")
		return
	}

	var tableSpec = testTableDefinition(dbo.DialectName())

	if err = dbo.CreateTable("perf_table", tableSpec, ""); err != nil {
		suite.T().Error("create table", err)
		return
	}

	if exists, err = dbo.TableExists("perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else if !exists {
		suite.T().Error("table not exists")
		return
	}

	if dbo.DialectName() != db.CLICKHOUSE && dbo.DialectName() != db.CASSANDRA {
		if exists, err = dbo.IndexExists("perf_index", "perf_table"); err != nil {
			suite.T().Error(err)
			return
		} else if exists {
			suite.T().Error("index exists")
			return
		}

		if err = dbo.CreateIndex("perf_index", "perf_table", []string{"type", "name"}, db.IndexTypeBtree); err != nil {
			suite.T().Error("create index", err)
			return
		}

		if exists, err = dbo.IndexExists("perf_index", "perf_table"); err != nil {
			suite.T().Error(err)
			return
		} else if !exists {
			suite.T().Error("index not exists")
			return
		}

		if err = dbo.DropIndex("perf_index", "perf_table"); err != nil {
			suite.T().Error("create index", err)
			return
		}

		if exists, err = dbo.IndexExists("perf_index", "perf_table"); err != nil {
			suite.T().Error(err)
			return
		} else if exists {
			suite.T().Error("index exists")
			return
		}
	}

	if err = dbo.DropTable("perf_table"); err != nil {
		suite.T().Error("drop table", err)
		return
	}

	if exists, err = dbo.TableExists("perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else if exists {
		suite.T().Error("table exists")
		return
	}
}
