package es

import (
	"time"

	"github.com/acronis/perfkit/db"
)

func (suite *TestingSuite) TestElasticSearchSchemaInit() {
	dbo, err := db.Open(db.Config{
		ConnString:      suite.ConnString,
		MaxOpenConns:    16,
		MaxConnLifetime: 1000 * time.Millisecond,
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

	var tableSpec = testTableDefinition()

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
