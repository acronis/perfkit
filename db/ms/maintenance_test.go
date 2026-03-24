package ms

import (
	"time"

	"github.com/acronis/perfkit/db"
)

func (suite *TestingSuite) TestMeilisearchSchemaInit() {
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
	if exists, err = dbo.TableExists("ms_perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else if exists {
		// Clean up from previous test run
		if err = dbo.DropTable("ms_perf_table"); err != nil {
			suite.T().Error("drop leftover table", err)
			return
		}
	}

	var tableSpec = testTableDefinition()

	if err = dbo.CreateTable("ms_perf_table", tableSpec, ""); err != nil {
		suite.T().Error("create table", err)
		return
	}

	if exists, err = dbo.TableExists("ms_perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else if !exists {
		suite.T().Error("table not exists after create")
		return
	}

	if err = dbo.DropTable("ms_perf_table"); err != nil {
		suite.T().Error("drop table", err)
		return
	}

	// After drop, give Meilisearch time to process
	time.Sleep(1 * time.Second)

	if exists, err = dbo.TableExists("ms_perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else if exists {
		suite.T().Error("table still exists after drop")
		return
	}
}
