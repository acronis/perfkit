package sql

import (
	"strings"

	"github.com/acronis/perfkit/db"
)

func (suite *TestingSuite) TestDatabaseInfoProvider() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	var qry string
	switch d.DialectName() {
	case db.CASSANDRA:
		qry = `
			BEGIN BATCH
				INSERT INTO perf_table (origin, type, name) VALUES (2, 2, 'test');
				INSERT INTO perf_table (origin, type, name) VALUES (3, 4, 'perf');
			APPLY BATCH;
		`
	default:
		qry = `
			INSERT INTO perf_table (origin, type, name)
			VALUES (2, 2, 'test'),
			       (3, 4, 'perf');
		`
	}

	if _, err := s.Exec(qry); err != nil {
		suite.T().Error(err)
		return
	}

	dia, version, err := d.GetVersion()
	if err != nil {
		suite.T().Error(err)
		return
	}

	content, dbInfo, err := d.GetInfo(version)
	if err != nil {
		suite.T().Error(err)
		return
	}

	// SQLite and ClickHouse do not support SHOW ALL command
	if dia != db.SQLITE && dia != db.CLICKHOUSE {
		if len(content) == 0 {
			suite.T().Error("empty content")
			return
		}

		suite.T().Log(content)
	}

	if dbInfo == nil {
		suite.T().Error("nil dbInfo")
		return
	}

	dbInfo.ShowRecommendations()

	var schemaInfo []string
	if schemaInfo, err = d.GetTablesSchemaInfo([]string{"perf_table"}); err != nil {
		suite.T().Error(err)
		return
	}

	suite.T().Log(strings.Join(schemaInfo, "\n"))

	if d.DialectName() != db.MYSQL {
		var volumeInfo []string
		if volumeInfo, err = d.GetTablesVolumeInfo([]string{"perf_table"}); err != nil {
			suite.T().Error(err)
			return
		}

		suite.T().Log(strings.Join(volumeInfo, "\n"))
	}
}
