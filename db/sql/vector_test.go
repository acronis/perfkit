package sql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/acronis/perfkit/db"
)

func (suite *TestingSuite) makeVectorTestSession() (db.Database, db.Session, *db.Context) {
	var logger = &testLogger{t: suite.T()}

	dbo, err := db.Open(db.Config{
		ConnString:      suite.ConnString,
		MaxOpenConns:    16,
		MaxConnLifetime: 100 * time.Millisecond,
		QueryLogger:     logger,
	})

	require.NoError(suite.T(), err, "making test sqlSession")

	var tableSpec = testVectorTableDefinition(dbo.DialectName())

	if err = dbo.CreateTable("vector_perf_table", tableSpec, ""); err != nil {
		require.NoError(suite.T(), err, "init scheme")
	}

	var c = dbo.Context(context.Background(), false)

	s := dbo.Session(c)

	return dbo, s, c
}

func vectorCleanup(t *testing.T, dbo db.Database) {
	t.Helper()

	if err := dbo.DropTable("vector_perf_table"); err != nil {
		t.Error("drop table", err)
		return
	}
}

func testVectorTableDefinition(dia db.DialectName) *db.TableDefinition {
	return &db.TableDefinition{
		TableRows: []db.TableRow{
			{Name: "id", Type: db.DataTypeBigIntAutoIncPK},
			{Name: "embedding", Type: db.DataTypeVector3Float32},
		},
	}
}

func (suite *TestingSuite) TestVectorSearch() {
	var actualDialect, err = dbDialect(suite.ConnString)
	if err != nil {
		suite.T().Error(err)
		return
	}

	if actualDialect.name() != db.POSTGRES {
		suite.T().Skip("only postgresql supports vector search")
		return
	}

	d, s, c := suite.makeVectorTestSession()
	defer logDbTime(suite.T(), c)
	defer vectorCleanup(suite.T(), d)

	if err := s.BulkInsert("vector_perf_table", [][]interface{}{
		{[]float32{1, 2, 3}},
		{[]float32{4, 5, 6}},
	}, []string{"embedding"}); err != nil {
		suite.T().Error(err)
		return
	}

	if rows, err := s.Select("vector_perf_table",
		&db.SelectCtrl{
			Fields: []string{"id", "embedding"},
			Order:  []string{"nearest(embedding;L2;[3,1,2])"},
		}); err != nil {
		suite.T().Error(err)
		return
	} else {
		defer rows.Close()
		suite.T().Log("rows", rows)

		for rows.Next() {
			var id int
			var embedding []uint8
			if scanErr := rows.Scan(&id, &embedding); scanErr != nil {
				suite.T().Error(scanErr)
				return
			}
			suite.T().Log("row", id, string(embedding))
		}
	}
}
