package es

import (
	"context"
	"fmt"
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
		MaxConnLifetime: 1000 * time.Millisecond,
		QueryLogger:     logger,
	})

	require.NoError(suite.T(), err, "making test esSession")

	var tableSpec = testVectorTableDefinition(dbo.DialectName())

	if err = dbo.CreateTable("vector_perf_table", tableSpec, ""); err != nil {
		require.NoError(suite.T(), err, "init scheme")
	}

	var c = dbo.Context(context.Background())

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
			{Name: "id", Type: db.DataTypeInt, Indexed: true},
			{Name: "embedding", Type: db.DataTypeVector3Float32, Indexed: true},
			{Name: "text", Type: db.DataTypeVarChar, Indexed: true},
		},
	}
}

func (suite *TestingSuite) TestVectorSearch() {
	d, s, c := suite.makeVectorTestSession()
	defer logDbTime(suite.T(), c)
	defer vectorCleanup(suite.T(), d)

	if err := s.BulkInsert("vector_perf_table", [][]interface{}{
		{int64(1), "text1", []float32{0.5, 10, 6}},
		{int64(2), "text2", []float32{-0.5, 10, 10}},
	}, []string{"id", "text", "embedding"}); err != nil {
		suite.T().Error(err)
		return
	}

	if rows, err := s.Select("vector_perf_table",
		&db.SelectCtrl{
			Fields: []string{"text", "embedding"},
			Order:  []string{"nearest(embedding;L2;[3,1,2])"},
		}); err != nil {
		suite.T().Error(err)
		return
	} else {
		defer rows.Close()
		suite.T().Log("rows", rows)

		for rows.Next() {
			var text string
			var embedding []float32
			if scanErr := rows.Scan(&text, &embedding); scanErr != nil {
				suite.T().Error(scanErr)
				return
			}
			suite.T().Log("row", text, fmt.Sprintf("%v", embedding))
		}
	}
}
