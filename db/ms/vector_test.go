package ms

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/acronis/perfkit/db"
)

func testVectorTableDefinition() *db.TableDefinition {
	return &db.TableDefinition{
		TableRows: []db.TableRow{
			db.TableRowItem{Name: "id", Type: db.DataTypeInt, PrimaryKey: true, Indexed: true},
			db.TableRowItem{Name: "embedding", Type: db.DataTypeVector3Float32, Indexed: true},
			db.TableRowItem{Name: "text", Type: db.DataTypeVarChar, Indexed: true},
		},
	}
}

func (suite *TestingSuite) makeVectorTestSession() (db.Database, db.Session, *db.Context) {
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

	var tableSpec = testVectorTableDefinition()

	// Clean up if left over from a previous run
	if exists, _ := dbo.TableExists("ms_vector_table"); exists {
		_ = dbo.DropTable("ms_vector_table")
	}

	if err = dbo.CreateTable("ms_vector_table", tableSpec, ""); err != nil {
		require.NoError(suite.T(), err, "init vector scheme")
	}

	var c = dbo.Context(context.Background(), false)
	s := dbo.Session(c)

	return dbo, s, c
}

func vectorCleanup(t *testing.T, dbo db.Database) {
	t.Helper()

	if err := dbo.DropTable("ms_vector_table"); err != nil {
		t.Error("drop vector table", err)
	}
}

func (suite *TestingSuite) TestVectorSearch() {
	d, s, c := suite.makeVectorTestSession()
	defer logDbTime(suite.T(), c)
	defer vectorCleanup(suite.T(), d)

	if err := s.BulkInsert("ms_vector_table", [][]interface{}{
		{int64(1), "text1", []float32{0.5, 10, 6}},
		{int64(2), "text2", []float32{-0.5, 10, 10}},
	}, []string{"id", "text", "embedding"}); err != nil {
		suite.T().Error(err)
		return
	}

	// Wait for async indexing
	time.Sleep(3 * time.Second)

	rows, err := s.Select("ms_vector_table",
		&db.SelectCtrl{
			Fields: []string{"text", "embedding"},
			Order:  []string{"nearest(embedding;L2;[3,1,2])"},
		})
	if err != nil {
		suite.T().Error(err)
		return
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var text string
		var embedding []float32
		if scanErr := rows.Scan(&text, &embedding); scanErr != nil {
			suite.T().Error(scanErr)
			return
		}
		suite.T().Log("row", text, fmt.Sprintf("%v", embedding))
		suite.NotEmpty(text)
		suite.Len(embedding, 3)
		count++
	}

	suite.Equal(2, count, "expected 2 rows from vector search")
}
