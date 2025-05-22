package sql

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/acronis/perfkit/db"
)

// scanVector converts raw vector data to []float32 based on dialect
func scanVector(src interface{}, dialect db.DialectName) ([]float32, error) {
	if src == nil {
		return nil, nil
	}

	switch dialect {
	case db.MYSQL:
		// MySQL returns binary data
		bytes, ok := src.([]uint8)
		if !ok {
			return nil, fmt.Errorf("invalid vector type for MySQL: %T", src)
		}

		// Each float32 is 4 bytes
		if len(bytes)%4 != 0 {
			return nil, fmt.Errorf("invalid vector length: %d", len(bytes))
		}

		vector := make([]float32, len(bytes)/4)
		for i := 0; i < len(bytes); i += 4 {
			// Convert 4 bytes to float32
			bits := uint32(bytes[i]) | uint32(bytes[i+1])<<8 | uint32(bytes[i+2])<<16 | uint32(bytes[i+3])<<24
			vector[i/4] = math.Float32frombits(bits)
		}
		return vector, nil

	case db.POSTGRES:
		// PostgreSQL returns []float32 directly
		stringVector, ok := src.([]uint8)
		if !ok {
			return nil, fmt.Errorf("invalid vector type for PostgreSQL: %T", src)
		}

		var rawVector, err = db.ParseVector(string(stringVector), ",")
		if err != nil {
			return nil, fmt.Errorf("failed to parse vector: %v", err)
		}

		var vector []float32
		for _, v := range rawVector {
			var f float64
			v = strings.TrimSpace(v) // Removes any leading/trailing whitespace
			f, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse vector value: %v", err)
			}
			vector = append(vector, float32(f))
		}

		return vector, nil

	default:
		return nil, fmt.Errorf("unsupported dialect for vector scanning: %v", dialect)
	}
}

func (suite *TestingSuite) makeVectorTestSession() (db.Database, db.Session, *db.Context) {
	var logger = newTestLogger(suite.T())

	dbo, err := db.Open(db.Config{
		ConnString:               suite.ConnString,
		MaxOpenConns:             16,
		MaxConnLifetime:          100 * time.Millisecond,
		QueryStringInterpolation: true,
		LogOperationsTime:        true,
		QueryLogger:              logger,
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
			db.TableRowItem{Name: "id", Type: db.DataTypeBigIntAutoIncPK},
			db.TableRowItem{Name: "embedding", Type: db.DataTypeVector3Float32},
		},
	}
}

func (suite *TestingSuite) TestVectorSearch() {
	var actualDialect, err = dbDialect(suite.ConnString)
	if err != nil {
		suite.T().Error(err)
		return
	}

	if actualDialect.name() != db.POSTGRES && actualDialect.name() != db.MYSQL {
		suite.T().Skip("only postgresql and MariaDB supports vector search")
		return
	}

	d, s, c := suite.makeVectorTestSession()
	defer logDbTime(suite.T(), c)
	defer vectorCleanup(suite.T(), d)

	// Insert test vectors
	if err := s.BulkInsert("vector_perf_table", [][]interface{}{
		{[]float32{1, 2, 3}},
		{[]float32{4, 5, 6}},
	}, []string{"embedding"}); err != nil {
		suite.T().Error(err)
		return
	}

	// Test vector similarity search using L2 distance
	if rows, err := s.Select("vector_perf_table",
		&db.SelectCtrl{
			Fields: []string{"id", "embedding"},
			Order:  []string{"nearest(embedding;L2;[3,1,2])"},
		}); err != nil {
		suite.T().Error(err)
		return
	} else {
		defer rows.Close()

		var results []struct {
			id        int64
			embedding []float32
		}

		for rows.Next() {
			var r struct {
				id        int64
				embedding []float32
			}
			var rawEmbedding interface{}
			if err := rows.Scan(&r.id, &rawEmbedding); err != nil {
				suite.T().Error(err)
				return
			}

			// Convert raw embedding to []float32 based on dialect
			r.embedding, err = scanVector(rawEmbedding, actualDialect.name())
			if err != nil {
				suite.T().Error(err)
				return
			}

			results = append(results, r)
		}

		// Verify results are ordered by distance
		require.Equal(suite.T(), 2, len(results), "should return 2 results")
		require.Equal(suite.T(), []float32{1, 2, 3}, results[0].embedding, "first result should be [1,2,3]")
		require.Equal(suite.T(), []float32{4, 5, 6}, results[1].embedding, "second result should be [4,5,6]")
	}
}
