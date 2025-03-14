package vector_search

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	tests := []*engine.TestDesc{
		// Vector search tests
		&TestInsertVector768MultiValue,
		&TestSelectVector768NearestL2,
	}

	tables := map[string]engine.TestTable{
		TestTableVector768.TableName: TestTableVector768,
	}

	scenario := &engine.TestScenario{
		Name:   "vector-search",
		Tests:  tests,
		Tables: tables,
	}

	if err := engine.RegisterTestScenario(scenario); err != nil {
		panic(err)
	}
}

// TestTableVector768 is table to store 768-dimensions vector objects
var TestTableVector768 = engine.TestTable{
	TableName: "acronis_db_bench_vector_768",
	Databases: []db.DialectName{db.POSTGRES, db.ELASTICSEARCH, db.OPENSEARCH},
	Columns: [][]interface{}{
		{"id", "dataset.id"},
		{"embedding", "dataset.emb.list.item"},
	},
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		var tableRows []db.TableRow

		tableRows = append(tableRows,
			db.TableRow{Name: "id", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRow{Name: "embedding", Type: db.DataTypeVector768Float32, Indexed: true},
		)

		var tableDef = &db.TableDefinition{
			TableRows: tableRows,
		}

		if dialect == db.ELASTICSEARCH {
			tableDef.Resilience.NumberOfReplicas = 2
		}

		return tableDef
	},
}

// TestInsertVector768MultiValue inserts rows into the 'heavy' table using golang DB query builder
var TestInsertVector768MultiValue = engine.TestDesc{
	Name:        "insert-vector-768-multivalue",
	Metric:      "rows/sec",
	Description: "insert a 768-dim vectors with ids into the 'vector' table by batches",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableVector768,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, engine.InsertMultiValueDataWorker, 0)
	},
}

// TestSelectVector768NearestL2 selects k nearest vectors by L2 from the 'vector' table to the given vector
var TestSelectVector768NearestL2 = engine.TestDesc{
	Name:        "select-vector-768-nearest-l2",
	Metric:      "rows/sec",
	Description: "selects k nearest vectors by L2 norm from the 'vector' table to the given 768-dim vector",
	Category:    engine.TestSelect,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableVector768,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var colConfs = []benchmark.DBFakeColumnConf{
			{ColumnName: "id", ColumnType: "dataset.id"},
			{ColumnName: "embedding", ColumnType: "dataset.emb.list.item"},
		}

		var idToRead int64
		var vectorToRead = make([]float64, 768)

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			b := worker.Benchmark
			_, vals, err := b.Randomizer.GenFakeData(&colConfs, false)
			if err != nil {
				b.Exit(err)
			}

			var vec = "[" + strings.Trim(strings.Replace(fmt.Sprint(vals[1]), " ", ", ", -1), "[]") + "]"
			return []string{fmt.Sprintf("nearest(embedding;L2;%s)", vec)}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"id", "embedding"}, []interface{}{&idToRead, &vectorToRead}, nil, orderBy, 1)
	},
}
