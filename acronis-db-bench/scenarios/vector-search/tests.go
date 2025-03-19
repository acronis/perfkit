package vector_search

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

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

		var orderBy = func(b *benchmark.Benchmark, workerId int) []string { //nolint:revive
			var _, vals = b.GenFakeData(workerId, &colConfs, false)
			var vec = "[" + strings.Trim(strings.Replace(fmt.Sprint(vals[1]), " ", ", ", -1), "[]") + "]"
			return []string{fmt.Sprintf("nearest(embedding;L2;%s)", vec)}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"id", "embedding"}, []interface{}{&idToRead, &vectorToRead}, nil, orderBy, 1)
	},
}
