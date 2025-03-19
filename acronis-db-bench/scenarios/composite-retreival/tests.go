package composite_retreival

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestInsertEmailSecurityMultiValue inserts email security data into the 'email_security' table
var TestInsertEmailSecurityMultiValue = engine.TestDesc{
	Name:        "insert-email-security-multivalue",
	Metric:      "rows/sec",
	Description: "insert an email security data into the 'email_security' table by batches",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailSecurity,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, engine.InsertMultiValueDataWorker, 0)
	},
}

// TestSelectEmailByEmbeddingNearestL2 selects k nearest vectors by L2 from the 'email_security' table to the given vector
var TestSelectEmailByEmbeddingNearestL2 = engine.TestDesc{
	Name:        "select-email-security-768-nearest-l2",
	Metric:      "rows/sec",
	Description: "selects k nearest emails by vector L2 norm from the 'email_security' table to the given vectorized 768-dim email",
	Category:    engine.TestSelect,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailSecurity,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var colConfs = []benchmark.DBFakeColumnConf{
			{ColumnName: "id", ColumnType: "dataset.id"},
			// {ColumnName: "body", ColumnType: "dataset.Body"},
			{ColumnName: "embedding", ColumnType: "dataset.Embedding.list.element"},
		}

		var bodyToRead string
		var vectorToRead = make([]float64, 768)

		var orderBy = func(b *benchmark.Benchmark, workerId int) []string { //nolint:revive
			var _, vals = b.GenFakeData(workerId, &colConfs, false)
			var vec = "[" + strings.Trim(strings.Replace(fmt.Sprint(vals[1]), " ", ", ", -1), "[]") + "]"
			return []string{fmt.Sprintf("nearest(embedding;L2;%s)", vec)}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"body", "embedding"}, []interface{}{&bodyToRead, &vectorToRead}, nil, orderBy, 1)
	},
}
