package composite_retrieval

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var tg = engine.NewTestGroup("Composite retrieval tests group")

	tg.Add(&TestInsertEmailSecurityMultiValue)
	tg.Add(&TestSelectEmailByEmbeddingNearestL2)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}

// TestTableEmailSecurity is table to store email security objects
var TestTableEmailSecurity = engine.TestTable{
	TableName: "acronis_db_bench_email_security",
	Databases: []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Columns: [][]interface{}{
		{"id", "dataset.id"},
		{"date", "dataset.Date"},
		{"sender", "dataset.From"},
		{"recipient", "dataset.To"},
		{"subject", "dataset.Subject"},
		{"body", "dataset.Body"},
		{"embedding", "dataset.Embedding.list.element"},
	},
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		var tableRows []db.TableRow

		tableRows = append(tableRows,
			db.TableRow{Name: "id", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRow{Name: "date", Type: db.DataTypeDateTime, Indexed: true},
			db.TableRow{Name: "sender", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRow{Name: "recipient", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRow{Name: "subject", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRow{Name: "body", Type: db.DataTypeText, Indexed: true},
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

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			b := worker.Benchmark
			_, vals, err := b.Randomizer.GenFakeData(&colConfs, false)
			if err != nil {
				b.Exit(err)
			}
			var vec = "[" + strings.Trim(strings.Replace(fmt.Sprint(vals[1]), " ", ", ", -1), "[]") + "]"
			return []string{fmt.Sprintf("nearest(embedding;L2;%s)", vec)}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"body", "embedding"}, []interface{}{&bodyToRead, &vectorToRead}, nil, orderBy, 1)
	},
}
