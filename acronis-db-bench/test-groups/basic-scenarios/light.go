package basic_scenarios

import (
	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestTableLight is table to store light objects
var TestTableLight = engine.TestTable{
	TableName: "acronis_db_bench_light",
	Databases: engine.ALL,
	Columns: [][]interface{}{
		{"id", "autoinc"},
		{"uuid", "uuid"},
	},
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		return &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "id", Type: db.DataTypeBigIntAutoIncPK},
				{Name: "uuid", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
			},
		}
	},
	CreateQuery: `create table {table} (
			id {$bigint_autoinc_pk},
			uuid {$uuid} {$notnull}
			) {$engine};`,
}

// TestInsertLight inserts a row into the 'light' table
var TestInsertLight = engine.TestDesc{
	Name:        "insert-light",
	Metric:      "rows/sec",
	Description: "insert a row into the 'light' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   engine.ALL,
	Table:       TestTableLight,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertLightPrepared inserts a row into the 'light' table using prepared statement for the batch
var TestInsertLightPrepared = engine.TestDesc{
	Name:        "insert-light-prepared",
	Metric:      "rows/sec",
	Description: "insert a row into the 'light' table using prepared statement for the batch",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableLight,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, insertByPreparedDataWorker, 0)
	},
}

// TestInsertLightMultiValue inserts a row into the 'light' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...)
var TestInsertLightMultiValue = engine.TestDesc{
	Name:        "insert-light-multivalue",
	Metric:      "rows/sec",
	Description: "insert a row into the 'light' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) ",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   engine.ALL,
	Table:       TestTableLight,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, engine.InsertMultiValueDataWorker, 0)
	},
}

// TestCopyLight copies a row into the 'light' table
var TestCopyLight = engine.TestDesc{
	Name:        "copy-light",
	Metric:      "rows/sec",
	Description: "copy a row into the 'light' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableLight,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, copyDataWorker, 0)
	},
}
