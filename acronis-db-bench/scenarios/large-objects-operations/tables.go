package large_objects_operations

import "github.com/acronis/perfkit/acronis-db-bench/engine"

// TestTableLargeObj is table to store large objects
var TestTableLargeObj = engine.TestTable{
	TableName: "acronis_db_bench_largeobj",
	Databases: engine.RELATIONAL,
	Columns: [][]interface{}{
		{"uuid", "uuid"},
		{"tenant_id", "tenant_uuid"},
		{"timestamp", "time_ns"},
		{"oid", "int"},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `create table {table} (
		id {$bigint_autoinc_pk},
		uuid char(36) not null,
		tenant_id varchar(64) not null,
		timestamp bigint not null,
		oid int not null
		) {$engine};`,
	Indexes: [][]string{{"tenant_id"}, {"uuid"}},
}
