package json_search

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestTableJSON is table to store JSON data
var TestTableJSON = engine.TestTable{
	TableName: "acronis_db_bench_json",
	Databases: []db.DialectName{db.MYSQL, db.POSTGRES},
	Columns: [][]interface{}{
		{"previous", "bigint", 0},
		{"sequence", "bigint", 0},
		{"time", "time", 0},
		{"json_data", "json", 0},
		{"source", "string", 0, 32},
		{"specversion", "string", 3, 32},
		{"tag", "string", 256, 32},
		{"type", "cti_uuid", 64},
		{"subject", "uuid", 0},
		{"tenantid", "uuid", 0},
		{"clientid", "uuid", 0},
		{"topic", "cti_uuid", 0},
		{"event_id", "uuid", 0},
		{"date", "timestamp", 0},
		{"created_at", "timestamp", 0},
		{"subjecttype", "cti_uuid", 64},
	},
	InsertColumns: []string{}, // all
	UpdateColumns: []string{"time", "json_data", "date"},
	CreateQuery: `create table acronis_db_bench_json(
			id {$bigint_autoinc_pk},
			previous bigint,
			sequence bigint,
			time text,
			json_data {$json_type},
			database64 text,
			source text not null,
			specversion text not null,
			type text not null,
			subject text,
			tenantid {$uuid} not null,
			clientid {$uuid},
			originid {$uuid},
			dataref text,
			traceparent {$uuid},
			topic text not null,
			tag text not null,
			event_id {$uuid} not null,
			date timestamp not null,
			created_at timestamp not null,
			ingesttime timestamp,
			subjecttype text
			) {$engine};
			{$json_index}`,
	CreateQueryPatchFuncs: []engine.CreateQueryPatchFunc{JSONTableCreateQueryPatchFunc},
	Indexes:               [][]string{{"sequence"}, {"created_at"}},
}

func JSONTableCreateQueryPatchFunc(table string, query string, dialect db.DialectName) (string, error) { //nolint:revive
	switch dialect {
	case db.MYSQL:
		query = strings.ReplaceAll(query, "{$json_type}", "json")
		query = strings.ReplaceAll(query, "{$json_index}",
			"ALTER TABLE acronis_db_bench_json ADD COLUMN _data_f0f0 VARCHAR(1024) AS (JSON_EXTRACT(json_data, '$.field0.field0')) STORED;"+
				"ALTER TABLE acronis_db_bench_json ADD COLUMN _data_f0f0f0 VARCHAR(1024) AS (JSON_EXTRACT(json_data, '$.field0.field0.field0')) STORED;"+
				"CREATE INDEX acronis_db_bench_json_idx_data_f0f0 ON acronis_db_bench_json(_data_f0f0);"+
				"CREATE INDEX acronis_db_bench_json_idx_data_f0f0f0 ON acronis_db_bench_json(_data_f0f0f0);")
	case db.POSTGRES:
		query = strings.ReplaceAll(query, "{$json_type}", "jsonb")
		query = strings.ReplaceAll(query, "{$json_index}",
			"CREATE INDEX acronis_db_bench_json_idx_data ON acronis_db_bench_json USING GIN (json_data jsonb_path_ops)")
	default:
		return "", fmt.Errorf("unsupported driver: '%v', supported drivers are: postgres|mysql", dialect)
	}

	return query, nil
}
