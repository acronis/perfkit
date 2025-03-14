package time_series_operations

import (
	"strings"

	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestTableTimeSeriesSQL is table to store time series data
var TestTableTimeSeriesSQL = engine.TestTable{
	TableName: "acronis_db_bench_ts_sql",
	Databases: engine.ALL,
	Columns: [][]interface{}{
		{"id", "autoinc", 0},
		{"tenant_id", "tenant_uuid", 0},
		{"device_id", "tenant_uuid_bound_id", 50}, // up to 50 devices per tenant
		{"metric_id", "cti_uuid", 10},             // up to 10 metrics to be used per every device
		{"ts", "now", 0},
		{"value", "int", 100},
	},
	InsertColumns: []string{}, // all
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		return &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "id", Type: db.DataTypeBigIntAutoIncPK},
				{Name: "tenant_id", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "device_id", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "metric_id", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "ts", Type: db.DataTypeTimestamp, NotNull: true, Indexed: true},
				{Name: "value", Type: db.DataTypeInt, NotNull: true},
			},
		}
	},
	CreateQuery: `create table acronis_db_bench_ts_sql(
			id {$bigint_autoinc_pk},
			tenant_id {$varchar_uuid} {$notnull},
			device_id {$tenant_uuid_bound_id} {$notnull},
			metric_id {$varchar_uuid} {$notnull},
			ts timestamp {$notnull},
			value int {$notnull}
		) {$engine};`,
	CreateQueryPatchFuncs: []engine.CreateQueryPatchFunc{
		func(table string, query string, dialect db.DialectName) (string, error) { //nolint:revive
			if dialect == db.CASSANDRA {
				query = strings.ReplaceAll(query, string(db.DataTypeBigIntAutoIncPK), string(db.DataTypeBigIntAutoInc))
				query = strings.ReplaceAll(query, "value int {$notnull}", `value int,
						PRIMARY KEY ((tenant_id, device_id, metric_id), id, ts)
					`)
			}

			return query, nil
		},
	},
	Indexes: [][]string{{"tenant_id"}, {"device_id"}, {"metric_id"}},
}
