package nested_objects_search

import (
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestTableEmailNested is table to store email objects with nested fields
var TestTableEmailNested = engine.TestTable{
	TableName: "acronis_db_bench_email_nested",
	Databases: []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Columns: [][]interface{}{
		{"id", "dataset.id"},
		{"uuid", "uuid"},
		{"euc_id", "int", 2147483647},
		{"progress", "int", 100},
		{"sender", "dataset.From"},
		{"recipient", "dataset.To"},
		{"subject", "dataset.Subject"},
		{"body", "dataset.Body"},
		//{"history.history_type", "string", 3, 32, 4},
		//{"history.timestamp", "time_ns", 90},
		//{"history.event", "string", 10, 32, 8},
	},
	InsertColumns: []string{}, // all
	UpdateColumns: []string{"progress"},
	TableDefinition: func(_ db.DialectName) *db.TableDefinition {
		return &db.TableDefinition{
			TableRows: []db.TableRow{
				db.TableRowItem{Name: "id", Type: db.DataTypeBigIntAutoInc},
				db.TableRowItem{Name: "uuid", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				db.TableRowItem{Name: "euc_id", Type: db.DataTypeInt, NotNull: true, Indexed: true},
				db.TableRowItem{Name: "progress", Type: db.DataTypeInt},
				db.TableRowItem{Name: "sender", Type: db.DataTypeVarChar, Indexed: true},
				db.TableRowItem{Name: "recipient", Type: db.DataTypeVarChar, Indexed: true},
				db.TableRowItem{Name: "subject", Type: db.DataTypeVarChar, Indexed: true},
				db.TableRowItem{Name: "body", Type: db.DataTypeText, Indexed: true},
				db.TableRowSubtable{
					Name: "history",
					Type: db.DataTypeNested,
					Subtable: []db.TableRow{
						db.TableRowItem{Name: "history_type", Type: db.DataTypeVarChar, Indexed: true},
						db.TableRowItem{Name: "timestamp", Type: db.DataTypeDateTime, Indexed: true},
						db.TableRowItem{Name: "event", Type: db.DataTypeVarChar, Indexed: true},
					},
				},
			},
			PrimaryKey: []string{"id"},
		}
	},
	CreateQuery: `create table {table} (
			id {$bigint_autoinc_pk},
			uuid {$varchar_uuid} {$notnull},
			euc_id int {$notnull},
			progress int {$null}
			) {$engine};`,
	Indexes:             [][]string{{"tenant_id"}},
	DisableAutoCreation: true,
}

// TestTableEmailParentChild is table to store email objects with parent child
var TestTableEmailParentChild = engine.TestTable{
	TableName: "acronis_db_bench_email_pc",
	Databases: []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Columns: [][]interface{}{
		{"id", "dataset.id"},
		{"uuid", "uuid"},
		{"euc_id", "int", 2147483647},
		{"progress", "int", 100},
		{"sender", "dataset.From"},
		{"recipient", "dataset.To"},
		{"subject", "dataset.Subject"},
		{"body", "dataset.Body"},
		//{"history_type", "string", 3, 32, 4},
		//{"history_timestamp", "time_ns", 90},
		//{"history_event", "string", 10, 32, 8},
	},
	InsertColumns: []string{}, // all
	UpdateColumns: []string{"progress"},
	TableDefinition: func(_ db.DialectName) *db.TableDefinition {
		return &db.TableDefinition{
			TableRows: []db.TableRow{
				db.TableRowItem{Name: "id", Type: db.DataTypeBigIntAutoInc},
				db.TableRowItem{Name: "uuid", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				db.TableRowItem{Name: "euc_id", Type: db.DataTypeInt, NotNull: true, Indexed: true},
				db.TableRowItem{Name: "progress", Type: db.DataTypeInt},
				db.TableRowItem{Name: "sender", Type: db.DataTypeVarChar, Indexed: true},
				db.TableRowItem{Name: "recipient", Type: db.DataTypeVarChar, Indexed: true},
				db.TableRowItem{Name: "subject", Type: db.DataTypeVarChar, Indexed: true},
				db.TableRowItem{Name: "body", Type: db.DataTypeText, Indexed: true},
				db.TableRowItem{Name: "doc_type", Type: db.DataTypeVarChar, Indexed: true},
				db.TableRowItem{Name: "history_type", Type: db.DataTypeVarChar, Indexed: true},
				db.TableRowItem{Name: "history_timestamp", Type: db.DataTypeDateTime, Indexed: true},
				db.TableRowItem{Name: "history_event", Type: db.DataTypeVarChar, Indexed: true},
			},
			PrimaryKey: []string{"id"},
		}
	},
	CreateQuery: `create table {table} (
			id {$bigint_autoinc_pk},
			uuid {$varchar_uuid} {$notnull},
			euc_id int {$notnull},
			progress int {$null}
			) {$engine};`,
	Indexes:             [][]string{{"tenant_id"}},
	DisableAutoCreation: true,
}
