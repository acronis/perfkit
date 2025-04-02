package basic_scenarios

import (
	"context"
	"fmt"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

var tableHeavySchema = `
	id {$bigint_autoinc_pk},
	uuid                      {$uuid}        not null {$unique},
	checksum                  varchar(64) not null,
	tenant_id                 varchar(36) not null, -- conditional, high cardinality (about 100K), no empty values
	cti_entity_uuid           varchar(36),
	euc_id                    varchar(64) not null, -- conditional, high cardinality (about 100K)
	workflow_id               bigint,
	state                     integer     not null, -- conditional, orderable, small cardinality (around 5), no empty values
	type                      varchar(64) not null, -- conditional, orderable, small cardinality (around 100), no empty values
	queue                     varchar(64) not null, -- conditional, small cardinality (around 100), no empty values
	priority                  integer     not null, -- conditional, orderable, small cardinality (around 5), no empty values
	issuer_id                 varchar(64) not null,
	issuer_cluster_id         varchar(64),
	heartbeat_ivl_str         varchar(64),
	heartbeat_ivl_ns          bigint,
	queue_timeout_str         varchar(64),
	queue_timeout_ns          bigint,
	ack_timeout_str           varchar(64),
	ack_timeout_ns            bigint,
	exec_timeout_str          varchar(64),
	exec_timeout_ns           bigint,
	life_time_str             varchar(64),
	life_time_ns              bigint,
	max_assign_count          integer     not null,
	assign_count              integer     not null,
	max_fail_count            integer     not null,
	fail_count                integer     not null,
	cancellable               {$boolean}     not null,
	cancel_requested          {$boolean}     not null,
	blocker_count             integer     not null,
	started_by_user           varchar(256),         -- conditional, orderable, high cardinality (about 100K)
	policy_id                 varchar(64),          -- conditional, high cardinality (about 100K)
	policy_type               varchar(64),          -- conditional, high cardinality (about 100K)
	policy_name               varchar(256),         -- conditional, high cardinality (about 100K)
	resource_id               varchar(64),          -- conditional, high cardinality (about 100K)
	resource_type             varchar(64),          -- conditional, high cardinality (about 100K)
	resource_name             varchar(256),         -- conditional, orderable, high cardinality (about 100K)
	tags                      text,
	affinity_agent_id         varchar(64) not null,
	affinity_cluster_id       varchar(64) not null,
	argument                  {$binaryblobtype},
	context                   {$binaryblobtype},
	progress                  integer,
	progress_total            integer,
	assigned_agent_id         varchar(64),
	assigned_agent_cluster_id varchar(64),
	enqueue_time_str          varchar(64),
	enqueue_time_ns           bigint      not null, -- conditional, orderable, unique
	assign_time_str           varchar(64),
	assign_time_ns            bigint,               -- conditional, orderable, unique
	start_time_str            varchar(64),
	start_time_ns             bigint,               -- conditional, orderable, unique
	update_time_str           varchar(64) not null,
	update_time_ns            bigint      not null, -- conditional, orderable, unique
	completion_time_str       varchar(64),
	completion_time_ns        bigint,               -- conditional, orderable, unique
	result_code               integer,              -- conditional, orderable, small cardinality (around 5)
	result_error              {$binaryblobtype},
	result_warnings           {$binaryblobtype},
	result_payload            {$binaryblobtype},
	const_val                 integer               -- special data to test update of the same data
`

// TestTableHeavy is table to store heavy objects
var TestTableHeavy = engine.TestTable{
	TableName: "acronis_db_bench_heavy",
	Databases: engine.ALL,
	Columns: [][]interface{}{
		{"id", "autoinc"},
		{"uuid", "uuid", 0},
		{"checksum", "string", 0, 64},
		{"cti_entity_uuid", "cti_uuid", 0},

		{"tenant_id", "tenant_uuid", 0},
		{"euc_id", "string", 0, 64},

		// {"tenant_vis_list", "tenant_uuid_parents", 0},

		{"workflow_id", "int", 2147483647},
		{"state", "int", 16},
		{"type", "string", 256, 64},
		{"queue", "string", 256, 64},
		{"priority", "int", 5},

		{"issuer_id", "uuid", 0},

		{"max_assign_count", "int", 100},
		{"assign_count", "int", 5},
		{"cancellable", "bool", 0},
		{"cancel_requested", "bool", 0},
		{"blocker_count", "int", 3},

		{"started_by_user", "string", 0, 32},

		{"policy_id", "int", 1024},
		{"policy_type", "string", 1024, 64},
		{"policy_name", "string", 16384, 256},

		{"resource_id", "uuid", 0},
		{"resource_type", "int", 256},
		{"resource_name", "string", 0, 256},

		{"affinity_agent_id", "uuid", 0},
		{"affinity_cluster_id", "string", 0, 32},

		{"progress", "int", 100},
		{"progress_total", "int", 100},

		{"enqueue_time", "time_ns", 0},
		{"assign_time", "time_ns", 0},
		{"start_time", "time_ns", 0},
		{"update_time", "time_ns", 0},
		{"completion_time", "time_ns", 0},

		{"result_code", "int", 32},
		{"result_payload", "rbyte", 0, 256},

		{"const_val", "int", 1},
	},
	InsertColumns: []string{}, // all
	UpdateColumns: []string{"progress", "result_payload", "update_time", "completion_time"},
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		var tableRows []db.TableRow

		tableRows = append(tableRows,
			db.TableRow{Name: "id", Type: db.DataTypeBigIntAutoIncPK, Indexed: true},
			db.TableRow{Name: "uuid", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
			db.TableRow{Name: "checksum", Type: db.DataTypeVarChar64, NotNull: true},
			db.TableRow{Name: "cti_entity_uuid", Type: db.DataTypeVarChar36, Indexed: true},
		)

		if dialect == db.CLICKHOUSE {
			// Needed for primary key
			tableRows = append(tableRows,
				db.TableRow{Name: "partner_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				db.TableRow{Name: "customer_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				db.TableRow{Name: "tenant_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
			)
		} else if dialect == db.ELASTICSEARCH {
			tableRows = append(tableRows,
				db.TableRow{Name: "tenant_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				db.TableRow{Name: "tenant_vis_list", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
			)
		} else {
			tableRows = append(tableRows,
				db.TableRow{Name: "tenant_id", Type: db.DataTypeVarChar36, NotNull: true, Indexed: true},
				db.TableRow{Name: "euc_id", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			)
		}

		tableRows = append(tableRows,
			db.TableRow{Name: "workflow_id", Type: db.DataTypeBigInt, NotNull: true, Indexed: true},
			db.TableRow{Name: "state", Type: db.DataTypeInt, NotNull: true, Indexed: true},
			db.TableRow{Name: "type", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			db.TableRow{Name: "queue", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			db.TableRow{Name: "priority", Type: db.DataTypeInt, NotNull: true, Indexed: true},

			db.TableRow{Name: "issuer_id", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			db.TableRow{Name: "issuer_cluster_id", Type: db.DataTypeVarChar64, Indexed: true},

			db.TableRow{Name: "heartbeat_ivl", Type: db.DataTypeBigInt},
			db.TableRow{Name: "queue_timeout", Type: db.DataTypeBigInt},
			db.TableRow{Name: "ack_timeout", Type: db.DataTypeBigInt},
			db.TableRow{Name: "exec_timeout", Type: db.DataTypeBigInt},
			db.TableRow{Name: "life_time", Type: db.DataTypeBigInt},

			db.TableRow{Name: "max_assign_count", Type: db.DataTypeInt, NotNull: true},
			db.TableRow{Name: "assign_count", Type: db.DataTypeInt, NotNull: true},
			db.TableRow{Name: "cancellable", Type: db.DataTypeBoolean, NotNull: true},
			db.TableRow{Name: "cancel_requested", Type: db.DataTypeBoolean, NotNull: true},
			db.TableRow{Name: "blocker_count", Type: db.DataTypeInt, NotNull: true},

			db.TableRow{Name: "started_by_user", Type: db.DataTypeVarChar256, Indexed: true},
		)

		if dialect == db.CASSANDRA {
			tableRows = append(tableRows, db.TableRow{Name: "policy_id", Type: db.DataTypeInt, Indexed: true})
		} else {
			tableRows = append(tableRows, db.TableRow{Name: "policy_id", Type: db.DataTypeVarChar64, Indexed: true})
		}

		tableRows = append(tableRows,
			db.TableRow{Name: "policy_type", Type: db.DataTypeVarChar64, Indexed: true},
			db.TableRow{Name: "policy_name", Type: db.DataTypeVarChar256, Indexed: true},

			db.TableRow{Name: "resource_id", Type: db.DataTypeVarChar64, Indexed: true},
		)

		if dialect == db.CASSANDRA {
			tableRows = append(tableRows, db.TableRow{Name: "resource_type", Type: db.DataTypeInt, Indexed: true})
		} else {
			tableRows = append(tableRows, db.TableRow{Name: "resource_type", Type: db.DataTypeVarChar64, Indexed: true})
		}

		tableRows = append(tableRows,
			db.TableRow{Name: "resource_name", Type: db.DataTypeVarChar256, Indexed: true},

			db.TableRow{Name: "tags", Type: db.DataTypeText, Indexed: true},

			db.TableRow{Name: "affinity_agent_id", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			db.TableRow{Name: "affinity_cluster_id", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},

			db.TableRow{Name: "argument", Type: db.DataTypeBinaryBlobType},
			db.TableRow{Name: "context", Type: db.DataTypeBinaryBlobType},

			db.TableRow{Name: "progress", Type: db.DataTypeInt},
			db.TableRow{Name: "progress_total", Type: db.DataTypeInt},

			db.TableRow{Name: "assigned_agent_id", Type: db.DataTypeVarChar64, Indexed: true},
			db.TableRow{Name: "assigned_agent_cluster_id", Type: db.DataTypeVarChar64, Indexed: true},

			db.TableRow{Name: "enqueue_time", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRow{Name: "assign_time", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRow{Name: "start_time", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRow{Name: "update_time", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRow{Name: "completion_time", Type: db.DataTypeBigInt, Indexed: true},

			db.TableRow{Name: "result_code", Type: db.DataTypeInt, Indexed: true},
			db.TableRow{Name: "result_error", Type: db.DataTypeBinaryBlobType},
			db.TableRow{Name: "result_warnings", Type: db.DataTypeBinaryBlobType},
			db.TableRow{Name: "result_payload", Type: db.DataTypeBinaryBlobType},

			db.TableRow{Name: "const_val", Type: db.DataTypeInt},
		)

		var tableDef = &db.TableDefinition{
			TableRows: tableRows,
		}
		if dialect == db.CLICKHOUSE {
			tableDef.PrimaryKey = []string{"partner_id", "customer_id", "toDate(update_time)"}
		}

		if dialect == db.ELASTICSEARCH {
			tableDef.Resilience.NumberOfReplicas = 2
		}

		return tableDef
	},
	CreateQuery: `create table {table} (` + tableHeavySchema + `) {$engine};`,
	Indexes: [][]string{
		{"uuid"},
		{"completion_time"},
		{"cti_entity_uuid"},
		{"tenant_id"},
		{"euc_id"},
		{"queue", "state", "affinity_agent_id", "affinity_cluster_id", "tenant_id", "priority"},
		{"queue", "state", "affinity_agent_id", "affinity_cluster_id", "euc_id", "priority"},
		{"update_time"},
		{"state", "completion_time"},
		{"start_time"},
		{"enqueue_time"},
		{"resource_id"},
		{"policy_id"},
		{"result_code"},
		{"resource_id", "enqueue_time"},
		{"type"},
		{"type", "tenant_id", "enqueue_time"},
		{"type", "euc_id", "enqueue_time"},
		{"queue", "type", "tenant_id"},
		{"queue", "type", "euc_id"},
	},
}

// TestInsertHeavy inserts a row into the 'heavy' table
var TestInsertHeavy = engine.TestDesc{
	Name:        "insert-heavy",
	Metric:      "rows/sec",
	Description: "insert a row into the 'heavy' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertHeavyPrepared inserts a row into the 'heavy' table using prepared statement for the batch
var TestInsertHeavyPrepared = engine.TestDesc{
	Name:        "insert-heavy-prepared",
	Metric:      "rows/sec",
	Description: "insert a row into the 'heavy' table using prepared statement for the batch",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, insertByPreparedDataWorker, 0)
	},
}

// TestInsertHeavyMultivalue inserts a row into the 'heavy' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) "
var TestInsertHeavyMultivalue = engine.TestDesc{
	Name:        "insert-heavy-multivalue",
	Metric:      "rows/sec",
	Description: "insert a row into the 'heavy' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) ",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, engine.InsertMultiValueDataWorker, 0)
	},
}

// TestCopyHeavy copies a row into the 'heavy' table
var TestCopyHeavy = engine.TestDesc{
	Name:        "copy-heavy",
	Metric:      "rows/sec",
	Description: "copy a row into the 'heavy' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, copyDataWorker, 0)
	},
}

// TestInsertHeavyDBR inserts a row into the 'heavy' table using golang DB query builder
var TestInsertHeavyDBR = engine.TestDesc{
	Name:        "dbr-insert-heavy",
	Metric:      "rows/sec",
	Description: "insert a row into the 'heavy' table using golang DB query builder",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   true,
	Databases:   []db.DialectName{db.POSTGRES, db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestUpdateHeavy updates random row in the 'heavy' table
var TestUpdateHeavy = engine.TestDesc{
	Name:        "update-heavy",
	Metric:      "rows/sec",
	Description: "update random row in the 'heavy' table",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestUpdateGeneric(b, testDesc, 1, nil)
	},
}

// TestUpdateHeavyDBR updates random row in the 'heavy' table using golang DBR query builder
var TestUpdateHeavyDBR = engine.TestDesc{
	Name:        "dbr-update-heavy",
	Metric:      "rows/sec",
	Description: "update random row in the 'heavy' table using golang DB driver",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	IsDBRTest:   true,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestUpdateGeneric(b, testDesc, 1, nil)
	},
}

// TestUpdateHeavyBulk updates N rows (see --batch=, default 50000) in the 'heavy' table by single transaction
var TestUpdateHeavyBulk = engine.TestDesc{
	Name:        "bulkupdate-heavy",
	Metric:      "rows/sec",
	Description: "update N rows (see --batch=, default 50000) in the 'heavy' table by single transaction",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		origBatch := b.Vault.(*engine.DBTestData).EffectiveBatch
		testBatch := origBatch
		if b.TestOpts.(*engine.TestOpts).BenchOpts.Batch == 0 {
			testBatch = 50000
		}
		b.Vault.(*engine.DBTestData).EffectiveBatch = 1

		engine.TestUpdateGeneric(b, testDesc, uint64(testBatch), nil)

		b.Vault.(*engine.DBTestData).EffectiveBatch = origBatch
	},
}

// TestUpdateHeavyBulkDBR updates N rows (see --batch=, default 50000) in the 'heavy' table by single transaction using DBR query builder
var TestUpdateHeavyBulkDBR = engine.TestDesc{
	Name:        "dbr-bulkupdate-heavy",
	Metric:      "rows/sec",
	Description: "update N rows (see --update-rows-count= ) in the 'heavy' table by single transaction using DBR query builder",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	IsDBRTest:   true,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		origBatch := b.Vault.(*engine.DBTestData).EffectiveBatch
		b.Vault.(*engine.DBTestData).EffectiveBatch = 1
		testBatch := origBatch
		if b.TestOpts.(*engine.TestOpts).BenchOpts.Batch == 0 {
			testBatch = 50000
		}

		engine.TestUpdateGeneric(b, testDesc, uint64(testBatch), nil)

		b.Vault.(*engine.DBTestData).EffectiveBatch = origBatch
	},
}

// TestUpdateHeavySameVal updates random row in the 'heavy' table putting the value which already exists
var TestUpdateHeavySameVal = engine.TestDesc{
	Name:        "update-heavy-sameval",
	Metric:      "rows/sec",
	Description: "update random row in the 'heavy' table putting the value which already exists",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		colConf := testDesc.Table.GetColumnsConf([]string{"const_val"}, false)
		engine.TestUpdateGeneric(b, testDesc, 1, colConf)
	},
}

// TestUpdateHeavyPartialSameVal updates random row in the 'heavy' table putting two values, where one of them is already exists in this row
var TestUpdateHeavyPartialSameVal = engine.TestDesc{
	Name:        "update-heavy-partial-sameval",
	Metric:      "rows/sec",
	Description: "update random row in the 'heavy' table putting two values, where one of them is already exists in this row",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		colConf := testDesc.Table.GetColumnsConf([]string{"const_val", "progress"}, false)
		engine.TestUpdateGeneric(b, testDesc, 1, colConf)
	},
}

// TestSelectHeavyLast selects last row from the 'heavy' table
var TestSelectHeavyLast = engine.TestDesc{
	Name:        "select-heavy-last",
	Metric:      "rows/sec",
	Description: "select last row from the 'heavy' table",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var idToRead int64
		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { return []string{"desc(id)"} } //nolint:revive
		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, nil, orderBy, 1)
	},
}

// TestSelectHeavyLastDBR selects last row from the 'heavy' table using golang DBR driver
var TestSelectHeavyLastDBR = engine.TestDesc{
	Name:        "dbr-select-heavy-last",
	Metric:      "rows/sec",
	Description: "select last row from the 'heavy' table using golang DBR driver",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   true,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var idToRead int64
		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { return []string{"desc(id)"} } //nolint:revive
		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, nil, orderBy, 1)
	},
}

// TestSelectHeavyRand selects random row from the 'heavy' table
var TestSelectHeavyRand = engine.TestDesc{
	Name:        "select-heavy-rand",
	Metric:      "rows/sec",
	Description: "select random row from the 'heavy' table",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			id := worker.Randomizer.Uintn64(testDesc.Table.RowsCount - 1)

			return map[string][]string{"id": {fmt.Sprintf("gt(%d)", id)}}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"asc(id)"}
		}
		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}

// TestSelectHeavyRandDBR selects random row from the 'heavy' table using golang DBR query builder
var TestSelectHeavyRandDBR = engine.TestDesc{
	Name:        "dbr-select-heavy-rand",
	Metric:      "rows/sec",
	Description: "select random row from the 'heavy' table using golang DBR query builder",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   true,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			id := worker.Randomizer.Uintn64(testDesc.Table.RowsCount - 1)

			return map[string][]string{"id": {fmt.Sprintf("gt(%d)", id)}}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"asc(id)"}
		}
		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}

// TestSelectHeavyRandTenantLike selects random row from the 'heavy' table WHERE tenant_id = {} AND resource_name LIKE {}
var TestSelectHeavyRandTenantLike = engine.TestDesc{
	Name:        "select-heavy-rand-in-tenant-like",
	Metric:      "rows/sec",
	Description: "select random row from the 'heavy' table WHERE tenant_id = {} AND resource_name LIKE {}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var colConfs = testDesc.Table.GetColumnsConf([]string{"tenant_id"}, false)

		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			w, err := worker.Randomizer.GenFakeDataAsMap(colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			var tenant = fmt.Sprintf("%s", (*w)["tenant_id"])
			if tenant == "" {
				worker.Logger.Warn("tenant_id is empty")
			}

			return map[string][]string{
				"tenant_id":     {fmt.Sprintf("%s", (*w)["tenant_id"])},
				"resource_name": {"like(a)"},
			}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"desc(id)"}
		}
		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}

// TestSelectHeavyMinMaxTenant selects min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {}
var TestSelectHeavyMinMaxTenant = engine.TestDesc{
	Name:        "select-heavy-minmax-in-tenant",
	Metric:      "rows/sec",
	Description: "select min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var colConfs = testDesc.Table.GetColumnsConf([]string{"tenant_id"}, false)

		var minCompletionTime int64
		var maxCompletionTime int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			w, err := worker.Randomizer.GenFakeDataAsMap(colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			return map[string][]string{"tenant_id": {fmt.Sprintf("%s", (*w)["tenant_id"])}}
		}
		engine.TestSelectRun(b, testDesc, nil, []string{"min(completion_time)", "max(completion_time)"}, []interface{}{&minCompletionTime, &maxCompletionTime}, where, nil, 1)
	},
}

// TestSelectHeavyMinMaxTenantAndState selects min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {} AND state = {}
var TestSelectHeavyMinMaxTenantAndState = engine.TestDesc{
	Name:        "select-heavy-minmax-in-tenant-and-state",
	Metric:      "rows/sec",
	Description: "select min(completion_time) and max(completion_time) value from the 'heavy' table WHERE tenant_id = {} AND state = {}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {

		var colConfs = testDesc.Table.GetColumnsConf([]string{"tenant_id", "state"}, false)

		var minCompletionTime int64
		var maxCompletionTime int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			w, err := worker.Randomizer.GenFakeDataAsMap(colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			return map[string][]string{
				"tenant_id": {fmt.Sprintf("%s", (*w)["tenant_id"])},
				"state":     {fmt.Sprintf("%d", (*w)["state"])},
			}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"min(completion_time)", "max(completion_time)"}, []interface{}{&minCompletionTime, &maxCompletionTime}, where, nil, 1)
	},
}

// TestSelectHeavyForUpdateSkipLocked selects a row from the 'heavy' table and then updates it
var TestSelectHeavyForUpdateSkipLocked = engine.TestDesc{
	Name:        "select-heavy-for-update-skip-locked",
	Metric:      "updates/sec",
	Description: "do SELECT FOR UPDATE SKIP LOCKED and then UPDATE",
	Category:    engine.TestOther,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var query string
		max := b.CommonOpts.Workers*2 + 1

		var dialectName, err = db.GetDialectName(b.TestOpts.(*engine.TestOpts).DBOpts.ConnString)
		if err != nil {
			b.Exit(err)
		}

		switch dialectName {
		case db.POSTGRES, db.MYSQL:
			query = fmt.Sprintf("SELECT id, progress FROM acronis_db_bench_heavy WHERE id < %d LIMIT 1 FOR UPDATE SKIP LOCKED", max)
		case db.MSSQL:
			query = fmt.Sprintf("SELECT TOP(1) id, progress FROM acronis_db_bench_heavy WITH (UPDLOCK, READPAST, ROWLOCK) WHERE id < %d", max)
		default:
			b.Exit("unsupported driver: '%v', supported drivers are: %s|%s|%s", dialectName, db.POSTGRES, db.MYSQL, db.MSSQL)
		}

		worker := func(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
			var explain = b.TestOpts.(*engine.TestOpts).DBOpts.Explain
			var session = c.Database.Session(c.Database.Context(context.Background(), explain))
			if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
				var id int64
				var progress int

				if err := session.QueryRow(query).Scan(&id, &progress); err != nil {
					return err
				}

				if _, err := session.Exec(fmt.Sprintf("UPDATE acronis_db_bench_heavy SET progress = %d WHERE id = %d", progress+1, id)); err != nil {
					return err
				}

				return nil
			}); txErr != nil {
				b.Exit(txErr.Error())
			}

			return 1
		}

		engine.TestGeneric(b, testDesc, worker, 10000)
	},
}

// TestSelectHeavyLastTenant is the same as TestSelectHeavyLast but with tenant-awareness
var TestSelectHeavyLastTenant = engine.TestDesc{
	Name:        "select-heavy-last-in-tenant",
	Metric:      "rows/sec",
	Description: "select the last row from the 'heavy' table WHERE tenant_id = {random tenant uuid}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		worker := func(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
			return tenantAwareWorker(b, c, testDesc, "ORDER BY enqueue_time DESC", 1)
		}
		engine.TestGeneric(b, testDesc, worker, 1)
	},
}

// TestSelectHeavyLastTenantCTI is the same as TestSelectHeavyLastTenant but with CTI-awareness
var TestSelectHeavyLastTenantCTI = engine.TestDesc{
	Name:        "select-heavy-last-in-tenant-and-cti",
	Metric:      "rows/sec",
	Description: "select the last row from the 'heavy' table WHERE tenant_id = {} AND cti = {}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		worker := func(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
			return tenantAwareCTIAwareWorker(b, c, testDesc, "ORDER BY enqueue_time DESC", 1)
		}
		engine.TestGeneric(b, testDesc, worker, 1)
	},
}
