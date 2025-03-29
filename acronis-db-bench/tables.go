package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
)

// CreateQueryPatchFunc is a function to patch create query for specific DB
type CreateQueryPatchFunc func(table string, query string, dialect db.DialectName) (string, error)

// TestTable represents table to be used in tests and benchmarks
type TestTable struct {
	TableName             string
	Databases             []db.DialectName
	ColumnsConf           []benchmark.DBFakeColumnConf
	columns               [][]interface{}
	InsertColumns         []string
	UpdateColumns         []string
	TableDefinition       func(dialect db.DialectName) *db.TableDefinition
	CreateQuery           string
	CreateQueryPatchFuncs []CreateQueryPatchFunc
	Indexes               [][]string

	// runtime information
	RowsCount uint64
}

// dbIsSupported returns true if the database is supported by the test
func (t *TestTable) dbIsSupported(db db.DialectName) bool {
	for _, b := range t.Databases {
		if b == db {
			return true
		}
	}

	return false
}

func exit(fmts string, args ...interface{}) {
	fmt.Printf(fmts, args...)
	fmt.Println()
	os.Exit(127)
}

func castInterface2ColumnsConf(columns [][]interface{}) []benchmark.DBFakeColumnConf {
	// columns - is an array of fields:
	// {
	//   "column name", # mandatory, represents real DB colum name
	//   "column type", # mandatory, represents data type supported by benchmark/faker.go
	//   "cardinality", # optional, represents data cardinality (e.g. number of combinations)
	//   "max size",    # optional, represents max data field value length (e.g. max string length)
	//   "min size",    # optional, represents min data field value length (e.g. min string length)
	// }
	ret := make([]benchmark.DBFakeColumnConf, 0, len(columns))
	for _, c := range columns {
		var cc benchmark.DBFakeColumnConf
		var ok bool

		cc.ColumnName, ok = c[0].(string)
		if !ok {
			exit("can't cast value %v to ColumnName", c[0])
		}

		cc.ColumnType, ok = c[1].(string)
		if !ok {
			exit("can't cast value %v to ColumnType", c[1])
		}

		l := len(c)
		if l > 2 {
			cc.Cardinality, ok = c[2].(int)
			if !ok {
				exit("can't cast value %v to Cardinality", c[2])
			}
		}
		if l > 3 {
			cc.MaxSize, ok = c[3].(int)
			if !ok {
				exit("can't cast value %v to MaxSize", c[3])
			}
		}
		if l > 4 {
			cc.MinSize, ok = c[4].(int)
			if !ok {
				exit("can't cast value %v to MinSize", c[4])
			}
		}
		ret = append(ret, cc)
	}

	return ret
}

// InitColumnsConf initializes ColumnsConf field based on provided columns
func (t *TestTable) InitColumnsConf() {
	if t.ColumnsConf == nil {
		t.ColumnsConf = castInterface2ColumnsConf(t.columns)
	}
}

// GetColumnsConf returns columns for insert or update operations based on provided list of columns
func (t *TestTable) GetColumnsConf(columns []string, withAutoInc bool) *[]benchmark.DBFakeColumnConf {
	t.InitColumnsConf()

	var colConfs []benchmark.DBFakeColumnConf

	// empty list means any column is required
	if len(columns) == 0 {
		if withAutoInc {
			return &t.ColumnsConf
		}

		for _, c := range t.ColumnsConf {
			// skip autoinc
			if c.ColumnType == "autoinc" {
				continue
			}
			colConfs = append(colConfs, c)
		}
	}

	for _, c := range t.ColumnsConf {
		if c.ColumnType == "autoinc" && !withAutoInc {
			continue
		}
		for _, v := range columns {
			if v == c.ColumnName {
				colConfs = append(colConfs, c)
			}
		}
	}

	return &colConfs
}

// GetColumnsForInsert returns columns for insert
func (t *TestTable) GetColumnsForInsert(withAutoInc bool) *[]benchmark.DBFakeColumnConf {
	return t.GetColumnsConf(t.InsertColumns, withAutoInc)
}

// GetColumnsForUpdate returns columns for update
func (t *TestTable) GetColumnsForUpdate(withAutoInc bool) *[]benchmark.DBFakeColumnConf {
	return t.GetColumnsConf(t.UpdateColumns, withAutoInc)
}

// Create creates table in DB using provided DBConnector
func (t *TestTable) Create(c *DBConnector, b *benchmark.Benchmark) {
	if t.TableName == "" {
		return
	}

	if t.TableDefinition != nil {
		var table = t.TableDefinition(c.database.DialectName())
		if err := c.database.CreateTable(t.TableName, table, ""); err != nil {
			b.Exit(err.Error())
		}
	} else {
		if t.CreateQuery == "" {
			b.Logger.Error("no create query for '%s'", t.TableName)
			// b.Exit("internal error: no migration provided for table %s creation", t.TableName)
			return
		}
		tableCreationQuery := t.CreateQuery

		var err error

		for _, patch := range t.CreateQueryPatchFuncs {
			tableCreationQuery, err = patch(t.TableName, tableCreationQuery, c.database.DialectName())
			if err != nil {
				b.Exit(err.Error())
			}
		}

		if err = c.database.CreateTable(t.TableName, nil, tableCreationQuery); err != nil {
			b.Exit(err.Error())
		}
	}

	for _, columns := range t.Indexes {
		c.database.CreateIndex(fmt.Sprintf("%s_%s_idx", t.TableName, strings.Join(columns, "_")), t.TableName, columns, db.IndexTypeBtree)
	}
}

/*
 * Table definitions
 */

// TestTableLight is table to store light objects
var TestTableLight = TestTable{
	TableName: "acronis_db_bench_light",
	Databases: ALL,
	columns: [][]interface{}{
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

// TestTableMedium is table to store medium objects
var TestTableMedium = TestTable{
	TableName: "acronis_db_bench_medium",
	Databases: ALL,
	columns: [][]interface{}{
		{"id", "autoinc"},
		{"uuid", "uuid"},
		{"tenant_id", "tenant_uuid"},
		{"euc_id", "int", 2147483647},
		{"progress", "int", 100},
	},
	InsertColumns: []string{}, // all
	UpdateColumns: []string{"progress"},
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		return &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "id", Type: db.DataTypeBigIntAutoIncPK},
				{Name: "uuid", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "tenant_id", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "euc_id", Type: db.DataTypeInt, NotNull: true, Indexed: true},
				{Name: "progress", Type: db.DataTypeInt},
			},
		}
	},
	CreateQuery: `create table {table} (
			id {$bigint_autoinc_pk},
			tenant_id {$varchar_uuid} {$notnull},
			uuid {$varchar_uuid} {$notnull},
			euc_id int {$notnull},
			progress int {$null}
			) {$engine};`,
	Indexes: [][]string{{"tenant_id"}},
}

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
var TestTableHeavy = TestTable{
	TableName: "acronis_db_bench_heavy",
	Databases: ALL,
	columns: [][]interface{}{
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

// TestTableVector768 is table to store 768-dimensions vector objects
var TestTableVector768 = TestTable{
	TableName: "acronis_db_bench_vector_768",
	Databases: VECTOR,
	columns: [][]interface{}{
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

// TestTableEmailSecurity is table to store email security objects
var TestTableEmailSecurity = TestTable{
	TableName: "acronis_db_bench_email_security",
	Databases: VECTOR,
	columns: [][]interface{}{
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

// TestTableBlob is table to store blobs
var TestTableBlob = TestTable{
	TableName: "acronis_db_bench_blob",
	Databases: ALL,
	columns: [][]interface{}{
		{"id", "autoinc"},
		{"uuid", "uuid"},
		{"tenant_id", "tenant_uuid"},
		{"timestamp", "time_ns"},
		{"data", "blob"},
	},
	InsertColumns: []string{}, // all
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		return &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "id", Type: db.DataTypeBigIntAutoIncPK},
				{Name: "uuid", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				{Name: "tenant_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				{Name: "timestamp", Type: db.DataTypeBigInt, NotNull: true, Indexed: true},
				{Name: "data", Type: db.DataTypeHugeBlob, NotNull: true},
			},
		}
	},
	CreateQuery: `create table {table} (
		id {$bigint_autoinc_pk},
		uuid {$varchar_uuid} {$notnull},
		tenant_id {$varchar_uuid} {$notnull},
		timestamp bigint {$notnull},
		data {$hugeblob} {$notnull}
		) {$engine};`,
	Indexes: [][]string{{"tenant_id"}, {"uuid"}},
}

// TestTableLargeObj is table to store large objects
var TestTableLargeObj = TestTable{
	TableName: "acronis_db_bench_largeobj",
	Databases: RELATIONAL,
	columns: [][]interface{}{
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

// TestTableJSON is table to store JSON data
var TestTableJSON = TestTable{
	TableName: "acronis_db_bench_json",
	Databases: []db.DialectName{db.MYSQL, db.POSTGRES},
	columns: [][]interface{}{
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
	CreateQueryPatchFuncs: []CreateQueryPatchFunc{JSONTableCreateQueryPatchFunc},
	Indexes:               [][]string{{"sequence"}, {"created_at"}},
}

// TestTableTimeSeriesSQL is table to store time series data
var TestTableTimeSeriesSQL = TestTable{
	TableName: "acronis_db_bench_ts_sql",
	Databases: ALL,
	columns: [][]interface{}{
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
	CreateQueryPatchFuncs: []CreateQueryPatchFunc{
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

// TestTableAdvmTasks is table to store tasks
var TestTableAdvmTasks = TestTable{
	TableName: "acronis_db_bench_advm_tasks",
	Databases: RELATIONAL,
	columns: [][]interface{}{
		{"origin", "int", 20},
		{"uuid", "uuid", 0},
		{"tenant_id", "uuid", 32},
		{"type", "uuid", 32},
		{"queue", "string", 16, 32},
		{"created_at", "time_ns", 90},
		{"started_at", "time_ns", 90},
		{"completed_at", "time_ns", 90},
		{"duration", "int", 20},
		{"issuer_id", "uuid", 32},
		{"assigned_agent_id", "uuid", 1024},
		{"started_by", "string", 128, 32},
		{"policy_id", "uuid", 1024},
		{"resource_id", "uuid", 100000},
		{"result_code_indexed", "int", 8},
		{"result_code", "string", 8, 32},
		{"result_error_domain", "string", 8, 32},
		{"result_error_code", "string", 8, 32},
		{"backup_bytes_saved", "int", 0},
		{"backup_bytes_processed", "int", 0},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `create table acronis_db_bench_advm_tasks(
			origin                 INT         NOT NULL, -- CHAR(36)
			id                     {$bigint_autoinc} NOT NULL,
			uuid                   CHAR(36)    NOT NULL UNIQUE,

			tenant_id              VARCHAR(64) NOT NULL,
			type                   VARCHAR(64) NOT NULL,
			queue                  VARCHAR(64) NOT NULL,

			created_at             BIGINT      NOT NULL,
			started_at             BIGINT,
			completed_at           BIGINT,
			duration               BIGINT,

			issuer_id              VARCHAR(64) NOT NULL,
			assigned_agent_id      VARCHAR(64),
			started_by             VARCHAR(256),
			policy_id              VARCHAR(64),
			resource_id            VARCHAR(64),
			result_code_indexed    INTEGER,
			result_code            VARCHAR(64),
			result_error_domain    VARCHAR(64),
			result_error_code      VARCHAR(64),
			backup_bytes_saved     INTEGER,
			backup_bytes_processed INTEGER,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"created_at"}, {"result_code_indexed"}},
}

// TestTableAdvmResources is table to store resources
var TestTableAdvmResources = TestTable{
	TableName: "acronis_db_bench_advm_resources",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	columns: [][]interface{}{
		{"origin", "int", 20},
		{"resource_uuid", "uuid", 100000},
		{"tenant_id", "string", 10, 32},
		{"customer_id", "string", 10, 32},

		{"type", "int", 4},
		{"name", "string", 100000, 32},

		{"created_at", "time_ns", 90},

		{"os", "string", 4, 32},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_resources(
			origin        INT               NOT NULL, -- CHAR(36)
			resource_id   {$bigint_autoinc} NOT NULL,
			resource_uuid CHAR(36)          NOT NULL,
			tenant_id     CHAR(36),
			customer_id   CHAR(36),

			type          INTEGER          NOT NULL,
			name          VARCHAR(256),

			created_at    BIGINT           NOT NULL,
			deleted_at    BIGINT,

			os            VARCHAR(256),

			PRIMARY KEY (origin, resource_uuid, tenant_id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"type"}, {"name"}},
}

// TestTableAdvmResourcesStatuses is table to store resources statuses
// inspired by the Event Archive table
var TestTableAdvmResourcesStatuses = TestTable{
	TableName: "acronis_db_bench_advm_resources_statuses",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	columns: [][]interface{}{
		{"origin", "int", 20},
		{"state", "int", 4},
		{"severity", "int", 4},
		{"applied_policy_names", "string", 100, 32},
		{"last_successful_backup", "time_ns", 90},
		{"next_backup", "time_ns", 90},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_resources_statuses(
			origin                 CHAR(36),
			resource_id            {$bigint_autoinc} NOT NULL,

			state                  INTEGER  DEFAULT 0,
			severity               SMALLINT DEFAULT 0,
			applied_policy_names   VARCHAR(256),
			last_successful_backup BIGINT,
			next_backup            BIGINT,

			PRIMARY KEY (origin, resource_id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"state"}},
}

// TestTableAdvmAgentsResources is table to store agents
// inspired by the Event Archive table
var TestTableAdvmAgentsResources = TestTable{
	TableName: "acronis_db_bench_advm_agent_resources",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	columns: [][]interface{}{
		{"origin", "int", 20},
		{"agent_uuid", "uuid", 100000},
		{"resource_id", "int", 100000},
		{"tenant_id", "string", 10, 32},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_agent_resources(
			origin      CHAR(36),
			agent_uuid  CHAR(36) NOT NULL,
			resource_id BIGINT   NOT NULL,
			tenant_id   CHAR(36) NOT NULL,

			PRIMARY KEY (origin, agent_uuid, resource_id, tenant_id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}},
}

// TestTableAdvmAgents is table to store agents
// inspired by the Event Archive table
var TestTableAdvmAgents = TestTable{
	TableName: "acronis_db_bench_advm_agents",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	columns: [][]interface{}{
		{"origin", "int", 20},
		{"uuid", "uuid", 100000},

		{"tenant_id", "string", 10, 32},
		{"type", "int", 4},
		{"name", "string", 100000, 32},

		{"created_at", "time_ns", 90},

		{"is_active", "bool", 0},
		{"os_family", "string", 8, 32},
		{"os_name", "string", 8, 32},
		{"version", "string", 8, 32},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_agents(
			origin     CHAR(36),
			uuid       CHAR(36) NOT NULL,

			tenant_id  CHAR(36),
			type       VARCHAR(64)      NOT NULL,
			name       VARCHAR(128),

			created_at BIGINT           NOT NULL,
			deleted_at BIGINT,

			is_active  BIT,
			os_family  VARCHAR(64),
			os_name    VARCHAR(255),
			version    VARCHAR(36),

			PRIMARY KEY (origin, uuid)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"type"}, {"name"}},
}

// TestTableAdvmBackupResources is table to store backups
// inspired by the Event Archive table
var TestTableAdvmBackupResources = TestTable{
	TableName: "acronis_db_bench_advm_backup_resources",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	columns: [][]interface{}{
		{"origin", "int", 20},
		{"backup_id", "int", 400000},
		{"resource_uuid", "uuid", 100000},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_backup_resources(
			origin        CHAR(36),
			backup_id     BIGINT   NOT NULL,
			resource_uuid CHAR(36) NOT NULL DEFAULT '',

			PRIMARY KEY (origin, backup_id, resource_uuid)
			) {$engine};`,
	Indexes: [][]string{{"origin"}},
}

// TestTableAdvmBackups is table to store backups
// inspired by the Event Archive table
var TestTableAdvmBackups = TestTable{
	TableName: "acronis_db_bench_advm_backups",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	columns: [][]interface{}{
		{"origin", "int", 20},

		{"created_at", "time_ns", 90},

		{"archive_id", "int", 100000},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_backups(
			origin     CHAR(36),
			id         {$bigint_autoinc} NOT NULL,

			created_at BIGINT  NOT NULL,
			deleted_at BIGINT,

			archive_id BIGINT  NOT NULL,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"created_at"}, {"archive_id"}},
}

// TestTableAdvmArchives is table to store archives
// inspired by the Event Archive table
var TestTableAdvmArchives = TestTable{
	TableName: "acronis_db_bench_advm_archives",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	columns: [][]interface{}{
		{"origin", "int", 20},

		{"created_at", "time_ns", 90},

		{"vault_id", "int", 100000},
		{"size", "int", 100000},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_archives(
			origin     CHAR(36),
			id         {$bigint_autoinc} NOT NULL,

			created_at BIGINT NOT NULL,
			deleted_at BIGINT,

			vault_id   BIGINT NOT NULL,
			size       BIGINT NOT NULL,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"created_at"}, {"vault_id"}},
}

// TestTableAdvmVaults is table to store vaults
// inspired by the Event Archive table
var TestTableAdvmVaults = TestTable{
	TableName: "acronis_db_bench_advm_vaults",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	columns: [][]interface{}{
		{"origin", "int", 20},

		{"name", "string", 100000, 32},
		{"storage_type", "string", 4, 32},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_vaults(
			origin       CHAR(36),
			id           {$bigint_autoinc} NOT NULL,

			name         VARCHAR(128) NOT NULL,
			storage_type VARCHAR(64)  NOT NULL,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"name"}},
}

// TestTableAdvmDevices is table to store devices
// inspired by the Event Archive table
var TestTableAdvmDevices = TestTable{
	TableName: "acronis_db_bench_advm_devices",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	columns: [][]interface{}{
		{"origin", "int", 20},
		{"uuid", "uuid", 0},

		{"name", "string", 100000, 32},
		{"type", "string", 4, 32},
		{"group_name", "string", 1000, 32},
		{"resource_os", "string", 4, 32},
		{"registered_at", "time_ns", 90},

		{"agent_name", "string", 100000, 32},
		{"agent_is_active", "bool", 0},
		{"agent_version", "string", 8, 32},

		{"customer_name", "string", 10, 32},
		{"unit_name", "string", 1000, 32},

		{"applied_policy", "string", 8, 32},
		{"state", "string", 4, 32},
		{"last_result", "string", 4, 32},
		{"last_backup", "time_ns", 90},
		{"next_backup", "time_ns", 90},

		{"archives_count", "int", 8},
		{"backups_count", "int", 16},
		{"oldest_backup", "time_ns", 90},
		{"latest_backup", "time_ns", 90},
		{"used_total", "int", 1000000},
		{"used_cloud", "int", 1000000},
		{"used_local", "int", 1000000},

		{"alerts_count", "int", 8},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `create table acronis_db_bench_advm_devices(
			origin                 INT         NOT NULL, -- CHAR(36)
			id                     {$bigint_autoinc} NOT NULL,
			uuid                   CHAR(36)    NOT NULL UNIQUE,

			name                   VARCHAR(64) NOT NULL,
			type                   VARCHAR(64) NOT NULL,
			group_name             VARCHAR(64) NOT NULL,
			resource_os            VARCHAR(64) NOT NULL,
			registered_at          BIGINT,

			agent_name             VARCHAR(64) NOT NULL,
			agent_is_active        {$boolean},
			agent_version          VARCHAR(64) NOT NULL,

			customer_name          VARCHAR(64) NOT NULL,
			unit_name              VARCHAR(64) NOT NULL,

			applied_policy         VARCHAR(64) NOT NULL,
			state                  VARCHAR(64) NOT NULL,
			last_result            VARCHAR(64) NOT NULL,

			last_backup            BIGINT,
			next_backup            BIGINT,
			archives_count         BIGINT,
			backups_count          BIGINT,
			oldest_backup          BIGINT,
			latest_backup          BIGINT,
			used_total             BIGINT,
			used_cloud             BIGINT,
			used_local             BIGINT,

			alerts_count           BIGINT,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"name"}, {"type"}, {"group_name"}, {"registered_at"}, {"agent_name"}, {"agent_is_active"}},
}

// TestTableTenants is table to store tenants
var TestTableTenants = TestTable{}

// TestTableTenantsClosure is table to store tenants closure
var TestTableTenantsClosure = TestTable{}

// TestTableCTIEntities is table to store CTI entities
var TestTableCTIEntities = TestTable{}

/*
 * Main part
 */

// TestTables is a map of all test tables available for the benchmark run
var TestTables = map[string]TestTable{
	"acronis_db_bench_light":                     TestTableLight,
	"acronis_db_bench_medium":                    TestTableMedium,
	"acronis_db_bench_heavy":                     TestTableHeavy,
	"acronis_db_bench_vector_768":                TestTableVector768,
	"acronis_db_bench_email_security":            TestTableEmailSecurity,
	"acronis_db_bench_blob":                      TestTableBlob,
	"acronis_db_bench_largeobj":                  TestTableLargeObj,
	"acronis_db_bench_json":                      TestTableJSON,
	"acronis_db_bench_ts_sql":                    TestTableTimeSeriesSQL,
	"acronis_db_bench_cybercache_tenants":        TestTableTenants,
	"acronis_db_bench_cybercache_tenant_closure": TestTableTenantsClosure,
	"acronis_db_bench_advm_tasks":                TestTableAdvmTasks,
	"acronis_db_bench_advm_resources":            TestTableAdvmResources,
	"acronis_db_bench_advm_resources_statuses":   TestTableAdvmResourcesStatuses,
	"acronis_db_bench_advm_agent_resources":      TestTableAdvmAgentsResources,
	"acronis_db_bench_advm_agents":               TestTableAdvmAgents,
	"acronis_db_bench_advm_backup_resources":     TestTableAdvmBackupResources,
	"acronis_db_bench_advm_backups":              TestTableAdvmBackups,
	"acronis_db_bench_advm_archives":             TestTableAdvmArchives,
	"acronis_db_bench_advm_vaults":               TestTableAdvmVaults,
	"acronis_db_bench_advm_devices":              TestTableAdvmDevices,
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
