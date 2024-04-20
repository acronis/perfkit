package main

import (
	"fmt"
	"strconv"
	"strings"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/lib/pq"

	"github.com/acronis/perfkit/benchmark"
)

const (
	TestSelect      string = "select"      // TestSelect is a test category for SELECT queries
	TestUpdate      string = "update"      // TestUpdate is a test category for UPDATE queries
	TestInsert      string = "insert"      // TestInsert is a test category for INSERT queries
	TestDelete      string = "delete"      // TestDelete is a test category for DELETE queries
	TestTransaction string = "transaction" // TestTransaction is a test category for transactions
	TestOther       string = "other"       // TestOther is a test category for other queries
)

// MinChunk is a minimum number of rows to process in a single chunk
const MinChunk = 5000

// TestGroup is a group of tests
type TestGroup struct {
	name  string
	tests map[string]*TestDesc
}

// NewTestGroup creates a new test group
func NewTestGroup(name string) *TestGroup {
	return &TestGroup{name: name, tests: make(map[string]*TestDesc)}
}

var allTests *TestGroup

func (g *TestGroup) add(t *TestDesc) {
	g.tests[t.name] = t
	_, exists := allTests.tests[t.name]
	if exists {
		benchmark.FatalError("Internal error: test %s already defined")
	}
	allTests.tests[t.name] = t
}

// TestCategories is a list of all test categories
var TestCategories = []string{TestSelect, TestUpdate, TestInsert, TestDelete, TestTransaction}

type testWorkerFunc func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int)
type orderByFunc func(b *benchmark.Benchmark) string //nolint:unused
type launcherFunc func(b *benchmark.Benchmark, testDesc *TestDesc)

// TestDesc describes a test
type TestDesc struct {
	name        string
	metric      string
	description string
	category    string
	isReadonly  bool // indicates the test doesn't run DDL and doesn't modidy data
	isDBRTest   bool
	databases   []string

	table TestTable // SQL table name

	launcherFunc launcherFunc
}

// dbIsSupported returns true if the database is supported by the test
func (t *TestDesc) dbIsSupported(db string) bool {
	for _, b := range t.databases {
		if b == db {
			return true
		}
	}

	return false
}

// getDBs returns a string with supported databases
func (t *TestDesc) getDBs() string {
	ret := "["

	for _, db := range benchmark.GetDatabases() {
		if t.dbIsSupported(db.Driver) {
			ret += db.Symbol
		} else {
			ret += "-"
		}
	}
	ret += "]"

	return ret
}

var (
	// ALL is a list of all supported databases
	ALL = []string{benchmark.POSTGRES, benchmark.MYSQL, benchmark.MSSQL, benchmark.SQLITE, benchmark.CLICKHOUSE, benchmark.CASSANDRA}
	// RELATIONAL is a list of all supported relational databases
	RELATIONAL = []string{benchmark.POSTGRES, benchmark.MYSQL, benchmark.MSSQL, benchmark.SQLITE}
	// PMWSA is a list of all supported databases except ClickHouse
	PMWSA = []string{benchmark.POSTGRES, benchmark.MYSQL, benchmark.MSSQL, benchmark.SQLITE, benchmark.CASSANDRA}
)

// TestBaseAll tests all tests in the 'base' group
var TestBaseAll = TestDesc{
	name:        "all",
	description: "execute all tests in the 'base' group",
	databases:   ALL,
	//	launcherFunc: ...  # causes 'initialization cycle' go-lang compiler error
}

// TestPing tests just ping DB
var TestPing = TestDesc{
	name:        "ping",
	metric:      "ping/sec",
	description: "just ping DB",
	category:    TestOther,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   ALL,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		worker := func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
			err := c.Ping()
			if err != nil {
				return 0
			}

			return 1
		}
		testGeneric(b, testDesc, worker, 0)
	},
}

// TestRawQuery tests do custom DB query execution
var TestRawQuery = TestDesc{
	name:        "custom",
	metric:      "queries/sec",
	description: "custom DB query execution",
	category:    TestOther,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   ALL,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		query := b.TestOpts.(*TestOpts).BenchOpts.Query

		var worker testWorkerFunc

		if strings.Contains(query, "{") {
			worker = func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
				q := query
				if strings.Contains(q, "{CTI}") {
					rw := b.Randomizer.GetWorker(c.WorkerID)
					ctiUUID, err := b.TenantsCache.GetRandomCTIUUID(rw, 0)
					if err != nil {
						b.Exit(err.Error())
					}
					q = strings.Replace(q, "{CTI}", "'"+string(ctiUUID)+"'", -1)
				}
				if strings.Contains(query, "{TENANT}") {
					rw := b.Randomizer.GetWorker(c.WorkerID)
					tenantUUID, err := b.TenantsCache.GetRandomTenantUUID(rw, 0)
					if err != nil {
						b.Exit(err.Error())
					}
					q = strings.Replace(q, "{TENANT}", "'"+string(tenantUUID)+"'", -1)
				}
				fmt.Printf("query %s\n", q)
				c.SelectRaw(b.TestOpts.(*TestOpts).BenchOpts.Explain, q)

				return 1
			}
		} else {
			worker = func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
				c.SelectRaw(b.TestOpts.(*TestOpts).BenchOpts.Explain, query)

				return 1
			}
		}
		testGeneric(b, testDesc, worker, 0)
	},
}

// TestSelectOne tests do 'SELECT 1'
var TestSelectOne = TestDesc{
	name:        "select-1",
	metric:      "select/sec",
	description: "just do 'SELECT 1'",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   ALL,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		worker := func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
			c.SelectRaw(b.TestOpts.(*TestOpts).BenchOpts.Explain, "SELECT 1")

			return 1
		}
		testGeneric(b, testDesc, worker, 0)
	},
}

// TestSelectOneDBR tests do 'SELECT 1' using golang DBR query builder
var TestSelectOneDBR = TestDesc{
	name:        "dbr-select-1",
	metric:      "select/sec",
	description: "do 'SELECT 1' using golang DBR query builder",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   true,
	databases:   RELATIONAL,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		worker := func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
			var ret int
			if err := c.DbrSess().Select("1").LoadOne(&ret); err != nil {
				b.Exit("DBRSelect load error: %v", err)
			}
			c.DBRLogQuery(ret)

			return 1
		}
		testGeneric(b, testDesc, worker, 0)
	},
}

// TestSelectNextVal tests increment a DB sequence in a loop (or use SELECT FOR UPDATE, UPDATE)
var TestSelectNextVal = TestDesc{
	name:        "select-nextval",
	metric:      "ops/sec",
	description: "increment a DB sequence in a loop (or use SELECT FOR UPDATE, UPDATE)",
	category:    TestOther,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		c := dbConnector(b)
		c.CreateSequence(benchmark.SequenceName)
		c.Close()

		worker := func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
			c.GetNextVal(benchmark.SequenceName)

			return 1
		}

		testGeneric(b, testDesc, worker, 0)
	},
}

// TestSelectMediumLast tests select last row from the 'medium' table with few columns and 1 index
var TestSelectMediumLast = TestDesc{
	name:        "select-medium-last",
	metric:      "rows/sec",
	description: "select last row from the 'medium' table with few columns and 1 index",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   ALL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		orderby := func(b *benchmark.Benchmark) string { return "id DESC" }
		testSelect(b, testDesc, nil, "id", nil, orderby, 1)
	},
}

// TestSelectMediumLastDBR tests select last row from the 'medium' table with few columns and 1 index using golang DBR query builder
var TestSelectMediumLastDBR = TestDesc{
	name:        "dbr-select-medium-last",
	metric:      "rows/sec",
	description: "select last row from the 'medium' table with few columns and 1 index",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		orderby := func(b *benchmark.Benchmark) string { return "id DESC" }
		testSelect(b, testDesc, nil, "id", nil, orderby, 1)
	},
}

// TestSelectMediumRand selects random row from the 'medium' table with few columns and 1 index
var TestSelectMediumRand = TestDesc{
	name:        "select-medium-rand",
	metric:      "rows/sec",
	description: "select random row from the 'medium' table with few columns and 1 index",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   ALL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.table.RowsCount - 1)

			return fmt.Sprintf("id > %d", id)
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id ASC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSelectMediumRandDBR selects random row from the 'medium' table using golang DBR query builder
var TestSelectMediumRandDBR = TestDesc{
	name:        "dbr-select-medium-rand",
	metric:      "rows/sec",
	description: "select random row from the 'medium' table using golang DBR query builder",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.table.RowsCount - 1)

			return fmt.Sprintf("id > %d", id)
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id ASC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSelectHeavyLast selects last row from the 'heavy' table
var TestSelectHeavyLast = TestDesc{
	name:        "select-heavy-last",
	metric:      "rows/sec",
	description: "select last row from the 'heavy' table",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		orderby := func(b *benchmark.Benchmark) string { return "id DESC" }
		testSelect(b, testDesc, nil, "id", nil, orderby, 1)
	},
}

// TestSelectHeavyLastDBR selects last row from the 'heavy' table using golang DBR driver
var TestSelectHeavyLastDBR = TestDesc{
	name:        "dbr-select-heavy-last",
	metric:      "rows/sec",
	description: "select last row from the 'heavy' table using golang DBR driver",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		orderby := func(b *benchmark.Benchmark) string { return "id DESC" }
		testSelect(b, testDesc, nil, "id", nil, orderby, 1)
	},
}

// TestSelectHeavyRand selects random row from the 'heavy' table
var TestSelectHeavyRand = TestDesc{
	name:        "select-heavy-rand",
	metric:      "rows/sec",
	description: "select random row from the 'heavy' table",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.table.RowsCount - 1)

			return fmt.Sprintf("id > %d", id)
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id ASC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSelectHeavyRandDBR selects random row from the 'heavy' table using golang DBR query builder
var TestSelectHeavyRandDBR = TestDesc{
	name:        "dbr-select-heavy-rand",
	metric:      "rows/sec",
	description: "select random row from the 'heavy' table using golang DBR query builder",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.table.RowsCount - 1)

			return fmt.Sprintf("id > %d", id)
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id ASC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSelectHeavyRandTenantLike selects random row from the 'heavy' table WHERE tenant_id = {} AND resource_name LIKE {}
var TestSelectHeavyRandTenantLike = TestDesc{
	name:        "select-heavy-rand-in-tenant-like",
	metric:      "rows/sec",
	description: "select random row from the 'heavy' table WHERE tenant_id = {} AND resource_name LIKE {}",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {

		colConfs := testDesc.table.GetColumnsConf([]string{"tenant_id"}, false)

		where := func(b *benchmark.Benchmark, workerId int) string {
			w := b.GenFakeDataAsMap(workerId, colConfs, false)

			return fmt.Sprintf("tenant_id = '%s' AND resource_name LIKE '%s'", (*w)["tenant_id"], "%a%")
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id DESC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSelectHeavyMinMaxTenant selects min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {}
var TestSelectHeavyMinMaxTenant = TestDesc{
	name:        "select-heavy-minmax-in-tenant",
	metric:      "rows/sec",
	description: "select min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {}",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {

		colConfs := testDesc.table.GetColumnsConf([]string{"tenant_id"}, false)

		where := func(b *benchmark.Benchmark, workerId int) string {
			w := b.GenFakeDataAsMap(workerId, colConfs, false)

			return fmt.Sprintf("tenant_id = '%s'", (*w)["tenant_id"])
		}
		testSelect(b, testDesc, nil, "min(completion_time_ns), max(completion_time_ns)", where, nil, 1)
	},
}

// TestSelectHeavyMinMaxTenantAndState selects min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {} AND state = {}
var TestSelectHeavyMinMaxTenantAndState = TestDesc{
	name:        "select-heavy-minmax-in-tenant-and-state",
	metric:      "rows/sec",
	description: "select min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {} AND state = {}",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {

		colConfs := testDesc.table.GetColumnsConf([]string{"tenant_id", "state"}, false)

		where := func(b *benchmark.Benchmark, workerId int) string {
			w := b.GenFakeDataAsMap(workerId, colConfs, false)

			return fmt.Sprintf("tenant_id = '%s' AND state = '%d'", (*w)["tenant_id"], (*w)["state"])
		}
		testSelect(b, testDesc, nil, "min(completion_time_ns), max(completion_time_ns)", where, nil, 1)
	},
}

// TestSelectHeavyForUpdateSkipLocked selects a row from the 'heavy' table and then updates it
var TestSelectHeavyForUpdateSkipLocked = TestDesc{
	name:        "select-heavy-for-update-skip-locked",
	metric:      "updates/sec",
	description: "do SELECT FOR UPDATE SKIP LOCKED and then UPDATE",
	category:    TestOther,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		var query string
		max := b.CommonOpts.Workers*2 + 1

		switch b.TestOpts.(*TestOpts).DBOpts.Driver {
		case benchmark.POSTGRES, benchmark.MYSQL:
			query = fmt.Sprintf("SELECT id, progress FROM acronis_db_bench_heavy WHERE id < %d LIMIT 1 FOR UPDATE SKIP LOCKED", max)
		case benchmark.MSSQL:
			query = fmt.Sprintf("SELECT TOP(1) id, progress FROM acronis_db_bench_heavy WITH (UPDLOCK, READPAST, ROWLOCK) WHERE id < %d", max)
		default:
			b.Exit("unsupported driver: '%v', supported drivers are: %s|%s|%s", b.TestOpts.(*TestOpts).DBOpts.Driver, benchmark.POSTGRES, benchmark.MYSQL, benchmark.MSSQL)
		}

		worker := func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
			var id int64
			var progress int

			c.QueryRowAndScan(query, &id, &progress)
			c.ExecOrExit(fmt.Sprintf("UPDATE acronis_db_bench_heavy SET progress = %d WHERE id = %d", progress+1, id))

			return 1
		}
		testGeneric(b, testDesc, worker, 10000)
	},
}

// TestInsertLight inserts a row into the 'light' table
var TestInsertLight = TestDesc{
	name:        "insert-light",
	metric:      "rows/sec",
	description: "insert a row into the 'light' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   ALL,
	table:       TestTableLight,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// insertByPreparedDataWorker inserts a row into the 'light' table using prepared statement for the batch
func insertByPreparedDataWorker(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
	colConfs := testDesc.table.GetColumnsForInsert(benchmark.WithAutoInc(c.DbOpts.Driver))
	workerID := c.WorkerID

	tx := c.Begin()

	columns, _ := b.GenFakeData(workerID, colConfs, false)

	parametersPlaceholder := benchmark.GenDBParameterPlaceholders(0, len(*colConfs))
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", testDesc.table.TableName, strings.Join(columns, ","), parametersPlaceholder)
	sql = formatSQL(sql, c.DbOpts.Driver)

	t := c.StatementEnter(sql, nil)
	stmt, err := tx.Prepare(sql)
	c.StatementExit("Prepare()", t, err, false, nil, sql, nil, nil, nil)

	if err != nil {
		c.Exit(err.Error())
	}
	for i := 0; i < batch; i++ {
		_, values := b.GenFakeData(workerID, colConfs, false)

		t := c.StatementEnter("", nil)
		_, err = stmt.Exec(values...)
		c.StatementExit("Exec()", t, err, false, nil, "<< stdin ", values, nil, nil)

		if err != nil {
			stmt.Close() //nolint:sqlclosecheck
			c.Exit(err.Error())
		}
	}
	c.Commit()

	return batch
}

// TestInsertLightPrepared inserts a row into the 'light' table using prepared statement for the batch
var TestInsertLightPrepared = TestDesc{
	name:        "insert-light-prepared",
	metric:      "rows/sec",
	description: "insert a row into the 'light' table using prepared statement for the batch",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableLight,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, insertByPreparedDataWorker, 0)
	},
}

// insertMultiValueDataWorker inserts a row into the 'light' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...)
func insertMultiValueDataWorker(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
	colConfs := testDesc.table.GetColumnsForInsert(benchmark.WithAutoInc(c.DbOpts.Driver))
	workerID := c.WorkerID

	columns, _ := b.GenFakeData(workerID, colConfs, benchmark.WithAutoInc(c.DbOpts.Driver))
	if c.DbOpts.Driver == benchmark.CASSANDRA {
		var values []interface{}
		var sql string

		sqlInsertTpl := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", testDesc.table.TableName, strings.Join(columns, ","))

		for i := 0; i < batch; i++ {
			sql = fmt.Sprintf("%s\n%s (%s);", sql, sqlInsertTpl, benchmark.GenDBParameterPlaceholdersCassandra(i*len(*colConfs), len(*colConfs)))
		}

		sql = formatSQL(sql, c.DbOpts.Driver)

		for i := 0; i < batch; i++ {
			_, vals := b.GenFakeData(workerID, colConfs, benchmark.WithAutoInc(c.DbOpts.Driver))
			values = append(values, vals...)
		}

		sql = fmt.Sprintf("BEGIN BATCH%s\nAPPLY BATCH;", sql)
		c.ExecOrExit(sql, values...)

		return batch
	}

	var values []interface{}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", testDesc.table.TableName, strings.Join(columns, ","))

	for i := 0; i < batch; i++ {
		if i == 0 {
			sql = fmt.Sprintf("%s (%s)", sql, benchmark.GenDBParameterPlaceholders(i*len(*colConfs), len(*colConfs)))
		} else {
			sql = fmt.Sprintf("%s, (%s)", sql, benchmark.GenDBParameterPlaceholders(i*len(*colConfs), len(*colConfs)))
		}
	}

	sql = formatSQL(sql, c.DbOpts.Driver)

	for i := 0; i < batch; i++ {
		_, vals := b.GenFakeData(workerID, colConfs, benchmark.WithAutoInc(c.DbOpts.Driver))
		values = append(values, vals...)
	}

	c.Begin()
	c.ExecOrExit(sql, values...)
	c.Commit()

	return batch
}

// TestInsertLightMultiValue inserts a row into the 'light' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...)
var TestInsertLightMultiValue = TestDesc{
	name:        "insert-light-multivalue",
	metric:      "rows/sec",
	description: "insert a row into the 'light' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) ",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   PMWSA,
	table:       TestTableLight,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, insertMultiValueDataWorker, 0)
	},
}

// copyDataWorker copies a row into the 'light' table
func copyDataWorker(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
	var sql string
	colConfs := testDesc.table.GetColumnsForInsert(benchmark.WithAutoInc(c.DbOpts.Driver))
	workerID := c.WorkerID

	tx := c.Begin()

	columns, _ := b.GenFakeData(workerID, colConfs, false)

	switch c.DbOpts.Driver {
	case benchmark.POSTGRES:
		sql = pq.CopyIn(testDesc.table.TableName, columns...)
	case benchmark.MSSQL:
		sql = mssql.CopyIn(testDesc.table.TableName, mssql.BulkOptions{KeepNulls: true, RowsPerBatch: batch}, columns...)
	default:
		b.Exit("unsupported driver: '%v', supported drivers are: %s|%s", b.TestOpts.(*TestOpts).DBOpts.Driver, benchmark.POSTGRES, benchmark.MSSQL)
	}

	t := c.StatementEnter(sql, nil)
	stmt, err := tx.Prepare(sql)
	c.StatementExit("Prepare()", t, err, false, nil, sql, nil, nil, nil)

	if err != nil {
		c.Exit(err.Error())
	}
	for i := 0; i < batch; i++ {
		_, values := b.GenFakeData(workerID, colConfs, false)

		t := c.StatementEnter("", nil)
		_, err = stmt.Exec(values...)
		c.StatementExit("Exec()", t, err, false, nil, "<< stdin ", values, nil, nil)

		if err != nil {
			stmt.Close() //nolint:sqlclosecheck
			c.Exit(err.Error())
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		stmt.Close()
		c.Exit(err.Error())
	}
	c.Commit()

	return batch
}

// TestCopyLight copies a row into the 'light' table
var TestCopyLight = TestDesc{
	name:        "copy-light",
	metric:      "rows/sec",
	description: "copy a row into the 'light' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES, benchmark.MSSQL},
	table:       TestTableLight,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, copyDataWorker, 0)
	},
}

// TestInsertLightDBR inserts a row into the 'light' table using goland DBR query builder
var TestInsertLightDBR = TestDesc{
	name:        "dbr-insert-light",
	metric:      "rows/sec",
	description: "insert a row into the 'light' table using goland DBR query builder",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableLight,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertMedium inserts a row into the 'medium' table
var TestInsertMedium = TestDesc{
	name:        "insert-medium",
	metric:      "rows/sec",
	description: "insert a row into the 'medium' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   ALL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertMediumPrepared inserts a row into the 'medium' table using prepared statement for the batch
var TestInsertMediumPrepared = TestDesc{
	name:        "insert-medium-prepared",
	metric:      "rows/sec",
	description: "insert a row into the 'medium' table using prepared statement for the batch",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, insertByPreparedDataWorker, 0)
	},
}

// TestInsertMediumMultiValue inserts a row into the 'medium' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...)
var TestInsertMediumMultiValue = TestDesc{
	name:        "insert-medium-multivalue",
	metric:      "rows/sec",
	description: "insert a row into the 'medium' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) ",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   PMWSA,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, insertMultiValueDataWorker, 0)
	},
}

// TestCopyMedium copies a row into the 'medium' table
var TestCopyMedium = TestDesc{
	name:        "copy-medium",
	metric:      "rows/sec",
	description: "copy a row into the 'medium' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES, benchmark.MSSQL},
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, copyDataWorker, 0)
	},
}

// TestInsertMediumDBR inserts a row into the 'medium' table using goland DBR query builder
var TestInsertMediumDBR = TestDesc{
	name:        "dbr-insert-medium",
	metric:      "rows/sec",
	description: "insert a row into the 'medium' table using goland DBR query builder",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertBlob inserts a row with large random blob into the 'blob' table
var TestInsertBlob = TestDesc{
	name:        "insert-blob",
	metric:      "rows/sec",
	description: "insert a row with large random blob into the 'blob' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   ALL,
	table:       TestTableBlob,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testDesc.table.InitColumnsConf()
		for i := range testDesc.table.ColumnsConf {
			if testDesc.table.ColumnsConf[i].ColumnType == "blob" {
				testDesc.table.ColumnsConf[i].MaxSize = b.TestOpts.(*TestOpts).TestcaseOpts.MaxBlobSize
				testDesc.table.ColumnsConf[i].MinSize = b.TestOpts.(*TestOpts).TestcaseOpts.MinBlobSize
			}
		}
		testInsertGeneric(b, testDesc)
	},
}

// TestCopyBlob copies a row with large random blob into the 'blob' table
var TestCopyBlob = TestDesc{
	name:        "copy-blob",
	metric:      "rows/sec",
	description: "copy a row with large random blob into the 'blob' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES, benchmark.MSSQL},
	table:       TestTableBlob,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testDesc.table.InitColumnsConf()
		for i := range testDesc.table.ColumnsConf {
			if testDesc.table.ColumnsConf[i].ColumnType == "blob" {
				testDesc.table.ColumnsConf[i].MaxSize = b.TestOpts.(*TestOpts).TestcaseOpts.MaxBlobSize
				testDesc.table.ColumnsConf[i].MinSize = b.TestOpts.(*TestOpts).TestcaseOpts.MinBlobSize
			}
		}
		testGeneric(b, testDesc, copyDataWorker, 0)
	},
}

// createLargeObjectWorker inserts a row with large random object into the 'largeobject' table
func createLargeObjectWorker(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
	colConfs := testDesc.table.GetColumnsForInsert(benchmark.WithAutoInc(c.DbOpts.Driver))
	parametersPlaceholder := benchmark.GenDBParameterPlaceholders(0, len(*colConfs))
	testOpts := b.TestOpts.(*TestOpts)
	workerID := c.WorkerID

	c.Begin()

	var sql string

	for i := 0; i < batch; i++ {
		columns, values := b.GenFakeData(workerID, colConfs, false)

		blob := b.GenFakeValue(workerID, "blob", "", 0, b.TestOpts.(*TestOpts).TestcaseOpts.MaxBlobSize, b.TestOpts.(*TestOpts).TestcaseOpts.MinBlobSize, "")

		var oid int
		var fd int

		c.QueryRowAndScan("SELECT lo_create(0)", &oid)
		c.QueryRowAndScan(fmt.Sprintf("SELECT lo_open(%d, 131072)", oid), &fd) // 131072 == 0x20000 - write mode

		c.ExecOrExit("SELECT lowrite($1, $2)", fd, blob)
		c.ExecOrExit("SELECT lo_close($1)", fd)

		for c := range columns {
			if columns[c] == "oid" {
				values[c] = oid
			}
		}

		if i == 0 {
			insertSQL := "INSERT INTO %s (%s) VALUES(%s)"
			sqlTemplate := fmt.Sprintf(insertSQL, testDesc.table.TableName, strings.Join(columns, ","), parametersPlaceholder)
			sql = formatSQL(sqlTemplate, testOpts.DBOpts.Driver)
		}

		c.ExecOrExit(sql, values...)
	}
	c.Commit()

	return batch
}

// TestInsertLargeObj inserts a row with large random object into the 'largeobject' table
var TestInsertLargeObj = TestDesc{
	name:        "insert-largeobj",
	metric:      "rows/sec",
	description: "insert a row with large random object into the 'largeobject' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableLargeObj,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, createLargeObjectWorker, 0)
	},
}

// TestInsertHeavy inserts a row into the 'heavy' table
var TestInsertHeavy = TestDesc{
	name:        "insert-heavy",
	metric:      "rows/sec",
	description: "insert a row into the 'heavy' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertHeavyPrepared inserts a row into the 'heavy' table using prepared statement for the batch
var TestInsertHeavyPrepared = TestDesc{
	name:        "insert-heavy-prepared",
	metric:      "rows/sec",
	description: "insert a row into the 'heavy' table using prepared statement for the batch",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, insertByPreparedDataWorker, 0)
	},
}

// TestInsertHeavyMultivalue inserts a row into the 'heavy' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) "
var TestInsertHeavyMultivalue = TestDesc{
	name:        "insert-heavy-multivalue",
	metric:      "rows/sec",
	description: "insert a row into the 'heavy' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) ",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, insertMultiValueDataWorker, 0)
	},
}

// TestCopyHeavy copies a row into the 'heavy' table
var TestCopyHeavy = TestDesc{
	name:        "copy-heavy",
	metric:      "rows/sec",
	description: "copy a row into the 'heavy' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES, benchmark.MSSQL},
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, copyDataWorker, 0)
	},
}

// TestInsertHeavyDBR inserts a row into the 'heavy' table using golang DB query builder
var TestInsertHeavyDBR = TestDesc{
	name:        "dbr-insert-heavy",
	metric:      "rows/sec",
	description: "insert a row into the 'heavy' table using golang DB query builder",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertJSON inserts a row into a table with JSON(b) column
var TestInsertJSON = TestDesc{
	name:        "insert-json",
	metric:      "rows/sec",
	description: "insert a row into a table with JSON(b) column",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableJSON,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertJSONDBR inserts a row into a table with JSON(b) column using golang DBR driver
var TestInsertJSONDBR = TestDesc{
	name:        "dbr-insert-json",
	metric:      "rows/sec",
	description: "insert a row into a table with JSON(b) column using golang DBR driver",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableJSON,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestSelectJSONByIndexedValue selects a row from the 'json' table by some json condition
var TestSelectJSONByIndexedValue = TestDesc{
	name:        "select-json-by-indexed-value",
	metric:      "rows/sec",
	description: "select a row from the 'json' table by some json condition",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableJSON,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.table.RowsCount - 1)

			driver := b.TestOpts.(*TestOpts).DBOpts.Driver

			switch driver {
			case benchmark.MYSQL:
				return "_data_f0f0 = '10' AND id > " + strconv.FormatUint(id, 10)
			case benchmark.POSTGRES:
				return "json_data @> '{\"field0\": {\"field0\": 10}}' AND id > " + strconv.FormatUint(id, 10)
			default:
				b.Exit("The %s test is not supported on driver: %s", testDesc.name, driver)
			}

			return ""
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id ASC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSearchJSONByIndexedValue searches a row from the 'json' table using some json condition using LIKE {}
var TestSearchJSONByIndexedValue = TestDesc{
	name:        "search-json-by-indexed-value",
	metric:      "rows/sec",
	description: "search a row from the 'json' table using some json condition using LIKE {}",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableJSON,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.table.RowsCount - 1)

			driver := b.TestOpts.(*TestOpts).DBOpts.Driver

			switch driver {
			case benchmark.MYSQL:
				return "_data_f0f0f0 LIKE '%eedl%' AND id > " + strconv.FormatUint(id, 10)
			case benchmark.POSTGRES:
				return "json_data->'field0'->'field0'->>'field0' LIKE '%eedl%' AND id > " + strconv.FormatUint(id, 10) // searching for the 'needle' word
			default:
				b.Exit("The %s test is not supported on driver: %s", testDesc.name, driver)
			}

			return ""
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id ASC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSelectJSONByNonIndexedValue selects a row from the 'json' table by some json condition
var TestSelectJSONByNonIndexedValue = TestDesc{
	name:        "select-json-by-nonindexed-value",
	metric:      "rows/sec",
	description: "select a row from the 'json' table by some json condition",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableJSON,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.table.RowsCount - 1)

			driver := b.TestOpts.(*TestOpts).DBOpts.Driver

			switch driver {
			case benchmark.MYSQL:
				return "JSON_EXTRACT(json_data, '$.field0.field1') = '10' AND id > " + strconv.FormatUint(id, 10)
			case benchmark.POSTGRES:
				return "json_data @> '{\"field0\": {\"field1\": 10}}' AND id > " + strconv.FormatUint(id, 10)
			default:
				b.Exit("The %s test is not supported on driver: %s", testDesc.name, driver)
			}

			return ""
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id ASC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSearchJSONByNonIndexedValue searches a row from the 'json' table using some json condition using LIKE {}
var TestSearchJSONByNonIndexedValue = TestDesc{
	name:        "search-json-by-nonindexed-value",
	metric:      "rows/sec",
	description: "search a row from the 'json' table using some json condition using LIKE {}",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableJSON,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.table.RowsCount - 1)

			driver := b.TestOpts.(*TestOpts).DBOpts.Driver

			switch driver {
			case benchmark.MYSQL:
				return "JSON_EXTRACT(json_data, '$.field0.field1') LIKE '%eedl%' AND id > " + strconv.FormatUint(id, 10)
			case benchmark.POSTGRES:
				return "json_data->'field0'->'field0'->>'field0' LIKE '%eedl%' AND id > " + strconv.FormatUint(id, 10) // searching for the 'needle' word
			default:
				b.Exit("The %s test is not supported on driver: %s", testDesc.name, driver)
			}

			return ""
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id ASC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestUpdateMedium updates random row in the 'medium' table
var TestUpdateMedium = TestDesc{
	name:        "update-medium",
	metric:      "rows/sec",
	description: "update random row in the 'medium' table",
	category:    TestUpdate,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testUpdateGeneric(b, testDesc, 1, nil)
	},
}

// TestUpdateMediumDBR updates random row in the 'medium' table using golang DBR query builder
var TestUpdateMediumDBR = TestDesc{
	name:        "dbr-update-medium",
	metric:      "rows/sec",
	description: "update random row in the 'medium' table using golang DB driver",
	category:    TestUpdate,
	isReadonly:  false,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testUpdateGeneric(b, testDesc, 1, nil)
	},
}

// TestUpdateHeavy updates random row in the 'heavy' table
var TestUpdateHeavy = TestDesc{
	name:        "update-heavy",
	metric:      "rows/sec",
	description: "update random row in the 'heavy' table",
	category:    TestUpdate,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testUpdateGeneric(b, testDesc, 1, nil)
	},
}

// TestUpdateHeavyDBR updates random row in the 'heavy' table using golang DBR query builder
var TestUpdateHeavyDBR = TestDesc{
	name:        "dbr-update-heavy",
	metric:      "rows/sec",
	description: "update random row in the 'heavy' table using golang DB driver",
	category:    TestUpdate,
	isReadonly:  false,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testUpdateGeneric(b, testDesc, 1, nil)
	},
}

// TestUpdateHeavyBulk updates N rows (see --batch=, default 50000) in the 'heavy' table by single transaction
var TestUpdateHeavyBulk = TestDesc{
	name:        "bulkupdate-heavy",
	metric:      "rows/sec",
	description: "update N rows (see --batch=, default 50000) in the 'heavy' table by single transaction",
	category:    TestUpdate,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		origBatch := b.Vault.(*DBTestData).EffectiveBatch
		testBatch := origBatch
		if b.TestOpts.(*TestOpts).BenchOpts.Batch == 0 {
			testBatch = 50000
		}
		b.Vault.(*DBTestData).EffectiveBatch = 1

		testUpdateGeneric(b, testDesc, uint64(testBatch), nil)

		b.Vault.(*DBTestData).EffectiveBatch = origBatch
	},
}

// TestUpdateHeavyBulkDBR updates N rows (see --batch=, default 50000) in the 'heavy' table by single transaction using DBR query builder
var TestUpdateHeavyBulkDBR = TestDesc{
	name:        "dbr-bulkupdate-heavy",
	metric:      "rows/sec",
	description: "update N rows (see --update-rows-count= ) in the 'heavy' table by single transaction using DBR query builder",
	category:    TestUpdate,
	isReadonly:  false,
	isDBRTest:   true,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		origBatch := b.Vault.(*DBTestData).EffectiveBatch
		b.Vault.(*DBTestData).EffectiveBatch = 1
		testBatch := origBatch
		if b.TestOpts.(*TestOpts).BenchOpts.Batch == 0 {
			testBatch = 50000
		}

		testUpdateGeneric(b, testDesc, uint64(testBatch), nil)

		b.Vault.(*DBTestData).EffectiveBatch = origBatch
	},
}

// TestUpdateHeavySameVal updates random row in the 'heavy' table putting the value which already exists
var TestUpdateHeavySameVal = TestDesc{
	name:        "update-heavy-sameval",
	metric:      "rows/sec",
	description: "update random row in the 'heavy' table putting the value which already exists",
	category:    TestUpdate,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		colConf := testDesc.table.GetColumnsConf([]string{"const_val"}, false)
		testUpdateGeneric(b, testDesc, 1, colConf)
	},
}

// TestUpdateHeavyPartialSameVal updates random row in the 'heavy' table putting two values, where one of them is already exists in this row
var TestUpdateHeavyPartialSameVal = TestDesc{
	name:        "update-heavy-partial-sameval",
	metric:      "rows/sec",
	description: "update random row in the 'heavy' table putting two values, where one of them is already exists in this row",
	category:    TestUpdate,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		colConf := testDesc.table.GetColumnsConf([]string{"const_val", "progress"}, false)
		testUpdateGeneric(b, testDesc, 1, colConf)
	},
}

/*
 * Tenant-specific tests
 */

// TestInsertTenant inserts into the 'tenants' table
var TestInsertTenant = TestDesc{
	name:        "insert-tenant",
	metric:      "tenants/sec",
	description: "insert a tenant into the 'tenants' table",
	category:    TestInsert,
	databases:   ALL,
	table:       TestTableTenants,
	isReadonly:  false,
	isDBRTest:   false,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, CreateTenantWorker, 0)
	},
}

// TestInsertCTI inserts into the 'cti' table
var TestInsertCTI = TestDesc{
	name:        "insert-cti",
	metric:      "ctiEntity/sec",
	description: "insert a CTI entity into the 'cti' table",
	category:    TestInsert,
	databases:   ALL,
	table:       TestTableCTIEntities,
	isReadonly:  false,
	isDBRTest:   false,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testGeneric(b, testDesc, CreateCTIEntityWorker, 0)
	},
}

/*
 * Timeseries tests
 */

// TestInsertTimeSeriesSQL inserts into the 'timeseries' SQL table
var TestInsertTimeSeriesSQL = TestDesc{
	name:        "insert-ts-sql",
	metric:      "values/sec",
	description: "batch insert into the 'timeseries' SQL table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   PMWSA,
	table:       TestTableTimeSeriesSQL,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {

		origBatch := b.Vault.(*DBTestData).EffectiveBatch
		if b.TestOpts.(*TestOpts).BenchOpts.Batch == 0 {
			b.Vault.(*DBTestData).EffectiveBatch = 256
		}

		testInsertGeneric(b, testDesc)

		b.Vault.(*DBTestData).EffectiveBatch = origBatch
	},
}

// TestSelectTimeSeriesSQL selects last inserted row from the 'timeseries' SQL table
var TestSelectTimeSeriesSQL = TestDesc{
	name:        "select-ts-sql",
	metric:      "values/sec",
	description: "batch select from the 'timeseries' SQL table",
	category:    TestSelect,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   PMWSA,
	table:       TestTableTimeSeriesSQL,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {

		origBatch := b.Vault.(*DBTestData).EffectiveBatch
		if b.TestOpts.(*TestOpts).BenchOpts.Batch == 0 {
			b.Vault.(*DBTestData).EffectiveBatch = 256
		}

		colConfs := testDesc.table.GetColumnsConf([]string{"tenant_id", "device_id", "metric_id"}, false)

		where := func(b *benchmark.Benchmark, workerId int) string {
			w := b.GenFakeDataAsMap(workerId, colConfs, false)

			return fmt.Sprintf("tenant_id = '%s' AND device_id = '%s' AND metric_id = '%s'", (*w)["tenant_id"], (*w)["device_id"], (*w)["metric_id"])
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id DESC"
		}

		testSelect(b, testDesc, nil, "id", where, orderby, 1)

		b.Vault.(*DBTestData).EffectiveBatch = origBatch
	},
}

/*
 * Advanced monitoring simulation tests
 */

// TestInsertAdvmTasks inserts into the 'adv monitoring tasks' table
var TestInsertAdvmTasks = TestDesc{
	name:        "insert-advmtasks",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring tasks' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmTasks,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestSelectAdvmTasksLast selects last inserted row from the 'adv monitoring tasks' table
var TestSelectAdvmTasksLast = TestDesc{
	name:        "select-advmtasks-last",
	metric:      "values/sec",
	description: "get number of rows grouped by week+result_code",
	category:    TestSelect,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmTasks,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		where := func(b *benchmark.Benchmark, workerId int) string {
			return "origin in (1, 2, 3)"
		}
		orderby := func(b *benchmark.Benchmark) string {
			return "id ASC"
		}
		testSelect(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSelectAdvmTasksCodePerWeek selects number of rows grouped by week+result_code
var TestSelectAdvmTasksCodePerWeek = TestDesc{
	name:        "select-advmtasks-codeperweek",
	metric:      "values/sec",
	description: "get number of rows grouped by week+result_code",
	category:    TestSelect,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmTasks,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		// need to implement it
		b.Exit("%s: is not implemented!\n", testDesc.name)
	},
}

// TestInsertAdvmResources inserts into the 'adv monitoring resources' table
var TestInsertAdvmResources = TestDesc{
	name:        "insert-advmresources",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring resources' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmResources,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmResourcesStatuses inserts into the 'adv monitoring resources statuses' table
var TestInsertAdvmResourcesStatuses = TestDesc{
	name:        "insert-advmresourcesstatuses",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring resources statuses' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmResourcesStatuses,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmAgentResources inserts into the 'adv monitoring agent resources' table
var TestInsertAdvmAgentResources = TestDesc{
	name:        "insert-advmagentresources",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring agent resources' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmAgentsResources,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmAgents inserts into the 'adv monitoring agents' table
var TestInsertAdvmAgents = TestDesc{
	name:        "insert-advmagents",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring agents' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmAgents,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmBackupResources inserts into the 'adv monitoring backup resources' table
var TestInsertAdvmBackupResources = TestDesc{
	name:        "insert-advmbackupresources",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring backup resources' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmBackupResources,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmBackups inserts into the 'adv monitoring backups' table
var TestInsertAdvmBackups = TestDesc{
	name:        "insert-advmbackups",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring backups' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmBackups,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmArchives inserts into the 'adv monitoring archives' table
var TestInsertAdvmArchives = TestDesc{
	name:        "insert-advmarchives",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring archives' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmArchives,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmVaults inserts into the 'adv monitoring vaults' table
var TestInsertAdvmVaults = TestDesc{
	name:        "insert-advmvaults",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring vaults' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmVaults,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmDevices inserts into the 'adv monitoring devices' table
var TestInsertAdvmDevices = TestDesc{
	name:        "insert-advmdevices",
	metric:      "rows/sec",
	description: "insert into the 'adv monitoring devices' table",
	category:    TestInsert,
	isReadonly:  false,
	isDBRTest:   false,
	databases:   []string{benchmark.POSTGRES},
	table:       TestTableAdvmDevices,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testInsertGeneric(b, testDesc)
	},
}

/*
 * Other
 */

// CreateTenantWorker creates a tenant and optionally inserts an event into the event bus
func CreateTenantWorker(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) { //nolint:revive
	c.Begin()

	for i := 0; i < batch; i++ {
		tenantUUID := b.TenantsCache.CreateTenant(b.Randomizer.GetWorker(c.WorkerID), c)

		if b.TestOpts.(*TestOpts).BenchOpts.Events {
			b.Vault.(*DBTestData).EventBus.InsertEvent(b.Randomizer.GetWorker(c.WorkerID), c, string(tenantUUID))
		}
	}
	c.Commit()

	return batch
}

func CreateCTIEntityWorker(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) { //nolint:revive
	c.Begin()

	for i := 0; i < batch; i++ {
		b.TenantsCache.CreateCTIEntity(b.Randomizer.GetWorker(c.WorkerID), c)
	}
	c.Commit()

	return batch
}

func tenantAwareCTIAwareWorker(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, orderBy string, batch int) (loops int) { //nolint:revive
	c.Log(benchmark.LogTrace, "tenant-aware and CTI-aware SELECT test iteration")

	tableName := testDesc.table.TableName
	query := buildTenantAwareQuery(tableName)
	ctiUUID, err := b.TenantsCache.GetRandomCTIUUID(b.Randomizer.GetWorker(c.WorkerID), 0)
	if err != nil {
		b.Exit(err.Error())
	}
	ctiAwareQuery := query + fmt.Sprintf(
		" JOIN `%[1]s` AS `cti_ent` "+
			"ON `cti_ent`.`uuid` = `%[2]s`.`cti_entity_uuid` AND `%[2]s`.`cti_entity_uuid` IN ('%[4]s') "+
			"LEFT JOIN `%[3]s` as `cti_prov` "+
			"ON `cti_prov`.`tenant_id` = `tenants_child`.`id` AND `cti_prov`.`cti_entity_uuid` = `%[2]s`.`cti_entity_uuid` "+
			"WHERE `cti_prov`.`state` = 1 OR `cti_ent`.`global_state` = 1",
		benchmark.TableNameCtiEntities, tableName, benchmark.TableNameCtiProvisioning, string(ctiUUID))

	return tenantAwareGenericWorker(b, c, ctiAwareQuery, orderBy)
}

func tenantAwareWorker(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, orderBy string, batch int) (loops int) { //nolint:revive
	query := buildTenantAwareQuery(testDesc.table.TableName)

	return tenantAwareGenericWorker(b, c, query, orderBy)
}

func buildTenantAwareQuery(tableName string) string {
	return fmt.Sprintf("SELECT `%[1]s`.`id` id, `%[1]s`.`tenant_id` FROM `%[1]s` "+
		"JOIN `%[2]s` AS `tenants_child` ON ((`tenants_child`.`uuid` = `%[1]s`.`tenant_id`) AND (`tenants_child`.`is_deleted` != {true})) "+
		"JOIN `%[3]s` AS `tenants_closure` ON ((`tenants_closure`.`child_id` = `tenants_child`.`id`) AND (`tenants_closure`.`barrier` <= 0)) "+
		"JOIN `%[2]s` AS `tenants_parent` ON ((`tenants_parent`.`id` = `tenants_closure`.`parent_id`) "+
		"AND (`tenants_parent`.`uuid` IN ('{tenant_uuid}')) AND (`tenants_parent`.`is_deleted` != {true}))",
		tableName, benchmark.TableNameTenants, benchmark.TableNameTenantClosure)
}

func tenantAwareGenericWorker(b *benchmark.Benchmark, c *benchmark.DBConnector, query string, orderBy string) (loops int) {
	c.Log(benchmark.LogTrace, "tenant-aware SELECT test iteration")

	uuid, err := b.TenantsCache.GetRandomTenantUUID(b.Randomizer.GetWorker(c.WorkerID), 0)
	if err != nil {
		b.Exit(err.Error())
	}

	var valTrue string

	if b.TestOpts.(*TestOpts).DBOpts.Driver == benchmark.POSTGRES {
		valTrue = "true"
	} else {
		valTrue = "1"
	}
	query = strings.ReplaceAll(query, "{true}", valTrue)
	query = strings.ReplaceAll(query, "{tenant_uuid}", string(uuid))
	if orderBy != "" {
		query += " " + orderBy
	}
	query += " LIMIT 1"

	var id, tenantID string

	if b.TestOpts.(*TestOpts).DBOpts.Driver == benchmark.POSTGRES {
		query = strings.ReplaceAll(query, "`", "\"")
	}

	c.Log(benchmark.LogTrace, "executing query: %s", query)
	c.QueryRowAndScanAllowEmpty(query, &id, &tenantID)

	return 1
}

// TestSelectMediumLastTenant is the same as TestSelectMediumLast but with tenant-awareness
var TestSelectMediumLastTenant = TestDesc{
	name:        "select-medium-last-in-tenant",
	metric:      "rows/sec",
	description: "select the last row from the 'medium' table WHERE tenant_id = {random tenant uuid}",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   ALL,
	table:       TestTableMedium,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		worker := func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
			return tenantAwareWorker(b, c, testDesc, "ORDER BY enqueue_time_ns DESC", 1)
		}
		testGeneric(b, testDesc, worker, 1)
	},
}

// TestSelectBlobLastTenant is the same as TestSelectBlobLast but with tenant-awareness
var TestSelectBlobLastTenant = TestDesc{
	name:        "select-blob-last-in-tenant",
	metric:      "rows/sec",
	description: "select the last row from the 'blob' table WHERE tenant_id = {random tenant uuid}",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   ALL,
	table:       TestTableBlob,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		worker := func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
			return tenantAwareWorker(b, c, testDesc, "ORDER BY timestamp DESC", 1)
		}
		testGeneric(b, testDesc, worker, 1)
	},
}

// TestSelectHeavyLastTenant is the same as TestSelectHeavyLast but with tenant-awareness
var TestSelectHeavyLastTenant = TestDesc{
	name:        "select-heavy-last-in-tenant",
	metric:      "rows/sec",
	description: "select the last row from the 'heavy' table WHERE tenant_id = {random tenant uuid}",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		worker := func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
			return tenantAwareWorker(b, c, testDesc, "ORDER BY enqueue_time_ns DESC", 1)
		}
		testGeneric(b, testDesc, worker, 1)
	},
}

// TestSelectHeavyLastTenantCTI is the same as TestSelectHeavyLastTenant but with CTI-awareness
var TestSelectHeavyLastTenantCTI = TestDesc{
	name:        "select-heavy-last-in-tenant-and-cti",
	metric:      "rows/sec",
	description: "select the last row from the 'heavy' table WHERE tenant_id = {} AND cti = {}",
	category:    TestSelect,
	isReadonly:  true,
	isDBRTest:   false,
	databases:   RELATIONAL,
	table:       TestTableHeavy,
	launcherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		worker := func(b *benchmark.Benchmark, c *benchmark.DBConnector, testDesc *TestDesc, batch int) (loops int) {
			return tenantAwareCTIAwareWorker(b, c, testDesc, "ORDER BY enqueue_time_ns DESC", 1)
		}
		testGeneric(b, testDesc, worker, 1)
	},
}

// GetTests returns all tests in the package for execution
func GetTests() ([]*TestGroup, map[string]*TestDesc) {
	allTests = NewTestGroup("all tests")
	var g []*TestGroup

	tg := NewTestGroup("Base tests group")
	g = append(g, tg)

	TestBaseAll.launcherFunc = func(b *benchmark.Benchmark, testDesc *TestDesc) {
		testOpts, ok := b.TestOpts.(*TestOpts)
		if !ok {
			b.Exit("internal error: can't cast TestOpts struct")
		}
		executeAllTests(b, testOpts)
	}

	tg.add(&TestInsertTenant)
	tg.add(&TestInsertCTI)
	tg.add(&TestInsertLight)
	tg.add(&TestInsertLightPrepared)
	tg.add(&TestInsertLightMultiValue)
	tg.add(&TestCopyLight)
	tg.add(&TestInsertMedium)
	tg.add(&TestInsertMediumPrepared)
	tg.add(&TestInsertMediumMultiValue)
	tg.add(&TestCopyMedium)
	tg.add(&TestInsertHeavy)
	tg.add(&TestInsertHeavyPrepared)
	tg.add(&TestInsertHeavyMultivalue)
	tg.add(&TestCopyHeavy)
	tg.add(&TestUpdateMedium)
	tg.add(&TestUpdateHeavy)
	tg.add(&TestSelectOne)
	tg.add(&TestSelectMediumLast)
	tg.add(&TestSelectMediumRand)
	tg.add(&TestSelectHeavyLast)
	tg.add(&TestSelectHeavyRand)
	tg.add(&TestSelectHeavyMinMaxTenant)
	tg.add(&TestSelectHeavyMinMaxTenantAndState)
	tg.add(&TestBaseAll)

	tg = NewTestGroup("Advanced tests group")
	g = append(g, tg)

	tg.add(&TestSelectNextVal)
	tg.add(&TestPing)
	tg.add(&TestSelectHeavyForUpdateSkipLocked)
	tg.add(&TestInsertJSON)
	tg.add(&TestSelectJSONByIndexedValue)
	tg.add(&TestSearchJSONByIndexedValue)
	tg.add(&TestSelectJSONByNonIndexedValue)
	tg.add(&TestSearchJSONByNonIndexedValue)
	tg.add(&TestUpdateHeavySameVal)
	tg.add(&TestUpdateHeavyPartialSameVal)
	tg.add(&TestUpdateHeavyBulk)
	tg.add(&TestUpdateHeavyBulkDBR)

	tg = NewTestGroup("Tenant-aware tests")
	g = append(g, tg)

	tg.add(&TestSelectMediumLastTenant)
	tg.add(&TestSelectHeavyLastTenant)
	tg.add(&TestSelectHeavyLastTenantCTI)
	tg.add(&TestSelectHeavyRandTenantLike)

	tg = NewTestGroup("Blob tests")
	g = append(g, tg)

	tg.add(&TestInsertBlob)
	tg.add(&TestCopyBlob)
	tg.add(&TestInsertLargeObj)
	tg.add(&TestSelectBlobLastTenant)

	tg = NewTestGroup("Timeseries tests")
	g = append(g, tg)

	tg.add(&TestInsertTimeSeriesSQL)
	tg.add(&TestSelectTimeSeriesSQL)

	tg = NewTestGroup("Golang DBR query builder tests")
	g = append(g, tg)

	tg.add(&TestInsertLightDBR)
	tg.add(&TestInsertMediumDBR)
	tg.add(&TestInsertHeavyDBR)
	tg.add(&TestInsertJSONDBR)
	tg.add(&TestUpdateMediumDBR)
	tg.add(&TestUpdateHeavyDBR)
	tg.add(&TestSelectOneDBR)
	tg.add(&TestSelectMediumLastDBR)
	tg.add(&TestSelectMediumRandDBR)
	tg.add(&TestSelectHeavyLastDBR)
	tg.add(&TestSelectHeavyRandDBR)

	tg = NewTestGroup("Advanced monitoring tests")
	g = append(g, tg)

	tg.add(&TestInsertAdvmTasks)
	tg.add(&TestSelectAdvmTasksLast)
	tg.add(&TestSelectAdvmTasksCodePerWeek)
	tg.add(&TestInsertAdvmResources)
	tg.add(&TestInsertAdvmResourcesStatuses)
	tg.add(&TestInsertAdvmAgentResources)
	tg.add(&TestInsertAdvmAgents)
	tg.add(&TestInsertAdvmBackupResources)
	tg.add(&TestInsertAdvmBackups)
	tg.add(&TestInsertAdvmArchives)
	tg.add(&TestInsertAdvmVaults)
	tg.add(&TestInsertAdvmDevices)

	ret := make(map[string]*TestDesc)

	for _, t := range allTests.tests {
		ret[t.name] = t
	}

	return g, ret
}

func executeAllTests(b *benchmark.Benchmark, testOpts *TestOpts) {
	if testOpts.BenchOpts.Chunk > testOpts.BenchOpts.Limit {
		b.Exit("--chunk option must not be less then --limit")
	}

	if testOpts.BenchOpts.Chunk < MinChunk {
		b.Exit("--chunk option must not be less then %d", MinChunk)
	}

	cleanupTables(b)
	createTables(b)

	workers := b.CommonOpts.Workers
	if workers <= 1 {
		workers = 16
	}

	for i := 0; i < testOpts.BenchOpts.Limit; i += testOpts.BenchOpts.Chunk {
		executeAllTestsOnce(b, testOpts, workers)
	}

	testData := b.Vault.(*DBTestData)

	fmt.Printf("--------------------------------------------------------------------\n")

	scores := []string{TestSelect, TestInsert, TestUpdate}
	for _, s := range scores {
		fmt.Printf("%s geomean: %.0f\n", s, b.Geomean(testData.scores[s]))
	}

	cleanupTables(b)
}

func executeOneTest(b *benchmark.Benchmark, testDesc *TestDesc) {
	testDesc.launcherFunc(b, testDesc)
}

func executeAllTestsOnce(b *benchmark.Benchmark, testOpts *TestOpts, workers int) {
	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectOne)

	/* Insert */

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 10000
	executeOneTest(b, &TestInsertTenant)

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 1000
	executeOneTest(b, &TestInsertCTI)

	//	b.CommonOpts.Duration = 0
	//	b.CommonOpts.Workers = workers
	//	b.CommonOpts.Loops = 30000
	//	executeOneTest(b, &TestInsertTenant)

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 5
	executeOneTest(b, &TestInsertLight)
	executeOneTest(b, &TestInsertMedium)
	executeOneTest(b, &TestInsertHeavy)
	executeOneTest(b, &TestInsertJSON)
	executeOneTest(b, &TestInsertTimeSeriesSQL)

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 95
	executeOneTest(b, &TestInsertLight)
	executeOneTest(b, &TestInsertMedium)
	executeOneTest(b, &TestInsertJSON)
	executeOneTest(b, &TestInsertTimeSeriesSQL)

	/* Update */

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 2
	executeOneTest(b, &TestUpdateMedium)
	executeOneTest(b, &TestUpdateHeavy)
	executeOneTest(b, &TestUpdateHeavyPartialSameVal)
	executeOneTest(b, &TestUpdateHeavySameVal)

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 28
	executeOneTest(b, &TestUpdateMedium)
	executeOneTest(b, &TestUpdateHeavy)
	executeOneTest(b, &TestUpdateHeavyPartialSameVal)
	executeOneTest(b, &TestUpdateHeavySameVal)

	/* Select */

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectMediumRand)
	executeOneTest(b, &TestSelectHeavyRand)

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectMediumRand)
	executeOneTest(b, &TestSelectHeavyRand)

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectMediumLast)
	executeOneTest(b, &TestSelectHeavyLast)

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectMediumLast)
	executeOneTest(b, &TestSelectHeavyLast)

	/* Other select's */

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectHeavyLastTenant)
	executeOneTest(b, &TestSelectHeavyRandTenantLike)
	executeOneTest(b, &TestSelectHeavyLastTenantCTI)
	executeOneTest(b, &TestSelectJSONByIndexedValue)
	executeOneTest(b, &TestSelectJSONByNonIndexedValue)
	executeOneTest(b, &TestSelectTimeSeriesSQL)
	executeOneTest(b, &TestSelectHeavyMinMaxTenant)
	executeOneTest(b, &TestSelectHeavyMinMaxTenantAndState)

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectHeavyLastTenant)
	executeOneTest(b, &TestSelectHeavyRandTenantLike)
	executeOneTest(b, &TestSelectHeavyLastTenantCTI)
	executeOneTest(b, &TestSelectJSONByIndexedValue)
	executeOneTest(b, &TestSelectJSONByNonIndexedValue)
	executeOneTest(b, &TestSelectTimeSeriesSQL)
	executeOneTest(b, &TestSelectHeavyMinMaxTenant)
	executeOneTest(b, &TestSelectHeavyMinMaxTenantAndState)
}
