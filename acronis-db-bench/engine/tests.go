package engine

import (
	"context"
	"fmt"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
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
	g.tests[t.Name] = t
	_, exists := allTests.tests[t.Name]
	if exists {
		FatalError("Internal error: test %s already defined")
	}
	allTests.tests[t.Name] = t
}

// TestCategories is a list of all test categories
var TestCategories = []string{TestSelect, TestUpdate, TestInsert, TestDelete, TestTransaction}

type TestWorkerFunc func(b *benchmark.Benchmark, c *DBConnector, testDesc *TestDesc, batch int) (loops int)
type orderByFunc func(b *benchmark.Benchmark) string //nolint:unused
type launcherFunc func(b *benchmark.Benchmark, testDesc *TestDesc)

// TestDesc describes a test
type TestDesc struct {
	Name        string
	Metric      string
	Description string
	Category    string
	IsReadonly  bool // indicates the test doesn't run DDL and doesn't modidy data
	IsDBRTest   bool
	Databases   []db.DialectName

	Table TestTable // SQL table name

	LauncherFunc launcherFunc
}

// dbIsSupported returns true if the database is supported by the test
func (t *TestDesc) dbIsSupported(db db.DialectName) bool {
	for _, b := range t.Databases {
		if b == db {
			return true
		}
	}

	return false
}

// getDBs returns a string with supported databases
func (t *TestDesc) getDBs() string {
	ret := "["

	for _, database := range db.GetDatabases() {
		if t.dbIsSupported(database.Driver) {
			ret += database.Symbol
		} else {
			ret += "-"
		}
	}
	ret += "]"

	return ret
}

var (
	// ALL is a list of all supported databases
	ALL = []db.DialectName{db.POSTGRES, db.MYSQL, db.MSSQL, db.SQLITE, db.CLICKHOUSE, db.CASSANDRA, db.ELASTICSEARCH, db.OPENSEARCH}
	// RELATIONAL is a list of all supported relational databases
	RELATIONAL = []db.DialectName{db.POSTGRES, db.MYSQL, db.MSSQL, db.SQLITE}
	// PMWSA is a list of all supported databases except ClickHouse
	PMWSA = []db.DialectName{db.POSTGRES, db.MYSQL, db.MSSQL, db.SQLITE, db.CASSANDRA}
)

// TestBaseAll tests all tests in the 'base' group
var TestBaseAll = TestDesc{
	Name:        "all",
	Description: "execute all tests in the 'base' group",
	Databases:   ALL,
	//	launcherFunc: ...  # causes 'initialization cycle' go-lang compiler error
}

// InsertMultiValueDataWorker inserts a row into the 'light' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...)
func InsertMultiValueDataWorker(b *benchmark.Benchmark, c *DBConnector, testDesc *TestDesc, batch int) (loops int) {
	colConfs := testDesc.Table.GetColumnsForInsert(db.WithAutoInc(c.Database.DialectName()))
	workerID := c.WorkerID

	var columns []string
	var values [][]interface{}
	for i := 0; i < batch; i++ {
		var genColumns, vals = b.GenFakeData(workerID, colConfs, db.WithAutoInc(c.Database.DialectName()))
		values = append(values, vals)
		if i == 0 {
			columns = genColumns
		}
	}

	var session = c.Database.Session(c.Database.Context(context.Background(), false))
	if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
		return tx.BulkInsert(testDesc.Table.TableName, values, columns)
	}); txErr != nil {
		b.Exit(txErr.Error())
	}

	return batch
}

// GetTests returns all tests in the package for execution
func GetTests() ([]*TestGroup, map[string]*TestDesc) {
	allTests = NewTestGroup("all tests")
	var g []*TestGroup

	tg := NewTestGroup("Base tests group")
	g = append(g, tg)

	TestBaseAll.LauncherFunc = func(b *benchmark.Benchmark, testDesc *TestDesc) { //nolint:revive
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
	tg.add(&TestSelectHeavyRandPageByUUID)

	tg.add(&TestSelectHeavyRandCustomerRecent)
	tg.add(&TestSelectHeavyRandCustomerRecentLike)
	tg.add(&TestSelectHeavyRandCustomerUpdateTimePage)
	tg.add(&TestSelectHeavyRandCustomerCount)

	tg.add(&TestSelectHeavyRandPartnerRecent)
	tg.add(&TestSelectHeavyRandPartnerStartUpdateTimePage)

	tg.add(&TestBaseAll)

	tg = NewTestGroup("Vector tests group")
	g = append(g, tg)

	tg.add(&TestInsertVector768MultiValue)
	tg.add(&TestSelectVector768NearestL2)
	tg.add(&TestInsertEmailSecurityMultiValue)
	tg.add(&TestSelectEmailByEmbeddingNearestL2)

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
		ret[t.Name] = t
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
