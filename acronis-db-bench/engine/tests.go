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

	var columns []string
	var values [][]interface{}
	for i := 0; i < batch; i++ {
		var genColumns, vals, err = b.Randomizer.GenFakeData(colConfs, db.WithAutoInc(c.Database.DialectName()))
		if err != nil {
			b.Exit(err)
		}
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
