package engine

import (
	"fmt"
	"sync"

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
	Name  string
	tests map[string]*TestDesc
}

// NewTestGroup creates a new test group
func NewTestGroup(name string) *TestGroup {
	return &TestGroup{Name: name, tests: make(map[string]*TestDesc)}
}

func (g *TestGroup) Add(t *TestDesc) {
	g.tests[t.Name] = t
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

// TestGroupRegistry is an interface for registering test groups
type TestGroupRegistry interface {
	// RegisterTestGroup registers a new test groups
	RegisterTestGroup(testGroup *TestGroup) error
	// GetTestGroups returns all registered test groups
	GetTestGroups() []*TestGroup
	// GetTestByName returns all registered test groups
	GetTestByName(testName string) *TestDesc
	// GetTables returns all registered tables
	GetTables() map[string]TestTable
	// GetTableByName returns a table by its name
	GetTableByName(tableName string) TestTable
}

// testGroupRegistry implements TestGroupRegistry interface
type testGroupRegistry struct {
	groups map[string]*TestGroup
	tests  map[string]*TestDesc
	tables map[string]TestTable
	mu     sync.Mutex
}

// NewTestGroupRegistry creates a new test group registry
func NewTestGroupRegistry() TestGroupRegistry {
	return &testGroupRegistry{
		groups: make(map[string]*TestGroup),
		tests:  make(map[string]*TestDesc),
		tables: make(map[string]TestTable),
	}
}

// RegisterTestGroup registers a new test group
func (r *testGroupRegistry) RegisterTestGroup(group *TestGroup) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.groups[group.Name]; exists {
		return fmt.Errorf("test group %s already exists", group.Name)
	}

	var tables = make(map[string]TestTable)
	for _, test := range group.tests {
		if _, exists := r.tests[test.Name]; exists {
			return fmt.Errorf("test %s already exists", test.Name)
		}

		r.tests[test.Name] = test

		if test.Table.TableName != "" {
			tables[test.Table.TableName] = test.Table
		}
	}

	for name, table := range tables {
		if _, exists := r.tables[name]; exists {
			return fmt.Errorf("table %s already exists", name)
		}
		r.tables[name] = table
	}

	r.groups[group.Name] = group

	return nil
}

// GetTestGroups returns all registered test groups
func (r *testGroupRegistry) GetTestGroups() []*TestGroup {
	r.mu.Lock()
	defer r.mu.Unlock()

	var groups []*TestGroup
	for _, group := range r.groups {
		groups = append(groups, group)
	}

	return groups
}

// GetTestByName returns a test by its name
func (r *testGroupRegistry) GetTestByName(testName string) *TestDesc {
	r.mu.Lock()
	defer r.mu.Unlock()

	if test, exists := r.tests[testName]; exists {
		return test
	}
	return nil
}

// GetTables returns all registered tables
func (r *testGroupRegistry) GetTables() map[string]TestTable {
	r.mu.Lock()
	defer r.mu.Unlock()

	tables := make(map[string]TestTable)
	for name, table := range r.tables {
		tables[name] = table
	}
	return tables
}

// GetTableByName returns a table by its name
func (r *testGroupRegistry) GetTableByName(tableName string) TestTable {
	r.mu.Lock()
	defer r.mu.Unlock()

	if table, exists := r.tables[tableName]; exists {
		return table
	}
	return TestTable{}
}

var (
	// testRegistry is the global test registry
	testRegistry TestGroupRegistry = NewTestGroupRegistry()
)

// GetTests returns all registered test groups and tests
func GetTests() ([]*TestGroup, map[string]*TestDesc) {
	groups := testRegistry.GetTestGroups()
	tests := make(map[string]*TestDesc)

	for _, group := range groups {
		for name, test := range group.tests {
			tests[name] = test
		}
	}

	return groups, tests
}

// RegisterTestGroup registers a new test group globally
func RegisterTestGroup(testGroup *TestGroup) error {
	return testRegistry.RegisterTestGroup(testGroup)
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

	var allBasicSuite = suitesRegistry.GetPerfSuite("all")

	for i := 0; i < testOpts.BenchOpts.Limit; i += testOpts.BenchOpts.Chunk {
		allBasicSuite.Execute(b, testOpts, workers)
	}

	testData := b.Vault.(*DBTestData)

	fmt.Printf("--------------------------------------------------------------------\n")

	scores := []string{TestSelect, TestInsert, TestUpdate}
	for _, s := range scores {
		fmt.Printf("%s geomean: %.0f\n", s, b.Geomean(testData.scores[s]))
	}

	cleanupTables(b)
}
