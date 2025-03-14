package engine

import (
	"fmt"
	"sync"
)

// TestScenario represents a complete test scenario with its test group and associated tables
type TestScenario struct {
	// Name is the name of the test scenario
	Name string
	// Tests is the list of tests in this scenario
	Tests []*TestDesc
	// Tables is the map of tables used in this scenario
	Tables map[string]TestTable
}

// TestGroupRegistry is an interface for registering test scenarios
type TestGroupRegistry interface {
	// RegisterTestScenario registers a new test scenario
	RegisterTestScenario(scenario *TestScenario) error
	// GetTestGroups returns all registered test groups
	GetTestGroups() []*TestGroup
	// GetTestByName returns a test by its name
	GetTestByName(name string) *TestDesc
	// GetTableByName returns a table by its name
	GetTableByName(name string) TestTable
	// GetTables returns all registered tables
	GetTables() map[string]TestTable
}

// testScenarioRegistry implements TestGroupRegistry interface
type testScenarioRegistry struct {
	groups map[string]*TestGroup
	tests  map[string]*TestDesc
	tables map[string]TestTable
	mu     sync.Mutex
}

// NewTestGroupRegistry creates a new test group registry
func NewTestGroupRegistry() TestGroupRegistry {
	return &testScenarioRegistry{
		groups: make(map[string]*TestGroup),
		tests:  make(map[string]*TestDesc),
		tables: make(map[string]TestTable),
	}
}

// RegisterTestScenario registers a new test scenario
func (r *testScenarioRegistry) RegisterTestScenario(scenario *TestScenario) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.groups[scenario.Name]; exists {
		return fmt.Errorf("test scenario %s already exists", scenario.Name)
	}

	// Register tables first
	for name, table := range scenario.Tables {
		if _, exists := r.tables[name]; exists {
			return fmt.Errorf("table %s already exists", name)
		}
		r.tables[name] = table
	}

	// Register tests
	group := NewTestGroup(scenario.Name)
	for _, test := range scenario.Tests {
		if _, exists := r.tests[test.Name]; exists {
			return fmt.Errorf("test %s already exists", test.Name)
		}
		group.add(test)
		r.tests[test.Name] = test
	}
	r.groups[scenario.Name] = group

	return nil
}

// GetTestGroups returns all registered test groups
func (r *testScenarioRegistry) GetTestGroups() []*TestGroup {
	r.mu.Lock()
	defer r.mu.Unlock()

	var groups []*TestGroup
	for _, group := range r.groups {
		groups = append(groups, group)
	}
	return groups
}

// GetTestByName returns a test by its name
func (r *testScenarioRegistry) GetTestByName(name string) *TestDesc {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tests[name]
}

// GetTableByName returns a table by its name
func (r *testScenarioRegistry) GetTableByName(name string) TestTable {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tables[name]
}

// GetTables returns all registered tables
func (r *testScenarioRegistry) GetTables() map[string]TestTable {
	r.mu.Lock()
	defer r.mu.Unlock()

	tables := make(map[string]TestTable)
	for name, table := range r.tables {
		tables[name] = table
	}
	return tables
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

// GetTables returns all registered tables
func GetTables() map[string]TestTable {
	return testRegistry.GetTables()
}

// RegisterTestScenario registers a new test scenario globally
func RegisterTestScenario(scenario *TestScenario) error {
	return testRegistry.RegisterTestScenario(scenario)
}
