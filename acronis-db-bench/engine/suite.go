package engine

import (
	"fmt"
	"sync"

	"github.com/acronis/perfkit/benchmark"
)

// PerfSuiteRegistry is an interface for registering test scenarios
type PerfSuiteRegistry interface {
	// RegisterPerfSuite registers a new test perf suite
	RegisterPerfSuite(name string, suite *PerfSuite) error
	// GetPerfSuite returns registered perf suite with the given name
	GetPerfSuite(name string) *PerfSuite
}

// perfSuiteRegistry implements PerfSuiteRegistry interface
type perfSuiteRegistry struct {
	suites map[string]*PerfSuite
	mu     sync.Mutex
}

// NewPerfSuiteRegistry creates a new perf suite registry
func NewPerfSuiteRegistry() PerfSuiteRegistry {
	return &perfSuiteRegistry{
		suites: make(map[string]*PerfSuite),
	}
}

// RegisterPerfSuite registers a new perf suite
func (r *perfSuiteRegistry) RegisterPerfSuite(name string, suite *PerfSuite) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.suites[name]; exists {
		return fmt.Errorf("perf suite %s already exists", name)
	}

	r.suites[name] = suite

	return nil
}

// GetPerfSuite returns all registered perf suites
func (r *perfSuiteRegistry) GetPerfSuite(name string) *PerfSuite {
	r.mu.Lock()
	defer r.mu.Unlock()

	if suite, exists := r.suites[name]; exists {
		return suite
	}

	return nil
}

var (
	// suitesRegistry is the global test registry
	suitesRegistry PerfSuiteRegistry = NewPerfSuiteRegistry()
)

// RegisterPerfSuite registers a new perf suite globally
func RegisterPerfSuite(name string, suite *PerfSuite) error {
	return suitesRegistry.RegisterPerfSuite(name, suite)
}

type SuiteStep interface {
	Execute(b *benchmark.Benchmark, testOpts *TestOpts, workers int) error
}

type suiteStepTestExecute struct {
	TestName string
}

func (s suiteStepTestExecute) Execute(b *benchmark.Benchmark, testOpts *TestOpts, workers int) error {
	var testDesc *TestDesc
	if testDesc = testRegistry.GetTestByName(s.TestName); testDesc == nil {
		return fmt.Errorf("test %s not found", s.TestName)
	}

	// Get current dialect
	var dialectName = getDBDriver(b)

	// Skip if current dialect is not supported by this test
	dialectSupported := false
	for _, supportedDialect := range testDesc.Databases {
		if dialectName == supportedDialect {
			dialectSupported = true
			break
		}
	}

	if !dialectSupported {
		// b.Log(benchmark.LogInfo, "Skipping test '%s' - not supported for dialect '%s'", testDesc.name, dialectName)
		return nil
	}

	testDesc.LauncherFunc(b, testDesc)

	// b.Log(benchmark.LogInfo, "Test '%s' completed", testDesc.name)
	select {
	case <-b.ShutdownCh:
		b.Logger.Debug("Gracefully stopping test execution...")
		b.Exit()
	default:
		if b.NeedToExit {
			b.Exit()
		}
	}

	return nil
}

type suiteStepParameterSetter struct {
	ParameterSetter func(b *benchmark.Benchmark, testOpts *TestOpts, workers int)
}

func (s suiteStepParameterSetter) Execute(b *benchmark.Benchmark, testOpts *TestOpts, workers int) error {
	if s.ParameterSetter != nil {
		s.ParameterSetter(b, testOpts, workers)
	}

	return nil
}

// PerfSuite provides common implementation for test suites
type PerfSuite struct {
	Pipeline []SuiteStep
}

// NewPerfSuite creates a new base suite instance
func NewPerfSuite() *PerfSuite {
	return &PerfSuite{
		Pipeline: make([]SuiteStep, 0),
	}
}

// SetParameters sets the parameter setter function and returns the suite for method chaining
func (s *PerfSuite) SetParameters(setter func(b *benchmark.Benchmark, testOpts *TestOpts, workers int)) *PerfSuite {
	s.Pipeline = append(s.Pipeline, suiteStepParameterSetter{ParameterSetter: setter})
	return s
}

// ScheduleTest adds a test to the suite's test list and returns the suite for method chaining
func (s *PerfSuite) ScheduleTest(testName string) *PerfSuite {
	s.Pipeline = append(s.Pipeline, suiteStepTestExecute{TestName: testName})
	return s
}

func (s *PerfSuite) Execute(b *benchmark.Benchmark, testOpts *TestOpts, workers int) {
	for _, step := range s.Pipeline {
		if err := step.Execute(b, testOpts, workers); err != nil {
			return
		}
	}

	return
}
