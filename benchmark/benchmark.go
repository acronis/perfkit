package benchmark

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/acronis/perfkit/logger"
)

// TestOpts represents all user specified flags
type TestOpts interface{}

// WorkerData represents test specific data
type WorkerData interface{}

// AnyData represents any data
type AnyData interface{}

// Score represents test score
type Score struct {
	Workers int
	Seconds float64
	Loops   uint64
	Rate    float64
	Metric  string
}

// FormatRate formats rate to 4 significant figures
func (s *Score) FormatRate(n int) string { //nolint:revive
	if s.Rate == 0.0 {
		return "0"
	}

	// Calculate magnitude of the number
	order := math.Floor(math.Log10(math.Abs(s.Rate))) + 1

	// Determine the precision needed for 4 significant figures
	precision := 4 - int(order)

	if precision < 0 {
		precision = 0
	}

	format := fmt.Sprintf("%%.%df", precision)

	return fmt.Sprintf(format, s.Rate)
}

// Benchmark is used for running tests
// Init is called once before InitPerWorker and should initialize program constants, global variables, etc.
// InitPerWorker is called Benchmark.CommonOpts.Workers times and should initialize data structs required for running Worker method
// Worker runs user logic and should use opts.WorkerData[id] and opts.Vault
// FinishPerWorker is called Benchmark.CommonOpts.Workers times and should deinit all WorkerData structs
// Finish is called once after FinishPerWorker and should call some logic(e.g. analyze data) and deinit used data structs
type Benchmark struct {
	AddOpts          func() TestOpts
	Init             func()
	WorkerInitFunc   func(worker *BenchmarkWorker)
	WorkerPreRunFunc func(worker *BenchmarkWorker)
	WorkerRunFunc    func(worker *BenchmarkWorker) (loops int)
	WorkerFinishFunc func(worker *BenchmarkWorker)
	Finish           func()
	PreExit          func()
	Metric           func() (metric string)
	GetRate          func(loops uint64, seconds float64) float64
	PrintScore       func(score Score)
	CommonOpts       CommonOpts
	Cli              CLI
	TestOpts         TestOpts
	OptsInitialized  bool
	ReadOnly         bool
	Logger           logger.Logger

	NeedToExit bool
	Score      Score

	CliArgs []string
	Vault   AnyData

	Randomizer *Randomizer

	Workers []*BenchmarkWorker

	ShutdownCh chan struct{}
	signalDone chan struct{}
}

func (b *Benchmark) Log(level logger.LogLevel, workerID int, format string, args ...interface{}) {
	if workerID == -1 {
		format = fmt.Sprintf("main worker: %s", format)
	} else {
		format = fmt.Sprintf("worker #%03d: %s", workerID, format)
	}

	b.Logger.Log(level, format, args...)
}

// NewBenchmark creates a new Benchmark instance with default values
func NewBenchmark() *Benchmark {
	b := Benchmark{
		AddOpts: func() TestOpts {
			var testOpts TestOpts

			return &testOpts
		},
		Init: func() {
		},
		WorkerInitFunc: func(worker *BenchmarkWorker) {
		},
		WorkerPreRunFunc: func(worker *BenchmarkWorker) {
		},
		WorkerRunFunc: func(worker *BenchmarkWorker) (loops int) {
			return 0
		},
		PreExit: func() {
		},
		WorkerFinishFunc: func(worker *BenchmarkWorker) {
		},
		Finish: func() {
		},
		Metric: func() (metric string) {
			return "loops/sec"
		},
		GetRate: func(loops uint64, seconds float64) float64 {
			return float64(loops) / seconds
		},
		PrintScore: func(score Score) {
			fmt.Printf("time: %f sec; threads: %d; loops: %d; rate: %.2f %s;\n", score.Seconds, score.Workers, score.Loops, score.Rate, score.Metric)
		},
		OptsInitialized: false,
		ShutdownCh:      make(chan struct{}),
		signalDone:      make(chan struct{}),
	}

	b.Logger = logger.NewPlaneLogger(logger.LevelWarn, false)
	b.Cli.Init(os.Args[0], &b.CommonOpts)

	return &b
}

// InitOpts initializes CLI options and logger
// InitOpts initializes CLI options and logger
func (b *Benchmark) InitOpts() {
	if b.OptsInitialized {
		return
	}
	b.TestOpts = b.AddOpts()
	args := b.Cli.Parse()
	b.CliArgs = args
	b.OptsInitialized = true

	if b.CommonOpts.Quiet {
		b.Logger = logger.NewPlaneLogger(logger.LevelError, false)
	} else {
		b.Logger = logger.NewPlaneLogger(logger.LogLevel(len(b.CommonOpts.Verbose))+logger.LevelWarn, false)
	}
	b.adjustFilenoUlimit()
}

// SetUsage sets usage information
func (b *Benchmark) SetUsage(usage string) {
	b.Cli.SetUsage(usage)
}

// RunOnce runs the test once and prints the score
func (b *Benchmark) RunOnce(printScore bool) {

	if b.CommonOpts.Loops != 0 {
		l := b.CommonOpts.Loops / b.CommonOpts.Workers
		rest := b.CommonOpts.Loops % b.CommonOpts.Workers
		for i := 0; i < b.CommonOpts.Workers; i++ {
			b.Workers[i].PlannedLoops = l
			if i < rest {
				b.Workers[i].PlannedLoops++
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(b.CommonOpts.Workers)

	if len(b.Workers) == 0 {
		b.Exit("internal error: Workers not initialized")
	}

	startTime := time.Now().UnixNano()
	for i := 0; i < b.CommonOpts.Workers; i++ {
		if b.Workers[i] == nil {
			b.Exit("internal error: Worker %d not initialized", i)
		}
		go runner(b.Workers[i], &wg)
	}
	wg.Wait()

	endTime := time.Now().UnixNano()

	var totalLoops uint64
	for _, worker := range b.Workers {
		totalLoops += uint64(worker.ExecutedLoops)
	}

	if totalLoops == 0 {
		return
	}

	b.Score.Seconds = float64(endTime-startTime) / float64(time.Second)
	b.Score.Rate = b.GetRate(totalLoops, b.Score.Seconds)
	b.Score.Metric = b.Metric()
	b.Score.Workers = b.CommonOpts.Workers
	b.Score.Loops = totalLoops

	if printScore {
		b.PrintScore(b.Score)
	}
}

// Run runs the test and prints the score (if repeat is 1) or the average, min and max scores (if repeat is > 1)
func (b *Benchmark) Run() {
	b.InitOpts()

	if b.CommonOpts.Workers < 0 {
		b.CommonOpts.Workers = 1
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure we cancel the context when Run() exits

	// Start signal handler with context
	go func() {
		select {
		case sig := <-sigChan:
			b.Logger.Info("Received signal %v, initiating graceful shutdown...", sig)
			b.Shutdown()
		case <-ctx.Done():
			// Clean up signal handling
			signal.Stop(sigChan)
			close(sigChan)
			return
		}
	}()

	b.Randomizer = NewRandomizer(b.CommonOpts.RandSeed, b.CommonOpts.Workers)
	b.Workers = make([]*BenchmarkWorker, b.CommonOpts.Workers)

	for i := 0; i < b.CommonOpts.Workers; i++ {
		b.Workers[i] = NewBenchmarkWorker(b, i)
	}

	b.Init()

	b.Logger.Debug("per-worker initialization")
	for i := 0; i < b.CommonOpts.Workers; i++ {
		b.WorkerInitFunc(b.Workers[i])
		if b.NeedToExit {
			break
		}
	}

	var minRate, maxRate, sumRate float64
	minRate = -1
	maxRate = -1
	sumRate = 0

	for r := 0; r < b.CommonOpts.Repeat; r++ {
		b.RunOnce(r != b.CommonOpts.Repeat-1)
		if minRate == -1 || minRate > b.Score.Rate {
			minRate = b.Score.Rate
		}
		if maxRate == -1 || maxRate < b.Score.Rate {
			maxRate = b.Score.Rate
		}
		sumRate += b.Score.Rate
		if b.NeedToExit {
			break
		}
	}

	b.Logger.Debug("per-worker termination")

	for i := 0; i < b.CommonOpts.Workers; i++ {
		b.WorkerFinishFunc(b.Workers[i])
	}

	b.Finish()

	b.PrintScore(b.Score)

	if b.CommonOpts.Repeat > 1 {
		fmt.Printf("Avg rate: %8.1f; Min rate: %8.1f; Max rate: %8.1f\n", sumRate/float64(b.CommonOpts.Repeat), minRate, maxRate)
	}

	// The deferred cancel() will clean up the signal handling goroutine
}

// runner is a helper function for running tests in parallel
func runner(w *BenchmarkWorker, wg *sync.WaitGroup) {
	var doneLoops = 0

	defer func() {
		w.ExecutedLoops = doneLoops
		wg.Done()
	}()

	var l int
	var startTime = time.Now().UnixNano()
	for {
		select {
		case <-w.Benchmark.ShutdownCh:
			w.Logger.Info("Worker %d shutting down...", w.WorkerID)
			return
		default:
			if w.Benchmark.CommonOpts.Loops != 0 {
				if doneLoops >= w.PlannedLoops {
					return
				}
			} else {
				if time.Now().UnixNano()-startTime >= int64(w.Benchmark.CommonOpts.Duration*1000000000) {
					return
				}
			}

			w.Benchmark.WorkerPreRunFunc(w)
			l = w.Benchmark.WorkerRunFunc(w)
			if l == 0 {
				return
			}
			doneLoops += l

			if w.Benchmark.NeedToExit {
				return
			}

			if w.Benchmark.CommonOpts.Sleep > 0 {
				time.Sleep(time.Millisecond * time.Duration(w.Benchmark.CommonOpts.Sleep))
			}
		}
	}
}

// Exit calls os.Exit() and sets 127 exit code if there is a message (+ args) passed, otherwise just exit with 0 (successfull exit)
func (b *Benchmark) Exit(fmtAndArgs ...interface{}) {
	if len(fmtAndArgs) == 0 {
		b.PreExit()
		os.Exit(0)
	}

	// Assume the first argument, if present, is the format string
	fmtStr, ok := fmtAndArgs[0].(string)
	if !ok {
		fmt.Println("First argument must be a format string.")
		b.PreExit()
		os.Exit(127)
	}

	// If there are more arguments, use them with fmt.Printf
	if len(fmtAndArgs) > 1 {
		args := fmtAndArgs[1:]
		fmt.Printf(fmtStr, args...)
	} else {
		// If fmtStr is the only argument, just print it
		fmt.Print(fmtStr)
	}

	fmt.Println()
	b.PreExit()
	os.Exit(127)
}

// Geomean calculates geometric mean of the given scores
func (b *Benchmark) Geomean(x []Score) float64 {
	var s float64
	for _, v := range x {
		s += math.Log(v.Rate)
	}
	s /= float64(len(x))

	return math.Exp(s)
}

// Shutdown gracefully shuts down the benchmark
func (b *Benchmark) Shutdown() {
	close(b.ShutdownCh)
	b.NeedToExit = true
}
