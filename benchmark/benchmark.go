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
	AddOpts         func() TestOpts
	Init            func()
	InitPerWorker   func(id int)
	PreWorker       func(id int)
	Worker          func(id int) (loops int)
	FinishPerWorker func(id int)
	Finish          func()
	PreExit         func()
	Metric          func() (metric string)
	GetRate         func(loops uint64, seconds float64) float64
	PrintScore      func(score Score)
	CommonOpts      CommonOpts
	Cli             CLI
	TestOpts        TestOpts
	OptsInitialized bool
	ReadOnly        bool
	Logger          *Logger
	Randomizer      *Randomizer

	NeedToExit bool
	Score      Score

	CliArgs    []string
	WorkerData []WorkerData
	Vault      AnyData

	ShutdownCh chan struct{}
	signalDone chan struct{}
}

// New creates a new Benchmark instance with default values
func New() *Benchmark {
	b := Benchmark{
		AddOpts: func() TestOpts {
			var testOpts TestOpts

			return &testOpts
		},
		Init: func() {
		},
		InitPerWorker: func(id int) { //nolint:revive
		},
		PreWorker: func(id int) { //nolint:revive
		},
		Worker: func(id int) (loops int) { //nolint:revive
			return 0
		},
		PreExit: func() {
		},
		FinishPerWorker: func(id int) { //nolint:revive
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
	b.Logger = NewLogger(LogWarn)
	b.Cli.Init(os.Args[0], &b.CommonOpts)

	return &b
}

// Logn logs a formatted log message to stdout if the log level is high enough
func (b *Benchmark) Logn(LogLevel int, workerID int, format string, args ...interface{}) {
	b.Logger.Logn(LogLevel, workerID, format, args...)
}

// Log logs a formatted log message to stdout if the log level is high enough
func (b *Benchmark) Log(LogLevel int, workerID int, format string, args ...interface{}) {
	b.Logger.Log(LogLevel, workerID, format, args...)
}

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
		b.Logger = NewLogger(LogError)
	} else {
		b.Logger = NewLogger(len(b.CommonOpts.Verbose) + 1)
	}
	b.adjustFilenoUlimit()
}

// SetUsage sets usage information
func (b *Benchmark) SetUsage(usage string) {
	b.Cli.SetUsage(usage)
}

// RunOnce runs the test once and prints the score
func (b *Benchmark) RunOnce(printScore bool) {
	var requiredLoops = make([]int, b.CommonOpts.Workers)

	if b.CommonOpts.Loops != 0 {
		l := b.CommonOpts.Loops / b.CommonOpts.Workers
		rest := b.CommonOpts.Loops % b.CommonOpts.Workers
		for i := 0; i < b.CommonOpts.Workers; i++ {
			requiredLoops[i] = l
			if i < rest {
				requiredLoops[i]++
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(b.CommonOpts.Workers)

	loops := make([]int, b.CommonOpts.Workers)

	startTime := time.Now().UnixNano()
	for i := 0; i < b.CommonOpts.Workers; i++ {
		go runner(i, b, &loops[i], requiredLoops[i], &wg)
	}
	wg.Wait()

	endTime := time.Now().UnixNano()

	var totalLoops uint64
	for _, loop := range loops {
		totalLoops += uint64(loop)
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
			b.Log(LogInfo, -1, "Received signal %v, initiating graceful shutdown...", sig)
			b.Shutdown()
		case <-ctx.Done():
			// Clean up signal handling
			signal.Stop(sigChan)
			close(sigChan)
			return
		}
	}()

	b.Randomizer = NewRandomizer(b.CommonOpts.RandSeed, b.CommonOpts.Workers)
	b.Init()

	b.WorkerData = make([]WorkerData, b.CommonOpts.Workers)

	b.Log(LogDebug, 0, "per-worker initialization")
	for i := 0; i < b.CommonOpts.Workers; i++ {
		b.InitPerWorker(i)
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

	b.Log(LogDebug, 0, "per-worker termination")

	for i := 0; i < b.CommonOpts.Workers; i++ {
		b.FinishPerWorker(i)
	}

	b.Finish()

	b.PrintScore(b.Score)

	if b.CommonOpts.Repeat > 1 {
		fmt.Printf("Avg rate: %8.1f; Min rate: %8.1f; Max rate: %8.1f\n", sumRate/float64(b.CommonOpts.Repeat), minRate, maxRate)
	}

	// The deferred cancel() will clean up the signal handling goroutine
}

// runner is a helper function for running tests in parallel
func runner(id int, b *Benchmark, loops *int, requiredLoops int, wg *sync.WaitGroup) {
	var doneLoops = 0

	defer func() {
		*loops = doneLoops
		wg.Done()
	}()

	var l int
	var startTime = time.Now().UnixNano()
	for {
		select {
		case <-b.ShutdownCh:
			b.Log(LogInfo, id, "Worker %d shutting down...", id)
			return
		default:
			if b.CommonOpts.Loops != 0 {
				if doneLoops >= requiredLoops {
					return
				}
			} else {
				if time.Now().UnixNano()-startTime >= int64(b.CommonOpts.Duration*1000000000) {
					return
				}
			}

			b.PreWorker(id)
			l = b.Worker(id)
			if l == 0 {
				return
			}
			doneLoops += l

			if b.NeedToExit {
				return
			}

			if b.CommonOpts.Sleep > 0 {
				time.Sleep(time.Millisecond * time.Duration(b.CommonOpts.Sleep))
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
