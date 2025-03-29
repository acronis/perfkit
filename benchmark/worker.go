package benchmark

import "github.com/acronis/perfkit/logger"

type BenchmarkWorker struct {
	Benchmark *Benchmark
	WorkerID  int
	Logger    logger.Logger
	Data      WorkerData

	Randomizer *Randomizer

	PlannedLoops  int
	ExecutedLoops int
}

func NewBenchmarkWorker(b *Benchmark, workerID int) *BenchmarkWorker {
	randomizer := NewRandomizer(b.CommonOpts.RandSeed, workerID)
	return &BenchmarkWorker{
		Benchmark:  b,
		WorkerID:   workerID,
		Logger:     logger.NewWorkerLogger(b.Logger.GetLevel(), false, workerID),
		Randomizer: randomizer,
	}
}

func (w *BenchmarkWorker) Exit(err error) {
	w.Benchmark.Exit(err)
}
