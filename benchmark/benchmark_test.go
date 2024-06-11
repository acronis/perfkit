package benchmark

import (
	"os"
	"testing"
	"time"
)

func TestNewBenchmark(t *testing.T) {
	b := New()
	if b == nil {
		t.Errorf("New() error, benchmark not created")
	}
}

func TestRunOnce(t *testing.T) {
	b := New()
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 1
	b.Worker = func(id int) (loops int) { //nolint:revive
		return 1
	}
	b.RunOnce(false)
	if b.Score.Workers != 1 {
		t.Errorf("RunOnce() error, workers = %v, want %v", b.Score.Workers, 1)
	}
}

func TestFormatRateWithZeroRate(t *testing.T) {
	score := Score{Rate: 0.0}
	result := score.FormatRate(4)
	if result != "0" {
		t.Errorf("FormatRate() error, expected '0', got '%s'", result)
	}
}

func TestFormatRateWithNonZeroRate(t *testing.T) {
	score := Score{Rate: 1234.5678}
	result := score.FormatRate(4)
	if result != "1235" {
		t.Errorf("FormatRate() error, expected '1234.5678', got '%s'", result)
	}
}

func TestFormatRateWithLargeRate(t *testing.T) {
	score := Score{Rate: 12345678.12345678}
	result := score.FormatRate(4)
	if result != "12345678" {
		t.Errorf("FormatRate() error, expected '12345678.1235', got '%s'", result)
	}
}

func TestInitOpts(t *testing.T) {
	b := New()
	os.Args = []string{"test", "--duration=1", "--loops=1", "-c=1"}
	b.InitOpts()

	if !b.OptsInitialized {
		t.Errorf("InitOpts() error, expected OptsInitialized to be true, got false")
	}

	if b.Logger == nil {
		t.Errorf("InitOpts() error, expected Logger to be initialized, got nil")
	}

	if b.Cli.parser == nil {
		t.Errorf("InitOpts() error, expected Cli.parser to be initialized, got nil")
	}

	if b.CommonOpts.Workers != 1 {
		t.Errorf("InitOpts() error, expected CommonOpts.Workers to be 1, got %d", b.CommonOpts.Workers)
	}
}

func TestGeomean(t *testing.T) {
	b := New()
	scores := []Score{
		{Rate: 2.0},
		{Rate: 8.0},
	}

	result := b.Geomean(scores)

	if result != 4.0 {
		t.Errorf("Geomean() error, expected 4.0, got %f", result)
	}
}

func TestRun(t *testing.T) {
	os.Args = []string{"test", "--duration=1", "--loops=1", "-c=1"}
	b := New()
	b.Worker = func(id int) (loops int) { //nolint:revive
		return 1
	}
	b.Run()

	if b.Score.Workers != 1 {
		t.Errorf("Run() error, workers = %v, want %v", b.Score.Workers, 1)
	}

	if b.Score.Loops != 1 {
		t.Errorf("Run() error, loops = %v, want %v", b.Score.Loops, 1)
	}

	if b.Score.Rate == 0 {
		t.Errorf("Run() error, rate should not be 0")
	}

	if b.Score.Seconds > float64(time.Second) {
		t.Errorf("Run() error, seconds = %v, want less than or equal to %v", b.Score.Seconds, 1)
	}
}
