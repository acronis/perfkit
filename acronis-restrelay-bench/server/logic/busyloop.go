package logic

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

var referenceIterations int64 = 500_000_000
var referenceIterationsPerSec float64

// SetupReferenceIterations setups reference iterations
func SetupReferenceIterations() {
	var i int64
	var foo = 2.0
	var saved = time.Now()

	for i = 0; i < referenceIterations; i++ {
		foo += math.Sqrt(foo) + 1
	}
	var took = time.Since(saved)

	referenceIterationsPerSec = float64(referenceIterations) / took.Seconds()
}

type busyLoopAction struct {
	duration   time.Duration
	iterations int
}

func (busyArgs *busyLoopAction) perform() error {
	var iterations int64
	if busyArgs.iterations != 0 {
		iterations = int64(busyArgs.iterations)
	} else {
		iterations = int64(referenceIterationsPerSec * float64(busyArgs.duration/time.Second))
	}

	var foo = 2.0
	for i := int64(0); i < iterations; i++ {
		foo += math.Sqrt(foo) + 1
	}

	return nil
}

func (busyArgs *busyLoopAction) parseParameters(params map[string]string) error {
	var iterations, iterationsSet = params["iterations"]
	var duration, durationSet = params["duration"]

	if iterationsSet && durationSet {
		return errors.New("both iterations and duration parameters are set")
	} else if !iterationsSet && !durationSet {
		return errors.New("either iterations or duration parameters should be set")
	}

	var err error

	if iterationsSet {
		busyArgs.iterations, err = strconv.Atoi(iterations)
		if err != nil {
			return fmt.Errorf("failed conversion string to int in BusyLoopArguments with: %v", err)
		}
	} else {
		busyArgs.duration, err = time.ParseDuration(duration)
		if err != nil {
			return fmt.Errorf("failed conversion string to duration in BusyLoopArguments with: %v", err)
		}
	}

	return nil
}
