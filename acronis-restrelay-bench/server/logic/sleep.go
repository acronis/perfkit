package logic

import (
	"errors"
	"fmt"
	"time"
)

type sleepAction struct {
	duration time.Duration
}

func (sleepArgs *sleepAction) perform() error {
	time.Sleep(sleepArgs.duration)

	return nil
}

func (sleepArgs *sleepAction) parseParameters(params map[string]string) error {
	var duration, ok = params["duration"]
	if !ok {
		return errors.New("duration parameter is missing")
	}

	var err error
	sleepArgs.duration, err = time.ParseDuration(duration)
	if err != nil {
		return fmt.Errorf("failed conversion string to int in SleepArguments with: %v", err)
	}

	return nil
}
