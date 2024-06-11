package logic

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestParseParameters(t *testing.T) {
	tests := []struct {
		params   map[string]string
		expected time.Duration
		err      error
	}{
		// Valid cases
		{map[string]string{"duration": "1s"}, time.Second, nil},
		{map[string]string{"duration": "500ms"}, 500 * time.Millisecond, nil},
		// Invalid cases
		{map[string]string{}, 0, errors.New("duration parameter is missing")},
		{map[string]string{"duration": "invalid"}, 0, fmt.Errorf("failed conversion string to int in SleepArguments with: %v", errors.New(`time: invalid duration "invalid"`))},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("params=%v", test.params), func(t *testing.T) {
			act := &sleepAction{}
			err := act.parseParameters(test.params)
			if err != nil && test.err == nil || err == nil && test.err != nil {
				t.Errorf("expected error: %v, got: %v", test.err, err)
			}
			if err != nil && test.err != nil && err.Error() != test.err.Error() {
				t.Errorf("expected error message: %v, got: %v", test.err.Error(), err.Error())
			}
			if act.duration != test.expected {
				t.Errorf("expected duration: %v, got: %v", test.expected, act.duration)
			}
		})
	}
}
