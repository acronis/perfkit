package logic

import (
	"reflect"
	"testing"
)

func TestParseFunctionString(t *testing.T) {
	tests := []struct {
		input             string
		expectedFunction  string
		expectedArguments map[string]string
		expectError       bool
	}{
		// Valid cases with arguments
		{"busyloop(iterations=5000,duration=5s)", "busyloop", map[string]string{"iterations": "5000", "duration": "5s"}, false},
		{"busyloop(duration=10m)", "busyloop", map[string]string{"duration": "10m"}, false},
		{"busyloop(iterations=1000)", "busyloop", map[string]string{"iterations": "1000"}, false},
		// Valid cases without arguments
		{"busyloop", "busyloop", map[string]string{}, false},
		// Invalid cases
		{"busyloop()", "busyloop", map[string]string{}, false},
		{"busyloop(", "", nil, true},
		{"busyloop(iterations=5000", "", nil, true},
		{"busyloop(iterations=5000,duration=5s", "", nil, true},
		{"busyloopiterations=5000,duration=5s)", "", nil, true},
		{"busyloop(iterations=5000 duration=5s)", "", nil, true},
		{"busyloop(iterations5000,duration=5s)", "", nil, true},
		// Edge cases
		{"busyloop(iterations=5000,duration=5s,)", "", nil, true},
		{"busyloop(,iterations=5000,duration=5s)", "", nil, true},
		{"busyloop(iterations=5000,,duration=5s)", "", nil, true},
		{"", "", nil, true},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			function, arguments, err := parseFunctionString(test.input)
			if (err != nil) != test.expectError {
				t.Errorf("expected error: %v, got: %v", test.expectError, err)
			}
			if function != test.expectedFunction {
				t.Errorf("expected function: %s, got: %s", test.expectedFunction, function)
			}
			if !reflect.DeepEqual(arguments, test.expectedArguments) {
				t.Errorf("expected arguments: %v, got: %v", test.expectedArguments, arguments)
			}
		})
	}
}
