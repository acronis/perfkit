package logic

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetupReferenceIterations(t *testing.T) {
	SetupReferenceIterations()
	assert.Greater(t, referenceIterationsPerSec, 0.0, "Expected referenceIterationsPerSec to be greater than 0")
}

func TestBusyLoopAction_ParseParameters(t *testing.T) {
	tests := []struct {
		params      map[string]string
		expected    busyLoopAction
		expectError bool
		errMessage  string
	}{
		// Valid cases
		{
			params: map[string]string{
				"iterations": "1000",
			},
			expected: busyLoopAction{
				iterations: 1000,
			},
			expectError: false,
		},
		{
			params: map[string]string{
				"duration": "2s",
			},
			expected: busyLoopAction{
				duration: 2 * time.Second,
			},
			expectError: false,
		},
		// Invalid cases
		{
			params: map[string]string{
				"iterations": "1000",
				"duration":   "2s",
			},
			expectError: true,
			errMessage:  "both iterations and duration parameters are set",
		},
		{
			params:      map[string]string{},
			expectError: true,
			errMessage:  "either iterations or duration parameters should be set",
		},
		{
			params: map[string]string{
				"iterations": "invalid",
			},
			expectError: true,
			errMessage:  "failed conversion string to int in BusyLoopArguments with: strconv.Atoi: parsing \"invalid\": invalid syntax",
		},
		{
			params: map[string]string{
				"duration": "invalid",
			},
			expectError: true,
			errMessage:  "failed conversion string to duration in BusyLoopArguments with: time: invalid duration \"invalid\"",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("params=%v", tt.params), func(t *testing.T) {
			action := &busyLoopAction{}
			err := action.parseParameters(tt.params)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errMessage, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected.iterations, action.iterations)
				assert.Equal(t, tt.expected.duration, action.duration)
			}
		})
	}
}
