package logic

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseAction_ParseParameters(t *testing.T) {
	tests := []struct {
		params      map[string]string
		expected    databaseAction
		expectError bool
		errMessage  string
	}{
		// Valid cases
		{
			params: map[string]string{
				"engine": "postgres",
				"query":  "1",
			},
			expected: databaseAction{
				Database: "postgres",
				Query:    1,
			},
			expectError: false,
		},
		{
			params: map[string]string{
				"engine": "mysql",
				"query":  "1",
			},
			expected: databaseAction{
				Database: "mysql",
				Query:    1,
			},
			expectError: false,
		},
		// Invalid cases
		{
			params:      map[string]string{"query": "1"},
			expectError: true,
			errMessage:  "engine parameter is missing",
		},
		{
			params:      map[string]string{"engine": "postgres"},
			expectError: true,
			errMessage:  "query parameter is missing",
		},
		{
			params: map[string]string{
				"engine": "unknown",
				"query":  "1",
			},
			expectError: true,
			errMessage:  "unknown database",
		},
		{
			params: map[string]string{
				"engine": "postgres",
				"query":  "invalid",
			},
			expectError: true,
			errMessage:  "failed conversion string to int in DatabaseArguments with: strconv.Atoi: parsing \"invalid\": invalid syntax",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("params=%v", tt.params), func(t *testing.T) {
			action := &databaseAction{}
			err := action.parseParameters(tt.params)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errMessage, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected.Database, action.Database)
				assert.Equal(t, tt.expected.Query, action.Query)
			}
		})
	}
}
