package logic

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFileSize(t *testing.T) {
	tests := []struct {
		sizeStr     string
		expected    int64
		expectError bool
		errMessage  string
	}{
		// Valid cases
		{"10B", 10, false, ""},
		{"1KB", 1024, false, ""},
		{"1.5KB", 1536, false, ""},
		{"1MB", 1024 * 1024, false, ""},
		{"2.5MB", 2.5 * 1024 * 1024, false, ""},
		{"1GB", 1024 * 1024 * 1024, false, ""},
		{"1TB", 1024 * 1024 * 1024 * 1024, false, ""},
		{"1PB", 1024 * 1024 * 1024 * 1024 * 1024, false, ""},
		// Invalid cases
		{"10", 0, true, "invalid size format"},
		{"10XB", 0, true, "invalid size format"},
		{"1.5.5KB", 0, true, "strconv.ParseFloat: parsing \"1.5.5\": invalid syntax"},
		{"", 0, true, "invalid size format"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("sizeStr=%s", tt.sizeStr), func(t *testing.T) { //nolint:perfsprint
			result, err := parseFileSize(tt.sizeStr)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errMessage, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestAllocationAction_ParseParameters(t *testing.T) {
	tests := []struct {
		params      map[string]string
		expected    allocationAction
		expectError bool
		errMessage  string
	}{
		// Valid cases
		{
			params: map[string]string{
				"size": "1KB",
			},
			expected: allocationAction{
				size: 1024,
			},
			expectError: false,
		},
		{
			params: map[string]string{
				"size": "10MB",
			},
			expected: allocationAction{
				size: 10 * 1024 * 1024,
			},
			expectError: false,
		},
		// Invalid cases
		{
			params:      map[string]string{},
			expectError: true,
			errMessage:  "size parameter is missing",
		},
		{
			params: map[string]string{
				"size": "invalid",
			},
			expectError: true,
			errMessage:  "failed conversion string to int in AllocationArguments with: invalid size format",
		},
		{
			params: map[string]string{
				"size": "unsupported",
			},
			expectError: true,
			errMessage:  "failed conversion string to int in AllocationArguments with: invalid size format",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("params=%v", tt.params), func(t *testing.T) {
			act := &allocationAction{}
			err := act.parseParameters(tt.params)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errMessage, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected.size, act.size)
			}
		})
	}
}

/*
func TestCalculateSize(t *testing.T) {
	res := make([]allocationStruct, 0)

	for i := int64(0); i < 1024; i++ {
		res = append(res, allocationStruct{ //nolint:staticcheck
			RandomFieldInt:    2,
			RandomFieldString: "test string",
		})
	}

	var structSize = unsafe.Sizeof(allocationStruct{
		RandomFieldInt:    2,
		RandomFieldString: "test string",
	})
	var iterations = int64(structSize)

	fmt.Println("Size of []int32:", iterations)

	fmt.Println("Size of []int32:", unsafe.Sizeof(res))
	fmt.Println("Size of int32:", unsafe.Sizeof(allocationStruct{}))
	fmt.Println("Size of [1000]int32:", unsafe.Sizeof([1024]allocationStruct{}))
	fmt.Println("Real size of s:", unsafe.Sizeof(res)+unsafe.Sizeof([1000]allocationStruct{}))
}

*/
