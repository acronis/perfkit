package dataset_source

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadParquetWithOffset(t *testing.T) {
	// Test cases with different offset values
	testCases := []struct {
		name     string
		offset   int64
		expected int64 // Expected number of records to read
	}{
		{
			name:     "Read from beginning",
			offset:   0,
			expected: 100, // File has 100 records
		},
		{
			name:     "Read from middle",
			offset:   50,
			expected: 50,
		},
		{
			name:     "Read from end",
			offset:   99,
			expected: 1,
		},
		{
			name:     "Read beyond file size",
			offset:   200,
			expected: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a new parquet reader
			reader, err := NewParquetFileDataSource("numbers.parquet", tc.offset)
			require.NoError(t, err)
			defer reader.Close()

			// Read all records
			var count int64
			for {
				row, err := reader.GetNextRow()
				if err != nil {
					break
				}
				if row == nil {
					break
				}
				count++
			}

			// Verify the number of records read
			assert.Equal(t, tc.expected, count)
		})
	}
}

func TestReadParquetWithOffsetAndLimit(t *testing.T) {
	// Test cases combining offset and limit
	testCases := []struct {
		name     string
		offset   int64
		limit    int64
		expected int64
	}{
		{
			name:     "Read first 10 records",
			offset:   0,
			limit:    10,
			expected: 10,
		},
		{
			name:     "Read 20 records from middle",
			offset:   40,
			limit:    20,
			expected: 20,
		},
		{
			name:     "Read beyond file size",
			offset:   90,
			limit:    20,
			expected: 10, // Only 10 records left from offset 90
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := NewParquetFileDataSource("numbers.parquet", tc.offset)
			require.NoError(t, err)
			defer reader.Close()

			var count int64
			for {
				row, err := reader.GetNextRow()
				if err != nil {
					break
				}
				if row == nil {
					break
				}
				if count >= tc.limit {
					break
				}
				count++
			}

			assert.Equal(t, tc.expected, count)
		})
	}
}
