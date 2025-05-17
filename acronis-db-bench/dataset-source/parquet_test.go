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
			reader, err := NewParquetFileDataSource("numbers.parquet", tc.offset, false)
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
			reader, err := NewParquetFileDataSource("numbers.parquet", tc.offset, false)
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

func TestReadParquetCircular(t *testing.T) {
	testCases := []struct {
		name           string
		offset         int64
		expectedRounds int   // Number of complete file reads to perform
		expectedTotal  int64 // Total number of records to read
	}{
		{
			name:           "Read file twice",
			offset:         0,
			expectedRounds: 2,
			expectedTotal:  201, // 100 records + 101 records (including the first record of the second round)
		},
		{
			name:           "Read file twice from middle",
			offset:         50,
			expectedRounds: 2,
			expectedTotal:  201, // 50 records + 100 records + 51 records (including the first record of the second round)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := NewParquetFileDataSource("numbers.parquet", tc.offset, true)
			require.NoError(t, err)
			defer reader.Close()

			var count int64
			var rounds int
			var firstRow []interface{}
			var isFirstRow = true

			for {
				row, err := reader.GetNextRow()
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if row == nil {
					t.Fatalf("Unexpected nil row")
				}

				// Store the first row we read
				if isFirstRow {
					firstRow = row
					isFirstRow = false
				}

				// Check if we've completed a full round
				if count > 0 && count%100 == 0 && !isFirstRow {
					rounds++
					// Verify we're back at the beginning by comparing with first row
					if rounds > 1 {
						assert.Equal(t, firstRow, row, "Row should match after completing a round")
					}
				}

				count++

				// Stop after expected number of rounds
				if rounds >= tc.expectedRounds {
					break
				}
			}

			assert.Equal(t, tc.expectedTotal, count, "Total number of records read")
			assert.Equal(t, tc.expectedRounds, rounds, "Number of complete rounds")
		})
	}
}
