package db

import (
	"testing"
)

// TestScanWithValidData tests Scan() function
func TestScanWithValidData(t *testing.T) {
	rows := &SurrogateRows{
		data: []surrogateRowsRow{
			{"test", 1, true},
		},
		idx: 1,
	}
	var str string
	var i int
	var b bool
	err := rows.Scan(&str, &i, &b)
	if err != nil {
		t.Errorf("Scan() error, expected no error but got: %v", err)
	}
}

// TestScanWithInvalidData tests Scan() function
func TestScanWithInvalidData(t *testing.T) {
	rows := &SurrogateRows{
		data: []surrogateRowsRow{
			{"test", "invalid", true},
		},
		idx: 1,
	}
	var str string
	var i int
	var b bool
	err := rows.Scan(&str, &i, &b)
	if err == nil {
		t.Errorf("Scan() error, expected error but got none")
	}
}

// TestNextWithRemainingData tests Next() function
func TestNextWithRemainingData(t *testing.T) {
	rows := &SurrogateRows{
		data: []surrogateRowsRow{
			{"test", 1, true},
			{"test2", 2, false},
		},
		idx: 1,
	}
	if !rows.Next() {
		t.Errorf("Next() error, expected true but got false")
	}
}

// TestNextWithoutRemainingData tests Next() function
func TestNextWithoutRemainingData(t *testing.T) {
	rows := &SurrogateRows{
		data: []surrogateRowsRow{
			{"test", 1, true},
		},
		idx: 1,
	}
	if rows.Next() {
		t.Errorf("Next() error, expected false but got true")
	}
}

// TestClose tests Close() function
func TestClose(t *testing.T) {
	rows := &SurrogateRows{}
	err := rows.Close()
	if err != nil {
		t.Errorf("Close() error, expected no error but got: %v", err)
	}
}

// TestDumpWithMultipleRows tests Dump() function
func TestDumpWithMultipleRows(t *testing.T) {
	rows := &SurrogateRows{
		data: []surrogateRowsRow{
			{"test", 1, true},
			{"test2", 2, false},
		},
	}
	dump := rows.Dump()
	if dump == "" {
		t.Errorf("Dump() error, dump is empty")
	}
}

// TestDumpWithSingleRow tests Dump() function
func TestDumpWithSingleRow(t *testing.T) {
	rows := &SurrogateRows{
		data: []surrogateRowsRow{
			{"test", 1, true},
		},
	}
	dump := rows.Dump()
	if dump == "" {
		t.Errorf("Dump() error, dump is empty")
	}
}
