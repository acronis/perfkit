package benchmark

import (
	"testing"
)

func TestRandStringBytesWithCardinality(t *testing.T) {
	r := NewRandomizer(1, 1)
	str := r.RandStringBytes("test_", 10, 20, 5, true)
	if len(str) < 5 || len(str) > 20 {
		t.Errorf("RandStringBytes() error, string length out of bounds")
	}
}

func TestRandStringBytesWithoutCardinality(t *testing.T) {
	r := NewRandomizer(1, 1)
	str := r.RandStringBytes("test_", 0, 20, 5, true)
	if len(str) < 5 || len(str) > 20 {
		t.Errorf("RandStringBytes() error, string length out of bounds")
	}
}

func TestGenFakeValueAutoInc(t *testing.T) {
	r := NewRandomizer(1, 1)
	val, err := r.GenFakeValue("autoinc", "test", 10, 20, 5, nil)
	if err != nil {
		t.Errorf("GenFakeValue() error: %v", err)
	}
	if val == nil {
		t.Errorf("GenFakeValue() error, value is nil")
	}
}

func TestGenFakeDataWithAutoInc(t *testing.T) {
	r := NewRandomizer(1, 1)
	cols, vals, err := r.GenFakeData(&[]DBFakeColumnConf{{"test", "autoinc", 10, 20, 5}}, true)
	if err != nil {
		t.Errorf("GenFakeData() error: %v", err)
	}
	if len(cols) != len(vals) {
		t.Errorf("GenFakeData() error, columns and values length mismatch")
	}
}

func TestGenFakeDataWithoutAutoInc(t *testing.T) {
	r := NewRandomizer(1, 1)
	cols, vals, err := r.GenFakeData(&[]DBFakeColumnConf{{"test", "autoinc", 10, 20, 5}}, false)
	if err != nil {
		t.Errorf("GenFakeData() error: %v", err)
	}
	if len(cols) != len(vals) {
		t.Errorf("GenFakeData() error, columns and values length mismatch")
	}
}
