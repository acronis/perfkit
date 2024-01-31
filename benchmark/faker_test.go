package benchmark

import (
	"testing"
)

func TestRandStringBytesWithCardinality(t *testing.T) {
	b := New()
	b.Randomizer = NewRandomizer(1, 1)
	str := b.RandStringBytes(1, "test_", 10, 20, 5, true)
	if len(str) < 5 || len(str) > 20 {
		t.Errorf("RandStringBytes() error, string length out of bounds")
	}
}

func TestRandStringBytesWithoutCardinality(t *testing.T) {
	b := New()
	b.Randomizer = NewRandomizer(1, 1)
	str := b.RandStringBytes(1, "test_", 0, 20, 5, true)
	if len(str) < 5 || len(str) > 20 {
		t.Errorf("RandStringBytes() error, string length out of bounds")
	}
}

func TestGenFakeValueAutoInc(t *testing.T) {
	b := New()
	b.Randomizer = NewRandomizer(1, 1)
	val := b.GenFakeValue(1, "autoinc", "test", 10, 20, 5, "")
	if val == nil {
		t.Errorf("GenFakeValue() error, value is nil")
	}
}

func TestGenFakeValueTenantUuid(t *testing.T) {
	b := New()
	b.Randomizer = NewRandomizer(1, 1)
	val := b.GenFakeValue(1, "tenant_uuid", "test", 10, 20, 5, "")
	if val == nil {
		t.Errorf("GenFakeValue() error, value is nil")
	}
}

func TestGenFakeDataWithAutoInc(t *testing.T) {
	b := New()
	b.Randomizer = NewRandomizer(1, 1)
	cols, vals := b.GenFakeData(1, &[]DBFakeColumnConf{{"test", "autoinc", 10, 20, 5}}, true)
	if len(cols) != len(vals) {
		t.Errorf("GenFakeData() error, columns and values length mismatch")
	}
}

func TestGenFakeDataWithoutAutoInc(t *testing.T) {
	b := New()
	b.Randomizer = NewRandomizer(1, 1)
	cols, vals := b.GenFakeData(1, &[]DBFakeColumnConf{{"test", "autoinc", 10, 20, 5}}, false)
	if len(cols) != len(vals) {
		t.Errorf("GenFakeData() error, columns and values length mismatch")
	}
}

func TestGenDBParameterPlaceholders(t *testing.T) {
	placeholders := GenDBParameterPlaceholders(1, 5)
	if placeholders != "$2,$3,$4,$5,$6" {
		t.Errorf("GenDBParameterPlaceholders() error, placeholders mismatch")
	}
}

func TestGenDBParameterPlaceholdersCassandra(t *testing.T) {
	placeholders := GenDBParameterPlaceholdersCassandra(1, 5)
	if placeholders != "?,?,?,?,?" {
		t.Errorf("GenDBParameterPlaceholdersCassandra() error, placeholders mismatch")
	}
}
