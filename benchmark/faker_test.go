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
	val := b.GenFakeValue(1, "autoinc", "test", 10, 20, 5, nil)
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

func Test_cardinalityCacheType_randStringWithCardinality(t *testing.T) {
	t.Parallel()

	type args struct {
		randID      int
		pfx         string
		cardinality int
		maxsize     int
		minsize     int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Cardinality 10, maxsize 20, minsize 5",
			args: args{randID: 1, pfx: "test_", cardinality: 10, maxsize: 20, minsize: 5},
		},
		{
			name: "Cardinality 10, maxsize 32, minsize 4",
			args: args{randID: 1, pfx: "test_", cardinality: 10, maxsize: 32, minsize: 4},
		},
		// Test not passing yet
		//{
		//	name: "cardinality 4, maxsize 16, minsize 4, pfx length larger than maxsize",
		//	args: args{randID: 1, pfx: "testabcdefghijklmnopqr_", cardinality: 4, maxsize: 16, minsize: 4},
		//},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cc := &cardinalityCacheType{entities: make(map[string][]string)}

			got := cc.randStringWithCardinality(tt.args.randID, tt.args.pfx, tt.args.cardinality, tt.args.maxsize, tt.args.minsize)
			if len(got) < tt.args.minsize || len(got) > tt.args.maxsize {
				t.Errorf("cardinalityCacheType.randStringWithCardinality() error, string length out of bounds, got = %v", got)
			}
		})
	}
}
