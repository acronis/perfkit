package logic

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializationCache_GetSerializable(t *testing.T) {
	tests := []struct {
		args serializationAction
	}{
		{serializationAction{TreeDepth: 2, TreeWidth: 2}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("args=%v", test.args), func(t *testing.T) {
			serializable := cache.GetSerializable(test.args)
			assert.NotNil(t, serializable, "Expected a non-nil Serializable object")
			assert.Equal(t, "Serializable", serializable.StringField, "Expected StringField to be 'Serializable'")
			assert.True(t, serializable.BooleanField, "Expected BooleanField to be true")
			assert.Equal(t, 12345, serializable.IntegerField, "Expected IntegerField to be 12345")
			assert.Equal(t, float32(11.1234), serializable.FloatField, "Expected FloatField to be 11.1234")
			assert.Equal(t, 2, len(serializable.ObjectArrayField), "Expected ObjectArrayField length to be 2")
		})
	}
}

func TestSerializationCache_GetSerialized(t *testing.T) {
	tests := []struct {
		args serializationAction
	}{
		{serializationAction{TreeDepth: 2, TreeWidth: 2}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("args=%v", test.args), func(t *testing.T) {
			serialized, err := cache.GetSerialized(test.args)
			assert.NoError(t, err, "Expected no error while getting serialized data")
			assert.NotEmpty(t, serialized, "Expected non-empty serialized data")
		})
	}
}

func TestSerializationAction_Validate(t *testing.T) {
	tests := []struct {
		action  serializationAction
		wantErr bool
	}{
		{serializationAction{TreeDepth: 1, TreeWidth: 1}, false},
		{serializationAction{TreeDepth: 0, TreeWidth: 0}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("action=%v", test.action), func(t *testing.T) {
			err := test.action.Validate()
			if test.wantErr {
				assert.Error(t, err, "Expected error for invalid serializationAction")
			} else {
				assert.NoError(t, err, "Expected no error for valid serializationAction")
			}
		})
	}
}

func TestParseAction_Validate(t *testing.T) {
	tests := []struct {
		action  parseAction
		wantErr bool
	}{
		{parseAction{TreeDepth: 1, TreeWidth: 1}, false},
		{parseAction{TreeDepth: 0, TreeWidth: 0}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("action=%v", test.action), func(t *testing.T) {
			err := test.action.Validate()
			if test.wantErr {
				assert.Error(t, err, "Expected error for invalid parseAction")
			} else {
				assert.NoError(t, err, "Expected no error for valid parseAction")
			}
		})
	}
}

func TestSerializationAction_ParseParameters(t *testing.T) {
	tests := []struct {
		params    map[string]string
		wantDepth int
		wantWidth int
		err       error
	}{
		// Valid cases
		{map[string]string{"depth": "2", "width": "3"}, 2, 3, nil},
		// Invalid cases
		{map[string]string{"width": "3"}, 0, 0, errors.New("depth parameter is missing")},
		{map[string]string{"depth": "invalid", "width": "3"}, 0, 0, fmt.Errorf("failed conversion string to int in SerializationArguments with: %v", errors.New(`strconv.Atoi: parsing "invalid": invalid syntax`))},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("params=%v", test.params), func(t *testing.T) {
			act := &serializationAction{}
			err := act.parseParameters(test.params)
			if err != nil && test.err == nil || err == nil && test.err != nil {
				t.Errorf("expected error: %v, got: %v", test.err, err)
			}
			if err != nil && test.err != nil && err.Error() != test.err.Error() {
				t.Errorf("expected error message: %v, got: %v", test.err.Error(), err.Error())
			}
			if act.TreeDepth != test.wantDepth {
				t.Errorf("expected TreeDepth: %v, got: %v", test.wantDepth, act.TreeDepth)
			}
			if act.TreeWidth != test.wantWidth {
				t.Errorf("expected TreeWidth: %v, got: %v", test.wantWidth, act.TreeWidth)
			}
		})
	}
}

func TestParseAction_ParseParameters(t *testing.T) {
	tests := []struct {
		params    map[string]string
		wantDepth int
		wantWidth int
		err       error
	}{
		// Valid cases
		{map[string]string{"depth": "2", "width": "3"}, 2, 3, nil},
		// Invalid cases
		{map[string]string{"width": "3"}, 0, 0, errors.New("depth parameter is missing")},
		{map[string]string{"depth": "invalid", "width": "3"}, 0, 0, fmt.Errorf("failed conversion string to int in ParseArguments with: %v", errors.New(`strconv.Atoi: parsing "invalid": invalid syntax`))},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("params=%v", test.params), func(t *testing.T) {
			act := &parseAction{}
			err := act.parseParameters(test.params)
			if err != nil && test.err == nil || err == nil && test.err != nil {
				t.Errorf("expected error: %v, got: %v", test.err, err)
			}
			if err != nil && test.err != nil && err.Error() != test.err.Error() {
				t.Errorf("expected error message: %v, got: %v", test.err.Error(), err.Error())
			}
			if act.TreeDepth != test.wantDepth {
				t.Errorf("expected TreeDepth: %v, got: %v", test.wantDepth, act.TreeDepth)
			}
			if act.TreeWidth != test.wantWidth {
				t.Errorf("expected TreeWidth: %v, got: %v", test.wantWidth, act.TreeWidth)
			}
		})
	}
}

/*
func TestSerializationActionCacheCalculate(t *testing.T) {
	for i := 1; i < 8; i++ {
		for j := 1; j < 10; j++ {
			var ser, err = cache.GetSerialized(serializationAction{TreeDepth: i, TreeWidth: j})
			if err != nil {
				t.Error(err)
			}
			fmt.Printf("depth: \t %d, width: \t %d, size: %d bytes\n", i, j, len(ser))
		}
	}
}

func TestSerializationCacheSizes(t *testing.T) {
	for _, sizeRaw := range []string{"100B", "1KB", "2KB", "4KB", "8KB", "16KB", "32KB", "64KB", "128KB", "256KB", "512KB", "1MB", "2MB", "4MB", "8MB", "16MB", "32MB", "64MB", "128MB", "256MB", "512MB", "1GB"} {
		var size, _ = parseFileSize(sizeRaw)
		depth, width := getClosestDepthAndWidth(int(size))
		fmt.Printf("For size %d bytes (or %s), the closest depth is %d and width is %d.\n", size, sizeRaw, depth, width)

		var ser, err = cache.GetSerialized(serializationAction{TreeDepth: depth, TreeWidth: width})
		if err != nil {
			t.Error(err)
		}
		fmt.Printf("depth: \t %d, width: \t %d, size in bytes: \t %d bytes\n", depth, width, len(ser))
	}
}

*/
