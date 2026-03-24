package ms

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMsRowsScanString(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"name": "alice"},
		},
		requestedColumns: []string{"name"},
	}

	require.True(t, rows.Next())

	var name string
	require.NoError(t, rows.Scan(&name))
	assert.Equal(t, "alice", name)

	assert.False(t, rows.Next())
	assert.NoError(t, rows.Close())
}

func TestMsRowsScanInt64FromFloat64(t *testing.T) {
	// JSON unmarshals numbers as float64 by default
	rows := &msRows{
		data: []map[string]interface{}{
			{"id": float64(42)},
		},
		requestedColumns: []string{"id"},
	}

	require.True(t, rows.Next())

	var id int64
	require.NoError(t, rows.Scan(&id))
	assert.Equal(t, int64(42), id)
}

func TestMsRowsScanInt64FromJsonNumber(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"id": json.Number("99")},
		},
		requestedColumns: []string{"id"},
	}

	require.True(t, rows.Next())

	var id int64
	require.NoError(t, rows.Scan(&id))
	assert.Equal(t, int64(99), id)
}

func TestMsRowsScanBool(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"active": true},
		},
		requestedColumns: []string{"active"},
	}

	require.True(t, rows.Next())

	var active bool
	require.NoError(t, rows.Scan(&active))
	assert.True(t, active)
}

func TestMsRowsScanBytes(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"data": "binary_content"},
		},
		requestedColumns: []string{"data"},
	}

	require.True(t, rows.Next())

	var data []byte
	require.NoError(t, rows.Scan(&data))
	assert.Equal(t, []byte("binary_content"), data)
}

func TestMsRowsScanStringSlice(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"tags": []interface{}{"tag1", "tag2", "tag3"}},
		},
		requestedColumns: []string{"tags"},
	}

	require.True(t, rows.Next())

	var tags []string
	require.NoError(t, rows.Scan(&tags))
	assert.Equal(t, []string{"tag1", "tag2", "tag3"}, tags)
}

func TestMsRowsScanStringSliceFromString(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"tags": "single_tag"},
		},
		requestedColumns: []string{"tags"},
	}

	require.True(t, rows.Next())

	var tags []string
	require.NoError(t, rows.Scan(&tags))
	assert.Equal(t, []string{"single_tag"}, tags)
}

func TestMsRowsScanTime(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	timeStr := now.Format(time.RFC3339Nano)

	rows := &msRows{
		data: []map[string]interface{}{
			{"created_at": timeStr},
		},
		requestedColumns: []string{"created_at"},
	}

	require.True(t, rows.Next())

	var ts time.Time
	require.NoError(t, rows.Scan(&ts))
	assert.Equal(t, timeStr, ts.Format(time.RFC3339Nano))
}

func TestMsRowsScanNilValue(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"name": nil},
		},
		requestedColumns: []string{"name"},
	}

	require.True(t, rows.Next())

	var name string
	require.NoError(t, rows.Scan(&name))
	assert.Equal(t, "", name) // stays zero value
}

func TestMsRowsScanMultipleColumns(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"id": float64(1), "name": "alice", "active": true},
			{"id": float64(2), "name": "bob", "active": false},
		},
		requestedColumns: []string{"id", "name", "active"},
	}

	require.True(t, rows.Next())
	var id int64
	var name string
	var active bool
	require.NoError(t, rows.Scan(&id, &name, &active))
	assert.Equal(t, int64(1), id)
	assert.Equal(t, "alice", name)
	assert.True(t, active)

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name, &active))
	assert.Equal(t, int64(2), id)
	assert.Equal(t, "bob", name)
	assert.False(t, active)

	assert.False(t, rows.Next())
}

func TestMsRowsScanColumnCountMismatch(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"id": float64(1), "name": "alice"},
		},
		requestedColumns: []string{"id", "name"},
	}

	require.True(t, rows.Next())

	var id int64
	err := rows.Scan(&id) // only 1 dest for 2 columns
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match")
}

func TestMsRowsScanNonPointer(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"name": "alice"},
		},
		requestedColumns: []string{"name"},
	}

	require.True(t, rows.Next())

	err := rows.Scan("not_a_pointer") // non-pointer
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-pointer")
}

func TestMsRowsScanTypeMismatch(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"name": float64(42)}, // float64, not string
		},
		requestedColumns: []string{"name"},
	}

	require.True(t, rows.Next())

	var name string
	err := rows.Scan(&name)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not equal type")
}

func TestMsRowsScanFloat32Slice(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"embedding": []interface{}{float64(0.5), float64(10), float64(6)}},
		},
		requestedColumns: []string{"embedding"},
	}

	require.True(t, rows.Next())

	var embedding []float32
	require.NoError(t, rows.Scan(&embedding))
	assert.Equal(t, []float32{0.5, 10, 6}, embedding)
}

func TestMsRowsScanFloat32SliceFromJsonNumber(t *testing.T) {
	rows := &msRows{
		data: []map[string]interface{}{
			{"embedding": []interface{}{json.Number("1.5"), json.Number("2.5"), json.Number("3.5")}},
		},
		requestedColumns: []string{"embedding"},
	}

	require.True(t, rows.Next())

	var embedding []float32
	require.NoError(t, rows.Scan(&embedding))
	assert.Equal(t, []float32{1.5, 2.5, 3.5}, embedding)
}

func TestMsRowsErr(t *testing.T) {
	rows := &msRows{}
	assert.NoError(t, rows.Err())
}
