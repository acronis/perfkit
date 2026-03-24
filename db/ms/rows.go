package ms

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/acronis/perfkit/db"
)

// msRows stores Meilisearch search results and provides db.Rows interface
type msRows struct {
	data []map[string]interface{}
	idx  int

	requestedColumns []string
}

// Next advances the cursor to the next row
func (r *msRows) Next() bool {
	if r.idx < len(r.data) {
		r.idx++
		return true
	}
	return false
}

// Err returns any error encountered during iteration
func (r *msRows) Err() error {
	return nil
}

// Scan copies the columns in the current row into the values pointed at by dest
func (r *msRows) Scan(dest ...interface{}) error {
	if len(dest) != len(r.requestedColumns) {
		return fmt.Errorf("number of columns in the result set does not match the number of destination fields")
	}

	row := r.data[r.idx-1]

	for i := range dest {
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Ptr {
			return fmt.Errorf("internal error: msRows.Scan() - non-pointer passed to Scan: %v", dest)
		}

		fieldName := r.requestedColumns[i]
		val := row[fieldName]

		if val == nil {
			continue
		}

		switch d := dest[i].(type) {
		case *string:
			strVal, ok := val.(string)
			if !ok {
				return fmt.Errorf("%s: not equal type in struct 'string', in map '%T'", fieldName, val)
			}
			*d = strVal
		case *int64:
			switch numberType := val.(type) {
			case json.Number:
				var err error
				*d, err = numberType.Int64()
				if err != nil {
					return fmt.Errorf("%s: failed to cast jsonNumber to int64 '%T': %v", fieldName, numberType, err)
				}
			case float64:
				*d = int64(numberType)
			default:
				return fmt.Errorf("%s: not equal type, in map '%T'", fieldName, val)
			}
		case *bool:
			boolVal, ok := val.(bool)
			if !ok {
				return fmt.Errorf("%s: not equal type in struct 'bool', in map '%T'", fieldName, val)
			}
			*d = boolVal
		case *[]byte:
			strVal, ok := val.(string)
			if !ok {
				return fmt.Errorf("%s: not equal type in struct '[]byte', in map '%T'", fieldName, val)
			}
			*d = []byte(strVal)
		case *[]string:
			switch v := val.(type) {
			case string:
				*d = []string{v}
			case []interface{}:
				strSlc := make([]string, len(v))
				for j, elem := range v {
					strSlc[j] = fmt.Sprint(elem)
				}
				*d = strSlc
			default:
				return fmt.Errorf("%s: not equal type in struct '[]string', in map '%T'", fieldName, val)
			}
		case *time.Time:
			strVal, ok := val.(string)
			if !ok {
				return fmt.Errorf("%s: not equal type in struct 'time.Time', in map '%T'", fieldName, val)
			}
			var err error
			*d, err = time.Parse(time.RFC3339Nano, strVal)
			if err != nil {
				return fmt.Errorf("%s: failed to cast string to time.Time '%T': %v", fieldName, strVal, err)
			}
		case *[]float32:
			sliceVal, ok := val.([]interface{})
			if !ok {
				return fmt.Errorf("%s: not equal type in struct '[]float32', in map '%T'", fieldName, val)
			}
			result := make([]float32, 0, len(sliceVal))
			for _, elem := range sliceVal {
				switch n := elem.(type) {
				case json.Number:
					f64, err := n.Float64()
					if err != nil {
						return fmt.Errorf("%s: failed to cast jsonNumber to float64 '%T': %v", fieldName, n, err)
					}
					result = append(result, float32(f64))
				case float64:
					result = append(result, float32(n))
				default:
					return fmt.Errorf("%s: unsupported element type in vector '%T'", fieldName, elem)
				}
			}
			*d = result
		default:
			return fmt.Errorf("unsupported type to convert (type=%T)", d)
		}
	}

	return nil
}

// Close closes the rows iterator
func (r *msRows) Close() error {
	return nil
}

// wrappedRows wraps msRows with logging support
type wrappedRows struct {
	rows *msRows

	logTime        bool
	readRowsLogger db.Logger
	printed        int
}

const maxRowsToPrint = 10

// Next advances the cursor to the next row
func (r *wrappedRows) Next() bool {
	return r.rows.Next()
}

// Err returns any error that was encountered during iteration
func (r *wrappedRows) Err() error {
	return r.rows.Err()
}

// Scan copies columns and logs the scanned values
func (r *wrappedRows) Scan(dest ...interface{}) error {
	since := time.Now()
	err := r.rows.Scan(dest...)

	if r.readRowsLogger != nil {
		if r.printed >= maxRowsToPrint {
			return err
		} else if r.printed == maxRowsToPrint {
			r.readRowsLogger.Log("... truncated ...")
			r.printed++
			return err
		}

		values := db.DumpRecursive(dest, " ")
		if r.logTime {
			dur := time.Since(since)
			r.readRowsLogger.Log("Row: %s -- %s", values, fmt.Sprintf("parse duration: %v", dur))
		} else {
			r.readRowsLogger.Log("Row: %s", values)
		}
		r.printed++
	}

	return err
}

// Close closes the rows iterator
func (r *wrappedRows) Close() error {
	return r.rows.Close()
}
