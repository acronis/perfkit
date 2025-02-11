package es

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Rows is a struct for storing DB rows (as a slice of Row) and current index
type esRows struct {
	data []map[string]interface{}
	idx  int

	requestedColumns []string
}

// Next implements sql.Rows interface for DBRows struct (used in tests)
func (r *esRows) Next() bool {
	if r.idx < len(r.data) {
		r.idx++

		return true
	}

	return false
}

func (r *esRows) Err() error {
	return nil
}

func (r *esRows) Scan(dest ...interface{}) error {
	if len(dest) != len(r.requestedColumns) {
		return fmt.Errorf("number of columns in the result set does not match the number of destination fields")
	}

	var row = r.data[r.idx-1]

	for i := range dest {
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Ptr {
			return fmt.Errorf("internal error: esRows.Scan() - non-pointer passed to Scan: %v", dest)
		}

		var esFieldName = r.requestedColumns[i]
		var val = row[esFieldName]

		if slcVal, ok := val.([]interface{}); !ok {
			return fmt.Errorf("%s is service field", esFieldName)
		} else if len(slcVal) == 1 {
			val = slcVal[0] // we will have no other field element
		}

		switch d := dest[i].(type) {
		case *string:
			strVal, ok := val.(string)
			if !ok {
				return fmt.Errorf("%s : not equal type in struct 'string', in map '%T'", esFieldName, val)
			}
			*d = strVal
		case *int64:
			switch numberType := val.(type) {
			case json.Number:
				var err error
				*d, err = numberType.Int64()
				if err != nil {
					return fmt.Errorf("%s : failed to cast jsonNumber to int64 '%T': %v", esFieldName, numberType, err)
				}
			case float64:
				*d = int64(numberType)
			default:
				return fmt.Errorf("%s : not equal type, in map '%T'", esFieldName, val)
			}

		case *bool:
			boolVal, ok := val.(bool)
			if !ok {
				return fmt.Errorf("%s : not equal type in struct 'bool', in map '%T'", esFieldName, val)
			}
			*d = boolVal
		case *[]byte:
			strVal, ok := val.(string)
			if !ok {
				return fmt.Errorf("%s : not equal type in struct '[]byte', in map '%T'", esFieldName, val)
			}
			*d = []byte(strVal)
		case *[]string:
			strVal, ok := val.(string)
			if ok {
				*d = []string{strVal}
				continue
			}

			slcVal, ok := val.([]interface{})
			if !ok {
				return fmt.Errorf("%s : not equal type in struct '[]interface{}', in map '%T'", esFieldName, val)
			}
			strSlcVal := make([]string, len(slcVal))
			for j, v := range slcVal {
				strSlcVal[j] = fmt.Sprint(v)
			}
			*d = strSlcVal
		case *time.Time:
			strVal, ok := val.(string)
			if !ok {
				return fmt.Errorf("%s : not equal type in struct 'time.Time', in map '%T'", esFieldName, val)
			}
			var err error
			*d, err = time.Parse(timeStoreFormatPrecise, strVal)
			if err != nil {
				return fmt.Errorf("%s : failed to cast string to time.Time '%T': %v", esFieldName, strVal, err)
			}
		case *[]float32:
			var sliceType, ok = val.([]interface{})
			if !ok {
				return fmt.Errorf("%s : not equal type in struct '[]float32', in map '%T'", esFieldName, val)
			}

			for _, valInSlice := range sliceType {
				switch numberType := valInSlice.(type) {
				case json.Number:
					if fl64, err := numberType.Float64(); err != nil {
						return fmt.Errorf("%s : failed to cast jsonNumber to int64 '%T': %v", esFieldName, numberType, err)
					} else {
						*d = append(*d, float32(fl64))
					}
				case float64:
					*d = append(*d, float32(numberType))
				default:
					return fmt.Errorf("%s : not equal type, in map '%T'", esFieldName, val)
				}
			}
		default:
			return fmt.Errorf("unsupported type to convert (type=%T)", d)
		}
	}

	return nil
}

func (r *esRows) Close() error {
	return nil
}
