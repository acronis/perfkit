package db

import (
	"fmt"
	"reflect"
	"strconv"
)

type Row interface {
	Scan(dest ...any) error
}

// EmptyRow is a struct for storing empty row
type EmptyRow struct{}

func (r *EmptyRow) Scan(dest ...any) error { return nil }

// Rows is a struct for storing DB rows (as a slice of Row) and current index
type Rows interface {
	Next() bool
	Err() error
	Scan(dest ...interface{}) error
	Close() error
}

// EmptyRows is a struct for storing DB rows (as a slice of dbRow) and current index
type EmptyRows struct{}

func (r *EmptyRows) Next() bool                     { return false }
func (r *EmptyRows) Err() error                     { return nil }
func (r *EmptyRows) Scan(dest ...interface{}) error { return nil }
func (r *EmptyRows) Close() error                   { return nil }

// CountRows is a struct for storing DB rows (as a slice of dbRow) and current index
type CountRows struct {
	Count int64
	read  bool

	readRowsLogger Logger
}

func (r *CountRows) Next() bool {
	if !r.read {
		r.read = true

		return true
	}

	return false
}
func (r *CountRows) Err() error { return nil }
func (r *CountRows) Scan(dest ...interface{}) error {
	if len(dest) != 1 {
		return fmt.Errorf("internal error: CountRows.Scan() - number of columns in the result set does not match the number of destination fields")
	}

	dv := reflect.ValueOf(dest[0])
	if dv.Kind() != reflect.Ptr {
		return fmt.Errorf("internal error: CountRows.Scan() - non-pointer passed to Scan: %v", dest)
	}

	var val = r.Count

	switch d := dest[0].(type) {
	case *int64:
		*d = val
	default:
		return fmt.Errorf("unsupported type to convert (type=%T)", d)
	}

	if r.readRowsLogger != nil {
		r.readRowsLogger.Log("Row: %d", val)
	}

	return nil
}
func (r *CountRows) Close() error { return nil }

type surrogateRowsRow []interface{}

// SurrogateRows is a struct for storing DB rows (as a slice of dbRow) and current index
type SurrogateRows struct {
	data []surrogateRowsRow
	idx  int
}

// Next implements sql.Rows interface for DBRows struct (used in tests)
func (r *SurrogateRows) Next() bool {
	if r.idx < len(r.data) {
		r.idx++

		return true
	}

	return false
}

func (r *SurrogateRows) Err() error {
	return nil
}

// Scan implements sql.Rows interface for DBRows struct (used in tests)
// and returns error if conversion is not implemented
func (r *SurrogateRows) Scan(dest ...interface{}) error {
	row := r.data[r.idx-1]

	for i := range row {
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Ptr {
			return fmt.Errorf("internal error: DBRows.Scan() - non-pointer passed to Scan: %v", dest)
		}
		sv := reflect.ValueOf(row[i])

		if !sv.IsValid() {
			switch dv.Elem().Kind() {
			case reflect.String:
				dv.Elem().SetString("")

				continue
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				dv.Elem().SetInt(0)

				continue
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				dv.Elem().SetUint(0)

				continue
			case reflect.Ptr, reflect.Interface:
				dv.Elem().Set(reflect.Zero(dv.Elem().Type()))

				continue
			default:
				return fmt.Errorf("unsupported type: %v", dv.Elem().Kind())
			}
		} else if sv.Kind() == reflect.Slice && sv.Type().Elem().Kind() == reflect.Uint8 {
			s := string(sv.Interface().([]uint8))
			switch dv.Elem().Kind() {
			case reflect.String:
				dv.Elem().SetString(s)

				continue
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				i64, err := strconv.ParseInt(s, 10, dv.Elem().Type().Bits())
				if err != nil {
					return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", sv, s, dv.Kind(), err)
				}
				dv.Elem().SetInt(i64)

				continue
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				u64, err := strconv.ParseUint(s, 10, dv.Elem().Type().Bits())
				if err != nil {
					return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", sv, s, dv.Kind(), err)
				}
				dv.Elem().SetUint(u64)

				continue
			case reflect.Interface:
				dv.Elem().Set(sv)

				continue
			default:
				return fmt.Errorf("unsupported type: %v", dv.Elem().Kind())
			}
		} else if sv.Kind() == reflect.String && dv.Elem().Kind() == reflect.String {
			dv.Elem().SetString(sv.Interface().(string))

			continue
		} else if sv.Kind() == reflect.Int64 && dv.Elem().Kind() == reflect.Int {
			dv.Elem().SetInt(sv.Int())

			continue
		} else if sv.Kind() == reflect.Int8 && dv.Elem().Kind() == reflect.Int {
			dv.Elem().SetInt(sv.Int())

			continue
			// } else if sv.Kind() == reflect.TypeOf(TenantUUID("")).Kind() && dv.Elem().Kind() == reflect.String {
			// 	dv.Elem().SetString(sv.Interface().(string))

			//	continue
		} else if sv.Type().AssignableTo(dv.Elem().Type()) {
			dv.Elem().Set(sv)

			continue
		}
		PrintStack()
		// fmt.Printf("xxx internal error: DBRows.Scan() - convertion of '%v' (%v) to '%v' is not implemented yet\n", sv.Kind(), reflect.TypeOf(TenantUUID("")).Kind(), dv.Elem().Kind())

		return fmt.Errorf("internal error: DBRows.Scan() - convertion of '%v' to '%v' is not implemented yet", sv.Kind(), dv.Elem().Kind())
	}

	return nil
}

// Close implements sql.Rows interface for DBRows struct (used in tests)
func (r *SurrogateRows) Close() error {
	return nil
}
