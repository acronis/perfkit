package benchmark

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type dbRow []interface{}

// DBRows is a struct for storing DB rows (as a slice of dbRow) and current index
type DBRows struct {
	data []dbRow
	idx  int
}

// Scan implements sql.Rows interface for DBRows struct (used in tests)
// and returns error if conversion is not implemented
func (r *DBRows) Scan(dest ...interface{}) error {
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
		} else if sv.Kind() == reflect.TypeOf(TenantUUID("")).Kind() && dv.Elem().Kind() == reflect.String {
			dv.Elem().SetString(sv.Interface().(string))

			continue
		} else if sv.Type().AssignableTo(dv.Elem().Type()) {
			dv.Elem().Set(sv)

			continue
		}
		printStack()
		fmt.Printf("xxx internal error: DBRows.Scan() - convertion of '%v' (%v) to '%v' is not implemented yet\n", sv.Kind(), reflect.TypeOf(TenantUUID("")).Kind(), dv.Elem().Kind())

		return fmt.Errorf("internal error: DBRows.Scan() - convertion of '%v' to '%v' is not implemented yet", sv.Kind(), dv.Elem().Kind())
	}

	return nil
}

// Next implements sql.Rows interface for DBRows struct (used in tests)
func (r *DBRows) Next() bool {
	if r.idx < len(r.data) {
		r.idx++

		return true
	}

	return false
}

// Close implements sql.Rows interface for DBRows struct (used in tests)
func (r *DBRows) Close() error {
	return nil
}

// Dump returns string representation of DBRows struct (used in tests)
func (r *DBRows) Dump() string {
	ret := make([]string, 0, len(r.data))
	for n, row := range r.data {
		if n > 10 {
			// do not flood the logs
			ret = append(ret, "... truncated ...")

			break
		}
		ret = append(ret, DumpRecursive(row, " "))
	}

	return "[" + strings.Join(ret, ", ") + "]"
}
