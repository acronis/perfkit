package db

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// TernaryStr returns trueVal if cond is true, falseVal otherwise
func TernaryStr(cond bool, trueVal, falseVal string) string {
	if cond {
		return trueVal
	}

	return falseVal
}

// tryCastToString tries to cast given interface to string
func tryCastToString(i interface{}) (string, bool) {
	result := ""
	chars, ok := i.([]uint8)
	if !ok {
		return "", false
	}
	for _, c := range chars {
		if c < 32 || c > 126 {
			return "", false
		}
		result += string(rune(c))
	}

	return "'" + result + "'", true
}

// DumpRecursive returns string representation of given interface
func DumpRecursive(i interface{}, indent string) string {
	val := reflect.ValueOf(i)

	if !val.IsValid() {
		return "nil"
	}

	if !val.CanInterface() {
		return "?"
	}

	typ := val.Type()

	switch val.Kind() {
	case reflect.String:
		return fmt.Sprintf("%q", val.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(val.Uint(), 10)
	case reflect.Bool:
		return strconv.FormatBool(val.Bool())
	case reflect.Slice, reflect.Array:
		var result []string
		for i := 0; i < val.Len(); i++ {
			s, ok := tryCastToString(val.Index(i).Interface())
			if ok {
				result = append(result, s)
			} else {
				result = append(result, DumpRecursive(val.Index(i).Interface(), indent+"  "))
			}
		}

		return "[" + strings.Join(result, ", ") + "]"
	case reflect.Struct:
		var result []string
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			if field.CanInterface() {
				result = append(result, indent+typ.Field(i).Name+" => "+DumpRecursive(val.Field(i).Interface(), indent+"  "))
			} else {
				result = append(result, indent+"??? => ???")
			}
		}

		return strings.Join(result, "\n")
	case reflect.Map:
		keys := val.MapKeys()
		var result []string
		for _, key := range keys {
			result = append(result, indent+fmt.Sprintf("%v", key.Interface())+" => "+DumpRecursive(val.MapIndex(key).Interface(), indent+"  "))
		}

		return strings.Join(result, "\n")
	case reflect.Ptr:
		switch typ.Elem().Kind() {
		case reflect.String:
			return fmt.Sprintf("%s", i)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			return strconv.Itoa(int(*i.(*int32)))
		case reflect.Int64:
			return strconv.FormatInt(*i.(*int64), 10)
		default:
			return fmt.Sprintf("%v", val.Interface())
		}

	default:
		return fmt.Sprintf("%v", val.Interface())
	}
}

// DefaultCreateQueryPatchFunc returns function that replaces placeholders in query with values from given table, sql_driver and sql_engine
func DefaultCreateQueryPatchFunc(table string, query string, dialect Dialect) (string, error) {
	query = strings.ReplaceAll(query, "{table}", table)

	for _, logicalType := range []DataType{
		DataTypeBigIntAutoIncPK,
		DataTypeBigIntAutoInc,
		DataTypeAscii,
		DataTypeUUID,
		DataTypeVarCharUUID,
		DataTypeTenantUUIDBoundID,
		DataTypeLongBlob,
		DataTypeHugeBlob,
		DataTypeDateTime,
		DataTypeDateTime6,
		DataTypeTimestamp6,
		DataTypeCurrentTimeStamp6,
		DataTypeBinary20,
		DataTypeBinaryBlobType,
		DataTypeBoolean,
		DataTypeBooleanFalse,
		DataTypeBooleanTrue,
		DataTypeTinyInt,
		DataTypeLongText,
		DataTypeUnique,
		DataTypeNotNull,
		DataTypeNull,
		DataTypeVector3Float32,
		DataTypeEngine,
	} {
		var specificType = dialect.GetType(logicalType)
		query = strings.ReplaceAll(query, string(logicalType), specificType)
	}

	return query, nil
}

var kb = int64(1024)

// StringToBytes converts string to bytes
func StringToBytes(str string) (int64, error) {
	multipliers := map[string]int64{
		"K":  kb,
		"KB": kb,
		"M":  kb * kb,
		"MB": kb * kb,
		"G":  kb * kb * kb,
		"GB": kb * kb * kb,
		"T":  kb * kb * kb * kb,
		"TB": kb * kb * kb * kb,
		"P":  kb * kb * kb * kb * kb,
		"PB": kb * kb * kb * kb * kb,
	}

	if str == "" {
		return 0, fmt.Errorf("empty string") //nolint:perfsprint
	}

	for suffix, multiplier := range multipliers {
		if strings.HasSuffix(str, suffix) {
			s := str[:len(str)-len(suffix)]
			number, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("error parsing '%v': %w", s, err)
			}

			return number * multiplier, nil
		}
	}

	number, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing '%v': %w", str, err)
	}

	return number, nil
}

// PrintStack prints stack trace
func PrintStack() {
	var buf [4096]byte
	n := runtime.Stack(buf[:], false)
	fmt.Printf("=== STACK TRACE ===\n%s\n", buf[:n])
}

// WithAutoInc returns true if DBDriver should support 'autoinc' field as current time nanoseconds
func WithAutoInc(name DialectName) bool {
	switch name {
	case CASSANDRA:
		return true
	default:
		return false
	}
}

func DedupStrings(ss []string) []string {
	var idx = map[string]struct{}{}
	var dd []string
	for _, s := range ss {
		if _, ok := idx[s]; !ok {
			idx[s] = struct{}{}
			dd = append(dd, s)
		}
	}

	return dd
}

func ParseFunc(s string) (fName string, arg string, err error) {
	argOpen, argClose := strings.Index(s, "("), strings.Index(s, ")")
	if argOpen == -1 && argClose == -1 {
		return "", s, nil
	}
	if argOpen == -1 {
		return "", "", fmt.Errorf("bad function '%v', no opening bracket", s)
	}
	if argClose == -1 {
		return "", "", fmt.Errorf("bad function '%v', no closing bracket", s)
	}

	if argClose <= argOpen {
		return "", "", fmt.Errorf("bad function '%v', closing bracket placed before opening bracket", s)
	}

	return s[:argOpen], s[argOpen+1 : argClose], nil
}

func ParseFuncMultipleArgs(s string, sep string) (fName string, args []string, err error) {
	argOpen, argClose := strings.Index(s, "("), strings.Index(s, ")")
	if argOpen == -1 && argClose == -1 {
		return "", strings.Split(s, sep), nil
	}
	if argOpen == -1 {
		return "", nil, fmt.Errorf("bad function '%v', no opening bracket", s)
	}
	if argClose == -1 {
		return "", nil, fmt.Errorf("bad function '%v', no closing bracket", s)
	}

	if argClose <= argOpen {
		return "", nil, fmt.Errorf("bad function '%v', closing bracket placed before opening bracket", s)
	}

	return s[:argOpen], strings.Split(s[argOpen+1:argClose], sep), nil
}

func ParseVector(s string, sep string) (args []string, err error) {
	argOpen, argClose := strings.Index(s, "["), strings.Index(s, "]")
	if argOpen == -1 && argClose == -1 {
		return strings.Split(s, sep), nil
	}
	if argOpen == -1 {
		return nil, fmt.Errorf("bad vector '%v', no opening bracket", s)
	}
	if argClose == -1 {
		return nil, fmt.Errorf("bad vector '%v', no closing bracket", s)
	}

	if argClose <= argOpen {
		return nil, fmt.Errorf("bad function '%v', closing bracket placed before opening bracket", s)
	}

	return strings.Split(s[argOpen+1:argClose], sep), nil
}

func ParseTimeUTC(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time value")
	}

	if strings.HasSuffix(s, "ns") {
		ns, err := strconv.ParseInt(strings.TrimSuffix(s, "ns"), 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("incorrect UNIX-TIMESTAMP-NANO format")
		}

		return time.Unix(0, ns).UTC(), nil
	}

	sec, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return time.Unix(sec, 0).UTC(), nil
	}

	var t time.Time
	if t, err = time.Parse(time.RFC3339, s); err != nil {
		if t, err = time.Parse(time.RFC1123, s); err != nil {
			if t, err = time.Parse(time.RFC850, s); err != nil {
				if t, err = time.Parse(time.ANSIC, s); err != nil {
					return time.Time{}, fmt.Errorf("incorrect time format, must be one of (UNIX-TIMESTAMP-NANO, UNIX-TIMESTAMP, RFC3339, RFC1123, RFC850, ANSI-C)")
				}
			}
		}
	}

	return t.UTC(), nil
}

func ParseScheme(s string) (scheme string, uri string, err error) {
	const schemeSeparator = "://"
	parts := strings.Split(s, schemeSeparator)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("'%s' is invalid scheme separator", schemeSeparator)
	}

	return parts[0], parts[1], nil
}

// Cond represents a condition
type Cond struct {
	Col  string
	Vals []string
}

// SortFields sorts fields by column name
func SortFields(fields map[string][]string) []Cond {
	var cs []Cond
	for k, v := range fields {
		cs = append(cs, Cond{k, v})
	}

	sort.Slice(cs, func(i, j int) bool {
		return cs[i].Col < cs[j].Col
	})

	return cs
}

// GenDBParameterPlaceholders generates placeholders for given start and count
func GenDBParameterPlaceholders(start int, count int) string {
	var ret = make([]string, count)
	end := start + count
	for i := start; i < end; i++ {
		ret[i-start] = fmt.Sprintf("$%d", i+1)
	}

	return strings.Join(ret, ",")
}
