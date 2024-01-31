package benchmark

import (
	"fmt"
	"html/template"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

// FatalError prints error message and exits with code 127
func FatalError(err string) {
	fmt.Printf("fatal error: %v", err)
	os.Exit(127)
}

// getTemplate returns template.Template object from given template string
func getTemplate(name, templateString string) *template.Template { //nolint:unused
	return template.Must(template.New(name).Parse(templateString))
}

// replaceAll replaces all keys in template with values from data map
func replaceAll(t *template.Template, data map[string]interface{}) string { //nolint:unused
	builder := &strings.Builder{}
	if err := t.Execute(builder, data); err != nil {
		FatalError(err.Error())
	}

	return builder.String()
}

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
func DefaultCreateQueryPatchFunc(table string, query string, sqlDriver string, sqlEngine string) (string, error) {
	query = strings.ReplaceAll(query, "{table}", table)
	switch sqlDriver {
	case MYSQL:
		query = strings.ReplaceAll(query, "{$bigint_autoinc_pk}", "BIGINT AUTO_INCREMENT PRIMARY KEY")
		query = strings.ReplaceAll(query, "{$bigint_autoinc}", "BIGINT AUTO_INCREMENT")
		query = strings.ReplaceAll(query, "{$ascii}", "character set ascii")
		query = strings.ReplaceAll(query, "{$uuid}", "VARCHAR(36)")
		query = strings.ReplaceAll(query, "{$varchar_uuid}", "VARCHAR(36)")
		query = strings.ReplaceAll(query, "{$tenant_uuid_bound_id}", "VARCHAR(64)")
		query = strings.ReplaceAll(query, "{$longblob}", "LONGBLOB")
		query = strings.ReplaceAll(query, "{$hugeblob}", "LONGBLOB")
		query = strings.ReplaceAll(query, "{$datetime}", "DATETIME")
		query = strings.ReplaceAll(query, "{$datetime6}", "DATETIME(6)")
		query = strings.ReplaceAll(query, "{$timestamp6}", "TIMESTAMP(6)")
		query = strings.ReplaceAll(query, "{$current_timestamp6}", "CURRENT_TIMESTAMP(6)")
		query = strings.ReplaceAll(query, "{$binary20}", "BINARY(20)")
		query = strings.ReplaceAll(query, "{$binaryblobtype}", "MEDIUMBLOB")
		query = strings.ReplaceAll(query, "{$boolean}", "BOOLEAN")
		query = strings.ReplaceAll(query, "{$boolean_false}", "0")
		query = strings.ReplaceAll(query, "{$boolean_true}", "1")
		query = strings.ReplaceAll(query, "{$tinyint}", "TINYINT")
		query = strings.ReplaceAll(query, "{$longtext}", "LONGTEXT")
		query = strings.ReplaceAll(query, "{$unique}", "unique")
		query = strings.ReplaceAll(query, "{$notnull}", "not null")
		query = strings.ReplaceAll(query, "{$null}", "null")
		if sqlEngine == "xpand-allnodes" {
			query = strings.ReplaceAll(query, "{$engine}", "engine = xpand")
		} else {
			query = strings.ReplaceAll(query, "{$engine}", "engine = "+sqlEngine)
		}
	case SQLITE, SQLITE3:
		query = strings.ReplaceAll(query, "{$bigint_autoinc_pk}", "INTEGER PRIMARY KEY AUTOINCREMENT")
		query = strings.ReplaceAll(query, "{$bigint_autoinc}", "INTEGER AUTOINCREMENT")
		query = strings.ReplaceAll(query, "{$ascii}", "")
		query = strings.ReplaceAll(query, "{$uuid}", "TEXT")
		query = strings.ReplaceAll(query, "{$varchar_uuid}", "TEXT")
		query = strings.ReplaceAll(query, "{$longblob}", "BLOB")
		query = strings.ReplaceAll(query, "{$hugeblob}", "BLOB")
		query = strings.ReplaceAll(query, "{$datetime}", "TEXT")
		query = strings.ReplaceAll(query, "{$datetime6}", "TEXT")
		query = strings.ReplaceAll(query, "{$timestamp6}", "TEXT")
		query = strings.ReplaceAll(query, "{$current_timestamp6}", "CURRENT_TIMESTAMP")
		query = strings.ReplaceAll(query, "{$binary20}", "BLOB")
		query = strings.ReplaceAll(query, "{$binaryblobtype}", "MEDIUMBLOB")
		query = strings.ReplaceAll(query, "{$boolean}", "BOOLEAN")
		query = strings.ReplaceAll(query, "{$boolean_false}", "0")
		query = strings.ReplaceAll(query, "{$boolean_true}", "1")
		query = strings.ReplaceAll(query, "{$tinyint}", "SMALLINT")
		query = strings.ReplaceAll(query, "{$longtext}", "TEXT")
		query = strings.ReplaceAll(query, "{$unique}", "unique")
		query = strings.ReplaceAll(query, "{$engine}", "")
		query = strings.ReplaceAll(query, "{$notnull}", "not null")
		query = strings.ReplaceAll(query, "{$null}", "null")
		query = strings.ReplaceAll(query, "{$tenant_uuid_bound_id}", "TEXT")
	case MSSQL:
		query = strings.ReplaceAll(query, "{$bigint_autoinc_pk}", "BIGINT IDENTITY(1,1) PRIMARY KEY")
		query = strings.ReplaceAll(query, "{$bigint_autoinc}", "BIGINT IDENTITY(1,1)")
		query = strings.ReplaceAll(query, "{$ascii}", "")
		query = strings.ReplaceAll(query, "{$uuid}", "UNIQUEIDENTIFIER")
		query = strings.ReplaceAll(query, "{$varchar_uuid}", "VARCHAR(36)")
		query = strings.ReplaceAll(query, "{$longblob}", "VARCHAR(MAX)")
		query = strings.ReplaceAll(query, "{$hugeblob}", "VARBINARY(MAX)")
		query = strings.ReplaceAll(query, "{$datetime}", "DATETIME")
		query = strings.ReplaceAll(query, "{$datetime6}", "DATETIME2(6)")
		query = strings.ReplaceAll(query, "{$timestamp6}", "DATETIME2(6)")
		query = strings.ReplaceAll(query, "{$current_timestamp6}", "SYSDATETIME()")
		query = strings.ReplaceAll(query, "{$binary20}", "BINARY(20)")
		query = strings.ReplaceAll(query, "{$binaryblobtype}", "varbinary(max)")
		query = strings.ReplaceAll(query, "{$boolean}", "BIT")
		query = strings.ReplaceAll(query, "{$boolean_false}", "0")
		query = strings.ReplaceAll(query, "{$boolean_true}", "1")
		query = strings.ReplaceAll(query, "{$tinyint}", "TINYINT")
		query = strings.ReplaceAll(query, "{$longtext}", "NVARCHAR(MAX)")
		query = strings.ReplaceAll(query, "{$unique}", "unique")
		query = strings.ReplaceAll(query, "{$engine}", "")
		query = strings.ReplaceAll(query, "{$notnull}", "not null")
		query = strings.ReplaceAll(query, "{$null}", "null")
		query = strings.ReplaceAll(query, "{$tenant_uuid_bound_id}", "VARCHAR(64)")
	case POSTGRES:
		query = strings.ReplaceAll(query, "{$bigint_autoinc_pk}", "BIGSERIAL PRIMARY KEY")
		query = strings.ReplaceAll(query, "{$bigint_autoinc}", "BIGSERIAL")
		query = strings.ReplaceAll(query, "{$ascii}", "")
		query = strings.ReplaceAll(query, "{$uuid}", "UUID")
		query = strings.ReplaceAll(query, "{$varchar_uuid}", "VARCHAR(36)")
		query = strings.ReplaceAll(query, "{$longblob}", "BYTEA")
		query = strings.ReplaceAll(query, "{$hugeblob}", "BYTEA")
		query = strings.ReplaceAll(query, "{$datetime}", "TIMESTAMP")
		query = strings.ReplaceAll(query, "{$datetime6}", "TIMESTAMP(6)")
		query = strings.ReplaceAll(query, "{$timestamp6}", "TIMESTAMP(6)")
		query = strings.ReplaceAll(query, "{$current_timestamp6}", "CURRENT_TIMESTAMP(6)")
		query = strings.ReplaceAll(query, "{$binary20}", "BYTEA")
		query = strings.ReplaceAll(query, "{$binaryblobtype}", "BYTEA")
		query = strings.ReplaceAll(query, "{$boolean}", "BOOLEAN")
		query = strings.ReplaceAll(query, "{$boolean_false}", "false")
		query = strings.ReplaceAll(query, "{$boolean_true}", "true")
		query = strings.ReplaceAll(query, "{$tinyint}", "SMALLINT")
		query = strings.ReplaceAll(query, "{$longtext}", "TEXT")
		query = strings.ReplaceAll(query, "{$unique}", "unique")
		query = strings.ReplaceAll(query, "{$engine}", "")
		query = strings.ReplaceAll(query, "{$notnull}", "not null")
		query = strings.ReplaceAll(query, "{$null}", "null")
		query = strings.ReplaceAll(query, "{$tenant_uuid_bound_id}", "VARCHAR(64)")
	case CLICKHOUSE:
		query = strings.ReplaceAll(query, "{$bigint_autoinc_pk}", "UInt64")     // ClickHouse does not support auto-increment
		query = strings.ReplaceAll(query, "{$bigint_autoinc}", "UInt64")        // Use UInt64 for large integers
		query = strings.ReplaceAll(query, "{$ascii}", "")                       // Charset specification is not needed in ClickHouse
		query = strings.ReplaceAll(query, "{$uuid}", "UUID")                    // ClickHouse supports UUID type
		query = strings.ReplaceAll(query, "{$varchar_uuid}", "FixedString(36)") // ClickHouse supports UUID type
		query = strings.ReplaceAll(query, "{$longblob}", "String")              // Use String for binary data
		query = strings.ReplaceAll(query, "{$hugeblob}", "String")              // Use String for binary data
		query = strings.ReplaceAll(query, "{$datetime}", "DateTime")            // DateTime type for date and time
		query = strings.ReplaceAll(query, "{$datetime6}", "DateTime64(6)")      // DateTime64 with precision for fractional seconds
		query = strings.ReplaceAll(query, "{$timestamp6}", "DateTime64(6)")     // DateTime64 for timestamp with fractional seconds
		query = strings.ReplaceAll(query, "{$current_timestamp6}", "now64(6)")  // Function for current timestamp
		query = strings.ReplaceAll(query, "{$binary20}", "FixedString(20)")     // FixedString for fixed-length binary data
		query = strings.ReplaceAll(query, "{$binaryblobtype}", "String")        // Use String for binary data
		query = strings.ReplaceAll(query, "{$boolean}", "UInt8")                // ClickHouse uses UInt8 for boolean values
		query = strings.ReplaceAll(query, "{$boolean_false}", "0")
		query = strings.ReplaceAll(query, "{$boolean_true}", "1")
		query = strings.ReplaceAll(query, "{$tinyint}", "Int8")    // Int8 for small integers
		query = strings.ReplaceAll(query, "{$longtext}", "String") // Use String for long text
		query = strings.ReplaceAll(query, "{$unique}", "")         // Unique values are not supported
		query = strings.ReplaceAll(query, "{$engine}", "ENGINE = MergeTree() ORDER BY id;")
		query = strings.ReplaceAll(query, "{$notnull}", "not null")
		query = strings.ReplaceAll(query, "{$null}", "null")
		query = strings.ReplaceAll(query, "{$tenant_uuid_bound_id}", "String")
	case CASSANDRA:
		query = strings.ReplaceAll(query, "{$bigint_autoinc_pk}", "bigint PRIMARY KEY") // Cassandra does not support auto-increment, bigint is closest
		query = strings.ReplaceAll(query, "{$bigint_autoinc}", "bigint")                // Use bigint for large integers
		query = strings.ReplaceAll(query, "{$ascii}", "")                               // Charset specification is not needed in Cassandra
		query = strings.ReplaceAll(query, "{$uuid}", "UUID")                            // Cassandra supports UUID type
		query = strings.ReplaceAll(query, "{$varchar_uuid}", "varchar")                 // Cassandra supports UUID type
		query = strings.ReplaceAll(query, "{$longblob}", "blob")                        // Use blob for binary data
		query = strings.ReplaceAll(query, "{$hugeblob}", "blob")                        // Use blob for binary data
		query = strings.ReplaceAll(query, "{$datetime}", "timestamp")                   // DateTime type for date and time
		query = strings.ReplaceAll(query, "{$datetime6}", "timestamp with time zone")   // Timestamp with time zone
		query = strings.ReplaceAll(query, "{$timestamp6}", "timestamp with time zone")  // Timestamp with time zone
		query = strings.ReplaceAll(query, "{$current_timestamp6}", "now()")             // Function for current timestamp
		query = strings.ReplaceAll(query, "{$binary20}", "blob")                        // varchar for fixed-length binary data
		query = strings.ReplaceAll(query, "{$binaryblobtype}", "blob")                  // Use blob for binary data
		query = strings.ReplaceAll(query, "{$boolean}", "boolean")
		query = strings.ReplaceAll(query, "{$boolean_false}", "false")
		query = strings.ReplaceAll(query, "{$boolean_true}", "true")
		query = strings.ReplaceAll(query, "{$tinyint}", "tinyint")
		query = strings.ReplaceAll(query, "{$longtext}", "text") // Use text for long text
		query = strings.ReplaceAll(query, "{$unique}", "")       // Unique values are not supported
		query = strings.ReplaceAll(query, "{$engine}", "")
		query = strings.ReplaceAll(query, "{$notnull}", "")
		query = strings.ReplaceAll(query, "{$null}", "")
		query = strings.ReplaceAll(query, "{$tenant_uuid_bound_id}", "varchar")
	default:
		return "", fmt.Errorf("unsupported driver: '%v', supported drivers are: postgres|sqlite|mysql|mssql", sqlDriver)
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
		return 0, fmt.Errorf("empty string")
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

// printStack prints stack trace
func printStack() {
	var buf [4096]byte
	n := runtime.Stack(buf[:], false)
	fmt.Printf("=== STACK TRACE ===\n%s\n", buf[:n])
}

// WithAutoInc returns true if DBDriver should support 'autoinc' field as current time nanoseconds
func WithAutoInc(DBDriver string) bool {
	switch DBDriver {
	case CASSANDRA:
		return true
	default:
		return false
	}
}
