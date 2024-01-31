package benchmark

import (
	"testing"
)

func TestDefaultCreateQueryPatchFuncWithMySQL(t *testing.T) {
	table := "test_table"
	query := "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"
	sqlDriver := "mysql"
	sqlEngine := "xpand-allnodes"

	result, err := DefaultCreateQueryPatchFunc(table, query, sqlDriver, sqlEngine)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	expected := "CREATE TABLE test_table (id BIGINT AUTO_INCREMENT PRIMARY KEY, name character set ascii)"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}

func TestDefaultCreateQueryPatchFuncWithUnsupportedDriver(t *testing.T) {
	table := "test_table"
	query := "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"
	sqlDriver := "UNSUPPORTED"
	sqlEngine := "xpand-allnodes"

	_, err := DefaultCreateQueryPatchFunc(table, query, sqlDriver, sqlEngine)

	if err == nil {
		t.Errorf("DefaultCreateQueryPatchFunc() expected error, got nil")
	}
}

func TestDefaultCreateQueryPatchFuncWithSQLite(t *testing.T) {
	table := "test_table"
	query := "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"
	sqlDriver := "sqlite3"
	sqlEngine := "xpand-allnodes"

	result, err := DefaultCreateQueryPatchFunc(table, query, sqlDriver, sqlEngine)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	expected := "CREATE TABLE test_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name )"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}

func TestDefaultCreateQueryPatchFuncWithPostgres(t *testing.T) {
	table := "test_table"
	query := "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"
	sqlDriver := "postgres"
	sqlEngine := "xpand-allnodes"

	result, err := DefaultCreateQueryPatchFunc(table, query, sqlDriver, sqlEngine)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	expected := "CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name )"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}

func TestDefaultCreateQueryPatchFuncWithMSSQL(t *testing.T) {
	table := "test_table"
	query := "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"
	sqlDriver := "mssql"
	sqlEngine := "xpand-allnodes"

	result, err := DefaultCreateQueryPatchFunc(table, query, sqlDriver, sqlEngine)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	expected := "CREATE TABLE test_table (id BIGINT IDENTITY(1,1) PRIMARY KEY, name )"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}

func TestDefaultCreateQueryPatchFuncWithCassandra(t *testing.T) {
	table := "test_table"
	query := "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"
	sqlDriver := "cassandra"
	sqlEngine := "xpand-allnodes"

	result, err := DefaultCreateQueryPatchFunc(table, query, sqlDriver, sqlEngine)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	expected := "CREATE TABLE test_table (id bigint PRIMARY KEY, name )"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}
