package sql

import (
	"testing"

	"github.com/acronis/perfkit/db"
)

func TestDefaultCreateQueryPatchFuncWithMySQL(t *testing.T) {
	var table = "test_table"
	var query = "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"

	var dia = &sqlDialect{dia: &mysqlDialect{
		sqlEngine: "xpand-allnodes",
	}}

	var result, err = db.DefaultCreateQueryPatchFunc(table, query, dia)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	var expected = "CREATE TABLE test_table (id BIGINT AUTO_INCREMENT PRIMARY KEY, name character set ascii)"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}

func TestDefaultCreateQueryPatchFuncWithSQLite(t *testing.T) {
	var table = "test_table"
	var query = "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"

	var dia = &sqlDialect{dia: &sqliteDialect{}}

	var result, err = db.DefaultCreateQueryPatchFunc(table, query, dia)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	var expected = "CREATE TABLE test_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name )"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}

func TestDefaultCreateQueryPatchFuncWithPostgres(t *testing.T) {
	var table = "test_table"
	var query = "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"

	var dia = &sqlDialect{dia: &pgDialect{}}

	var result, err = db.DefaultCreateQueryPatchFunc(table, query, dia)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	var expected = "CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name )"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}

func TestDefaultCreateQueryPatchFuncWithMSSQL(t *testing.T) {
	var table = "test_table"
	var query = "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"

	var dia = &sqlDialect{dia: &msDialect{}}

	var result, err = db.DefaultCreateQueryPatchFunc(table, query, dia)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	var expected = "CREATE TABLE test_table (id BIGINT IDENTITY(1,1) PRIMARY KEY, name )"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}

func TestDefaultCreateQueryPatchFuncWithCassandra(t *testing.T) {
	var table = "test_table"
	var query = "CREATE TABLE {table} (id {$bigint_autoinc_pk}, name {$ascii})"

	var dia = &sqlDialect{dia: &cassandraDialect{}}

	var result, err = db.DefaultCreateQueryPatchFunc(table, query, dia)

	if err != nil {
		t.Errorf("DefaultCreateQueryPatchFunc() error = %v", err)

		return
	}

	var expected = "CREATE TABLE test_table (id bigint PRIMARY KEY, name )"
	if result != expected {
		t.Errorf("DefaultCreateQueryPatchFunc() got = %v, want %v", result, expected)
	}
}
