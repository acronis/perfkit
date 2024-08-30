package db

import "testing"

func TestSchemeParse(t *testing.T) {
	mysql, _, err := ParseScheme("mysql://perfkit_user:truepassword@tcp(192.168.0.1:3306)/perf_db")
	if err != nil {
		t.Error("parse error", err)
		return
	}

	if mysql != "mysql" {
		t.Error("wrong scheme")
		return
	}

	_, _, err = ParseScheme("mysql:/perfkit_user:truepassword@tcp(192.168.0.1:3306)/perf_db")
	if err == nil {
		t.Error("parse error", err)
	}
}
