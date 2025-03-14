package engine

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestConstructConnStringFromOpts(t *testing.T) {
	type testConstructConnString struct {
		connString  string
		driver      string
		dsn         string
		expected    string
		expectedErr error
	}

	os.Setenv("ACRONIS_DB_BENCH_CONNECTION_STRING", "")

	tests := []testConstructConnString{
		{
			connString:  "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable", // example value of a secret
			expected:    "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable", // example value of a secret
			expectedErr: nil,
		},
		{
			driver:      "postgres",
			dsn:         "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable", // example value of a secret
			expected:    "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable", // example value of a secret
			expectedErr: nil,
		},
		{
			driver:      "postgres",
			dsn:         "host=localhost port=5432 user=<USER> password=<PASSWORD> dbname=<DATABASE_NAME> sslmode=disable", // example value of a secret
			expected:    "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable",         // example value of a secret
			expectedErr: nil,
		},
		{
			connString:  "mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			expected:    "mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			driver:      "mysql",
			dsn:         "<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>",         // example value of a secret
			expected:    "mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			driver: "mysql",
			dsn:    "mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			// It is invalid connection string, but it should be handled later in driver-specific code
			expected:    "mysql://mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			connString:  "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>", // example value of a secret
			expected:    "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			driver:      "mssql",
			dsn:         "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>", // example value of a secret
			expected:    "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			connString:  "sqlite://:memory:",
			expected:    "sqlite://:memory:",
			expectedErr: nil,
		},
		{
			driver:      "sqlite",
			dsn:         ":memory:",
			expected:    "sqlite://:memory:",
			expectedErr: nil,
		},
		{
			connString:  "sqlite:///tmp/fuzz.db",
			expected:    "sqlite:///tmp/fuzz.db",
			expectedErr: nil,
		},
		{
			driver:      "sqlite",
			dsn:         "/tmp/fuzz.db",
			expected:    "sqlite:///tmp/fuzz.db",
			expectedErr: nil,
		},
		{
			connString:  "clickhouse://<USER>:<PASSWORD>@<HOST>:<PORT>/<DATABASE NAME>", // example value of a secret
			expected:    "clickhouse://<USER>:<PASSWORD>@<HOST>:<PORT>/<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			driver:      "clickhouse",
			dsn:         "tcp://<HOST>:9000?username=<USER>&password=<PASSWORD>&database=<DATABASE_NAME>", // example value of a secret
			expected:    "clickhouse://%3CUSER%3E:%3CPASSWORD%3E@<HOST>:9000/%3CDATABASE_NAME%3E",         // example value of a secret
			expectedErr: nil,
		},
		{
			connString:  "cql://admin:password@localhost:9042?keyspace=perfkit_db_ci", // example value of a secret
			expected:    "cql://admin:password@localhost:9042?keyspace=perfkit_db_ci", // example value of a secret
			expectedErr: nil,
		},
		{
			driver:      "cassandra",
			dsn:         "localhost&port=9042&username=admin&password=password&keyspace=perfkit_db_ci", // example value of a secret
			expected:    "cql://admin:password@localhost:9042?keyspace=perfkit_db_ci",                  // example value of a secret
			expectedErr: nil,
		},
		{
			connString:  "sqlite://:memory:",
			driver:      "postgres",
			dsn:         "host=127.0.0.1 sslmode=disable user=test_user",
			expected:    "",
			expectedErr: fmt.Errorf("both connection-string and (driver or dsn) are configured, please use only one"),
		},
		{
			driver:      "",
			dsn:         "host=127.0.0.1 sslmode=disable user=test_user",
			expected:    "",
			expectedErr: fmt.Errorf("both driver and dsn must be provided to construct the connection string"),
		},
		{
			driver:      "mysql",
			dsn:         "",
			expected:    "",
			expectedErr: fmt.Errorf("both driver and dsn must be provided to construct the connection string"),
		},
		{
			expected:    "",
			expectedErr: fmt.Errorf("specify the DB connection string using --connection-string"),
		},
	}

	for _, test := range tests {
		result, err := constructConnStringFromOpts(test.connString, test.driver, test.dsn)

		if err != nil {
			if test.expectedErr != nil {
				if !strings.Contains(err.Error(), test.expectedErr.Error()) {
					t.Errorf("failure for opts: %v, %v, %v, expected error: %v, got error: %v", test.connString, test.driver, test.dsn, test.expectedErr, err)
				}
			} else {
				t.Errorf("unexpected error for opts:: %v, %v, %v, err: %v", test.connString, test.driver, test.dsn, err)
			}
			continue
		}

		if test.expected != result {
			t.Errorf("unexpected result for opts: %v, %v, %v: %s", test.connString, test.driver, test.dsn, result)
		}
	}
}

func TestConstructConnStringFromEnv(t *testing.T) {
	type testConstructConnString struct {
		envValue    string
		connString  string
		driver      string
		dsn         string
		expected    string
		expectedErr error
	}

	tests := []testConstructConnString{
		{
			envValue:    "postgres://user:pass@localhost:5432/testdb?sslmode=disable",
			connString:  "",
			driver:      "",
			dsn:         "",
			expected:    "postgres://user:pass@localhost:5432/testdb?sslmode=disable",
			expectedErr: nil,
		},
		{
			envValue:    "mysql://user:pass@localhost:3306/testdb",
			connString:  "postgres://override:me@localhost:5432/testdb",
			driver:      "",
			dsn:         "",
			expected:    "postgres://override:me@localhost:5432/testdb",
			expectedErr: nil,
		},
		{
			envValue:    "mysql://user:pass@localhost:3306/testdb",
			connString:  "",
			driver:      "postgres",
			dsn:         "host=localhost port=5432 user=test password=test dbname=testdb sslmode=disable",
			expected:    "postgres://test:test@localhost:5432/testdb?sslmode=disable",
			expectedErr: nil,
		},
		{
			envValue:    "",
			connString:  "",
			driver:      "",
			dsn:         "",
			expected:    "",
			expectedErr: fmt.Errorf("specify the DB connection string using --connection-string"),
		},
	}

	for _, test := range tests {
		os.Setenv("ACRONIS_DB_BENCH_CONNECTION_STRING", test.envValue)
		result, err := constructConnStringFromOpts(test.connString, test.driver, test.dsn)

		if err != nil {
			if test.expectedErr != nil {
				if !strings.Contains(err.Error(), test.expectedErr.Error()) {
					t.Errorf("failure for env: %s, connString: %s, driver: %s, dsn: %s, expected error: %v, got error: %v",
						test.envValue, test.connString, test.driver, test.dsn, test.expectedErr, err)
				}
			} else {
				t.Errorf("unexpected error for env: %s, connString: %s, driver: %s, dsn: %s, err: %v",
					test.envValue, test.connString, test.driver, test.dsn, err)
			}
			continue
		}

		if test.expected != result {
			t.Errorf("unexpected result for env: %s, connString: %s, driver: %s, dsn: %s, expected: %s, got: %s",
				test.envValue, test.connString, test.driver, test.dsn, test.expected, result)
		}
	}

	// Reset environment variable after tests
	os.Setenv("ACRONIS_DB_BENCH_CONNECTION_STRING", "")
}
