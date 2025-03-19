package engine

import (
	"fmt"
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
			expectedErr: fmt.Errorf("no valid connection configuration found"),
		},
	}

	for _, test := range tests {
		result, err := constructConnStringFromOpts(test.connString, test.driver, test.dsn)

		if err != nil {
			if test.expectedErr != nil {
				if test.expectedErr.Error() != err.Error() {
					t.Errorf("failure for opts: %v, %v, %v", test.connString, test.driver, test.dsn)
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
