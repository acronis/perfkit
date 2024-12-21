package main

import (
	"fmt"
	"testing"
)

func TestConstructConnStringFromOpts(t *testing.T) {
	type testConstructConnString struct {
		opts        DatabaseOpts
		expected    string
		expectedErr error
	}

	tests := []testConstructConnString{
		{
			opts: DatabaseOpts{
				ConnString: "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable", // example value of a secret
			},
			expected:    "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				Driver: "postgres",
				Dsn:    "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable", // example value of a secret
			},
			expected:    "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				Driver: "postgres",
				Dsn:    "host=localhost port=5432 user=<USER> password=<PASSWORD> dbname=<DATABASE_NAME> sslmode=disable", // example value of a secret
			},
			expected:    "postgres://%3CUSER%3E:%3CPASSWORD%3E@localhost:5432/%3CDATABASE_NAME%3E?sslmode=disable", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				ConnString: "mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			},
			expected:    "mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				Driver: "mysql",
				Dsn:    "<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			},
			expected:    "mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				Driver: "mysql",
				Dsn:    "mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			},
			// It is invalid connection string, but it should be handled later in driver-specific code
			expected:    "mysql://mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				ConnString: "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>", // example value of a secret
			},
			expected:    "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				Driver: "mssql",
				Dsn:    "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>", // example value of a secret
			},
			expected:    "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				ConnString: "sqlite://:memory:",
			},
			expected:    "sqlite://:memory:",
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				Driver: "sqlite",
				Dsn:    ":memory:",
			},
			expected:    "sqlite://:memory:",
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				ConnString: "sqlite:///tmp/fuzz.db",
			},
			expected:    "sqlite:///tmp/fuzz.db",
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				Driver: "sqlite",
				Dsn:    "/tmp/fuzz.db",
			},
			expected:    "sqlite:///tmp/fuzz.db",
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				ConnString: "clickhouse://<USER>:<PASSWORD>@<HOST>:<PORT>/<DATABASE NAME>", // example value of a secret
			},
			expected:    "clickhouse://<USER>:<PASSWORD>@<HOST>:<PORT>/<DATABASE NAME>", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				Driver: "clickhouse",
				Dsn:    "tcp://<HOST>:9000?username=<USER>&password=<PASSWORD>&database=<DATABASE_NAME>", // example value of a secret
			},
			expected:    "clickhouse://%3CUSER%3E:%3CPASSWORD%3E@<HOST>:9000/%3CDATABASE_NAME%3E", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				ConnString: "cql://admin:password@localhost:9042?keyspace=perfkit_db_ci", // example value of a secret
			},
			expected:    "cql://admin:password@localhost:9042?keyspace=perfkit_db_ci", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				Driver: "cassandra",
				Dsn:    "localhost&port=9042&username=admin&password=password&keyspace=perfkit_db_ci", // example value of a secret
			},
			expected:    "cql://admin:password@localhost:9042?keyspace=perfkit_db_ci", // example value of a secret
			expectedErr: nil,
		},
		{
			opts: DatabaseOpts{
				ConnString: "sqlite://:memory:",
				Driver:     "postgres",
				Dsn:        "host=127.0.0.1 sslmode=disable user=test_user",
			},
			expected:    "",
			expectedErr: fmt.Errorf("both connection-string and (driver or dsn) are configured, please use only one"),
		},
		{
			opts: DatabaseOpts{
				Driver: "",
				Dsn:    "host=127.0.0.1 sslmode=disable user=test_user",
			},
			expected:    "",
			expectedErr: fmt.Errorf("both driver and dsn must be provided to construct the connection string"),
		},
		{
			opts: DatabaseOpts{
				Driver: "mysql",
				Dsn:    "",
			},
			expected:    "",
			expectedErr: fmt.Errorf("both driver and dsn must be provided to construct the connection string"),
		},
		{
			opts:        DatabaseOpts{},
			expected:    "",
			expectedErr: fmt.Errorf("no valid connection configuration found"),
		},
	}

	for _, test := range tests {
		result, err := constructConnStringFromOpts(test.opts)

		if err != nil {
			if test.expectedErr != nil {
				if test.expectedErr.Error() != err.Error() {
					t.Errorf("failure for opts: %+v", test.opts)
				}
			} else {
				t.Errorf("unexpected error for opts: %+v, err: %v", test.opts, err)
			}
			continue
		}

		if test.expected != result {
			t.Errorf("unexpected result for opts: %+v: %s", test.opts, result)
		}
	}
}
