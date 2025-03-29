package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

func constructConnStringFromOpts(opts DatabaseOpts) (string, error) {
	// Check if both ConnString and (Driver or Dsn) are configured
	if opts.ConnString != "" && (opts.Driver != "" || opts.Dsn != "") {
		return "", fmt.Errorf("both connection-string and (driver or dsn) are configured, please use only one")
	}

	var connString string
	if opts.ConnString != "" {
		// Use the provided connection string from command line (highest priority)
		connString = opts.ConnString
	} else if opts.Driver != "" || opts.Dsn != "" {
		// Construct the connection string from Driver and Dsn if they are provided
		if opts.Driver == "" || opts.Dsn == "" {
			return "", fmt.Errorf("both driver and dsn must be provided to construct the connection string")
		}
		var err error
		if connString, err = convertDsnToUri(opts.Driver, opts.Dsn); err != nil {
			return "", err
		}
	} else {
		// Check for connection string in environment variable
		envConnString := os.Getenv("ACRONIS_DB_BENCH_CONNECTION_STRING")
		if envConnString != "" {
			connString = envConnString
		} else {
			// No valid connection configuration provided
			return "", fmt.Errorf("specify the DB connection string using --connection-string or ACRONIS_DB_BENCH_CONNECTION_STRING environment variable\n" +
				"examples:\n" +
				"  PostgreSQL:    postgresql://user:password@localhost:5432/database?sslmode=disable\n" +
				"  MySQL/MariaDB: mysql://user:password@tcp(localhost:3306)/database\n" +
				"  MS SQL Server: sqlserver://user:password@localhost:1433?database=database\n" +
				"  SQLite:        sqlite:///path/to/database.db\n" +
				"  ClickHouse:    clickhouse://user:password@localhost:9000/database\n" +
				"  Cassandra:     cql://user:password@localhost:9042?keyspace=database\n" +
				"  ElasticSearch: es://user::password@localhost:9200\n" +
				"  OpenSearch:    opensearch://user::password@localhost:9200")
		}
	}

	return connString, nil
}

// Helper function to convert DSN to URI format
func convertDsnToUri(dbDriver string, dsn string) (string, error) {
	switch dbDriver {
	case "postgres":
		// Postgres DSN can be respresnted in URI format and in key-value pairs format also
		// Check if the DSN is already in URI format
		// And if not, convert it to URI format
		if parsedUrl, parseErr := url.Parse(dsn); parseErr == nil {
			if parsedUrl.Scheme == "postgres" || parsedUrl.Scheme == "postgresql" {
				return dsn, nil
			}
		}

		// Parse the DSN string into key-value pairs
		parts := strings.Fields(dsn)
		params := make(map[string]string)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				return "", fmt.Errorf("invalid DSN format: %s", dsn)
			}
			params[kv[0]] = kv[1]
		}

		// Validate required keys
		user, hasUser := params["user"]
		pass, _ := params["password"]
		host, hasHost := params["host"]
		port, _ := params["port"]
		dbName, hasDBName := params["dbname"]

		var urlDSN = url.URL{}
		urlDSN.Scheme = "postgres"

		if hasUser {
			urlDSN.User = url.UserPassword(user, pass)
		}

		if hasHost {
			urlDSN.Host = fmt.Sprintf("%s:%s", host, port)
		}

		if hasDBName {
			urlDSN.Path = dbName
		}

		// Construct the URI
		query := url.Values{}
		for k, v := range params {
			if k != "user" && k != "password" && k != "host" && k != "port" && k != "dbname" {
				query.Add(k, v)
			}
		}
		if len(query) > 0 {
			urlDSN.RawQuery = query.Encode()
		}

		return urlDSN.String(), nil
	case "mysql":
		// Mysql DSN connection string should not have any schema, so just return the DSN with mysql:// prefix
		return fmt.Sprintf("mysql://%s", dsn), nil
	case "sqlserver", "mssql":
		// SQL Server DSN connection string should be in URI format, so just return the DSN
		return dsn, nil
	case "sqlite":
		return fmt.Sprintf("sqlite://%s", dsn), nil
	case "clickhouse":
		var urlConnString, err = url.Parse(dsn)
		if err != nil {
			return "", err
		}

		var params = urlConnString.Query()

		// Validate required keys
		userParams, hasUser := params["username"]
		passParams, _ := params["password"]
		dbNameParams, hasDBName := params["database"]

		var user, pass, dbName string
		if hasUser && len(userParams) > 0 {
			user = userParams[0]
		}
		if len(passParams) > 0 {
			pass = passParams[0]
		}
		if hasDBName && len(dbNameParams) > 0 {
			dbName = dbNameParams[0]
		}

		urlConnString.Scheme = dbDriver
		if hasUser && urlConnString.User == nil {
			urlConnString.User = url.UserPassword(user, pass)
		}

		if hasDBName && urlConnString.Path == "" {
			urlConnString.Path = dbName
		}

		// Clear the query params before constructing the URI
		urlConnString.RawQuery = ""

		// Construct the URI
		query := url.Values{}
		for k, values := range params {
			if k != "username" && k != "password" && k != "database" {
				for _, v := range values {
					query.Add(k, v)
				}
			}
		}
		if len(query) > 0 {
			urlConnString.RawQuery = query.Encode()
		}

		return urlConnString.String(), nil
	case "cassandra":
		var parts = strings.Split(dsn, "&")

		var params = make(map[string]string)

		if len(parts) == 0 {

		}

		var host = parts[0]
		for _, part := range parts[1:] {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				return "", fmt.Errorf("invalid DSN format: %s", dsn)
			}
			params[kv[0]] = kv[1]
		}

		// Validate required keys
		user, hasUser := params["username"]
		pass, _ := params["password"]
		port, _ := params["port"]

		var urlDSN = url.URL{}
		urlDSN.Scheme = "cql"

		if hasUser {
			urlDSN.User = url.UserPassword(user, pass)
		}

		urlDSN.Host = fmt.Sprintf("%s:%s", host, port)

		// Construct the URI
		query := url.Values{}
		for k, v := range params {
			if k != "username" && k != "password" && k != "port" {
				query.Add(k, v)
			}
		}
		if len(query) > 0 {
			urlDSN.RawQuery = query.Encode()
		}

		return urlDSN.String(), nil
	default:
		return "", fmt.Errorf("unsupported driver: %s", dbDriver)
	}
}
