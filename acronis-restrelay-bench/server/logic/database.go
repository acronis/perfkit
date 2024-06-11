package logic

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	// Importing drivers for database/sql
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var maxIdleConnections = 32

var postgres *sql.DB

func initPostgres(user string, password string, host string) error {
	databaseConnectionString := fmt.Sprintf("user=%s host=%s sslmode=disable", user, host)
	if password != "" {
		databaseConnectionString = fmt.Sprintf("%s password=%s", databaseConnectionString, password)
	}

	var err error
	postgres, err = sql.Open("postgres", databaseConnectionString)
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %v", err)
	}
	postgres.SetMaxIdleConns(maxIdleConnections) //There is no way to understand on server value of this constant. So for more clients it should be increased

	return nil
}

var mysql *sql.DB

func initMysql(user string, password string, host string) error {
	var databaseConnectionString string
	if password == "" {
		databaseConnectionString = fmt.Sprintf("%s@tcp(%s)/testdb", user, host)
	} else {
		databaseConnectionString = fmt.Sprintf("%s:%s@tcp(%s)/testdb", user, password, host)
	}

	var err error
	mysql, err = sql.Open("mysql", databaseConnectionString)
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %v", err)
	}
	mysql.SetMaxIdleConns(maxIdleConnections) //There is no way to understand on server value of this constant. So for more clients it should be increased

	return nil
}

// SetupDatabaseEnvironment initializes database connection
func SetupDatabaseEnvironment(user string, password string, postgresHost string, mysqlHost string) error {
	err := initPostgres(user, password, postgresHost)
	if err != nil {
		return err
	}

	err = initMysql(user, password, mysqlHost)

	return err
}

type databaseAction struct {
	Database string
	Query    int
}

func (dbArgs *databaseAction) perform() error {
	var database *sql.DB
	switch dbArgs.Database {
	case "postgres":
		database = postgres
	case "mysql":
		database = mysql
	}

	if database == nil {
		return errors.New("database was not configured")
	}

	switch dbArgs.Query {
	case 1:
		_, err := database.Exec("SELECT 1")
		if err != nil {
			return fmt.Errorf("failed to execute query: %v", err)
		}
	default:
		return fmt.Errorf("unknown action %d", dbArgs.Query)
	}

	return nil
}

func (dbArgs *databaseAction) parseParameters(params map[string]string) error {
	var engine, query string
	var ok bool

	if engine, ok = params["engine"]; !ok {
		return errors.New("engine parameter is missing")
	}

	if query, ok = params["query"]; !ok {
		return errors.New("query parameter is missing")
	}

	var err error

	dbArgs.Database = engine
	if dbArgs.Database != "postgres" && dbArgs.Database != "mysql" {
		return errors.New("unknown database")
	}

	dbArgs.Query, err = strconv.Atoi(query)
	if err != nil {
		return fmt.Errorf("failed conversion string to int in DatabaseArguments with: %v", err)
	}

	return nil
}
