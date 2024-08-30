package es

import (
	"time"

	"github.com/acronis/perfkit/db"
)

// StatementEnter is called before executing a statement
func (g *esGateway) StatementEnter(query string, args ...interface{}) time.Time { //nolint:revive
	return time.Now()
}

// StatementExit is called after executing a statement
func (g *esGateway) StatementExit(statement string, startTime time.Time, err error, showRowsAffected bool, result db.Result, format string, args []interface{}, rows db.Rows, dest []interface{}) {
}

func (g *esGateway) Prepare(query string) (db.Stmt, error) {
	return nil, nil
}
