package sql

import (
	"database/sql"
	"github.com/acronis/perfkit/db"
)

type sqlStatement interface {
	Exec(args ...any) (db.Result, error)
	Close() error
}

type sqlStmt struct {
	stmt *sql.Stmt
}

func (s *sqlStmt) Exec(args ...any) (db.Result, error) {
	return s.stmt.Exec(args...)
}

func (s *sqlStmt) Close() error {
	return s.stmt.Close()
}

func (g *sqlGateway) Prepare(query string) (db.Stmt, error) {
	return g.rw.prepareContext(g.ctx, query)
}
