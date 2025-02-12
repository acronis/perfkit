package sql

import (
	"fmt"

	"github.com/acronis/perfkit/db"
)

// GetNextVal returns the next value from a sequence.
// Uses dialect-specific SQL syntax for sequence operations.
// Implements sequence operations through the querier interface.
func (s *sqlSession) GetNextVal(sequenceName string) (uint64, error) {
	var nextVal uint64

	// Handle different sequence syntax based on dialect
	switch s.dialect.name() {
	case db.POSTGRES, db.MSSQL, db.MYSQL:
		var query string

		// Each dialect has its own syntax for getting the next sequence value
		switch s.dialect.name() {
		case db.POSTGRES:
			// PostgreSQL uses NEXTVAL('sequence_name')
			query = "SELECT NEXTVAL('" + sequenceName + "')"
		case db.MYSQL:
			// MySQL uses NEXTVAL(sequence_name)
			query = "SELECT NEXTVAL(" + sequenceName + ")"
		case db.MSSQL:
			// MSSQL uses NEXT VALUE FOR sequence_name
			query = "SELECT NEXT VALUE FOR " + sequenceName
		}

		// Uses querier.QueryRow to fetch single value from sequence
		// QueryRow is part of the querier interface for single-row queries
		if err := s.rw.queryRowContext(s.ctx, query).Scan(&nextVal); err != nil {
			return 0, fmt.Errorf("failed get nextVal for sequence '%s': %v", sequenceName, err)
		}

	case db.SQLITE:
		// SQLite doesn't have native sequences, so we implement using a table
		// Uses inTx to ensure atomic increment operation
		if txErr := inTx(s.ctx, s.t, s.dialect, func(q querier, dia dialect) error {
			var value int64

			// Read current value using querier.queryRowContext
			if err := q.queryRowContext(s.ctx, "SELECT value FROM "+sequenceName+" WHERE sequence_id = 1;").Scan(&value); err != nil {
				return err
			}

			// Increment value using querier.execContext
			if _, err := q.execContext(s.ctx, fmt.Sprintf("UPDATE %s SET value = %d WHERE sequence_id = 1", sequenceName, value+1)); err != nil {
				return err
			}

			nextVal = uint64(value)
			return nil
		}); txErr != nil {
			return 0, fmt.Errorf("failed get nextVal for sequence '%s': %v", sequenceName, txErr)
		}
	}

	return nextVal, nil
}
