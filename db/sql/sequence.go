package sql

import (
	"fmt"

	"github.com/acronis/perfkit/db"
)

// GetNextVal returns the next value from a sequence
func (s *esSession) GetNextVal(sequenceName string) (uint64, error) {
	var nextVal uint64

	switch s.dialect.name() {
	case db.POSTGRES, db.MSSQL, db.MYSQL:
		var query string

		switch s.dialect.name() {
		case db.POSTGRES:
			query = "SELECT NEXTVAL('" + sequenceName + "')"
		case db.MYSQL:
			query = "SELECT NEXTVAL(" + sequenceName + ")"
		case db.MSSQL:
			query = "SELECT NEXT VALUE FOR " + sequenceName
		}

		if err := s.QueryRow(query).Scan(&nextVal); err != nil {
			return 0, fmt.Errorf("failed get nextVal for sequence '%s': %v", sequenceName, err)
		}

	case db.SQLITE:
		if txErr := inTx(s.ctx, s.t, s.dialect, func(q querier, dia dialect) error {
			var value int64

			if err := q.queryRowContext(s.ctx, "SELECT value FROM "+sequenceName+" WHERE sequence_id = 1;").Scan(&value); err != nil {
				return err
			}

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
