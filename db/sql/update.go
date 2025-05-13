package sql

import "github.com/acronis/perfkit/db"

// Update implements the databaseUpdater interface for SQL databases.
// This is a dummy implementation that returns 0 affected rows and nil error.
func (g *sqlGateway) Update(tableName string, c *db.UpdateCtrl) (int64, error) {
	return 0, nil
}
