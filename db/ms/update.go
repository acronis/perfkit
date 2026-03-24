package ms

import "github.com/acronis/perfkit/db"

// Update is a no-op implementation for Meilisearch.
func (g *msGateway) Update(tableName string, c *db.UpdateCtrl) (int64, error) {
	return 0, nil
}
