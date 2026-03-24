package ms

import (
	"fmt"
	"time"
)

// @cpt-perfkit-db-algo-ms-adapter-bulk-insert
func (g *msGateway) BulkInsert(tableName string, rows [][]interface{}, columnNames []string) error {
	if len(rows) == 0 {
		return nil
	}

	var documents []map[string]interface{}

	for _, row := range rows {
		if len(row) != len(columnNames) {
			return fmt.Errorf("row length doesn't match column names length")
		}

		doc := make(map[string]interface{})
		vectors := make(map[string]interface{})

		for i, col := range row {
			// Vector columns are stored in _vectors under the embedder name (column name)
			if vec, ok := col.([]float32); ok {
				vectors[columnNames[i]] = vec
			} else {
				doc[columnNames[i]] = col
			}
		}

		if len(vectors) > 0 {
			doc["_vectors"] = vectors
		}

		documents = append(documents, doc)
	}

	index := g.client.Index(tableName)

	taskInfo, err := index.AddDocuments(documents, nil)
	if err != nil {
		return fmt.Errorf("failed to add documents to meilisearch index %s: %v", tableName, err)
	}

	task, err := g.client.WaitForTask(taskInfo.TaskUID, 100*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to wait for meilisearch task %d: %v", taskInfo.TaskUID, err)
	}

	if task.Status == "failed" {
		return fmt.Errorf("meilisearch bulk insert task %d failed: %s", task.UID, task.Error.Message)
	}

	return nil
}
