package ms

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/meilisearch/meilisearch-go" //nolint:depguard

	"github.com/acronis/perfkit/db"
)

// enableVectorStore enables the vectorStore experimental feature via the REST API.
// The meilisearch-go SDK v0.36.1 removed SetVectorStore since newer Meilisearch
// versions promoted it out of experimental, but older servers still need it.
func enableVectorStore(host string) error {
	endpoint := host + "/experimental-features"

	// Check current state
	getResp, err := http.Get(endpoint) //nolint:gosec // host is from config
	if err != nil {
		return nil // endpoint might not exist in newer versions
	}
	defer getResp.Body.Close()

	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		return fmt.Errorf("failed to read experimental features response: %v", err)
	}

	var features map[string]interface{}
	if err = json.Unmarshal(body, &features); err != nil {
		return nil // cannot parse, skip
	}

	// Check if vectorStore exists and is already enabled
	if vs, ok := features["vectorStore"]; ok {
		if enabled, ok := vs.(bool); ok && enabled {
			return nil // already enabled
		}
	} else {
		return nil // field doesn't exist, vector store is built-in
	}

	// Enable vectorStore
	payload := []byte(`{"vectorStore": true}`)
	req, err := http.NewRequest(http.MethodPatch, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	patchResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to enable vector store: %v", err)
	}
	defer patchResp.Body.Close()

	if patchResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(patchResp.Body)
		return fmt.Errorf("failed to enable vector store, status %d: %s", patchResp.StatusCode, string(respBody))
	}

	return nil
}

// @cpt-perfkit-db-algo-ms-adapter-index-lifecycle

// TableExists checks if a Meilisearch index exists
func (d *msDatabase) TableExists(tableName string) (bool, error) {
	_, err := d.client.GetIndex(tableName)
	if err != nil {
		// Meilisearch returns an error when the index doesn't exist
		return false, nil
	}
	return true, nil
}

// CreateTable creates a Meilisearch index with primary key and filterable/sortable attributes
func (d *msDatabase) CreateTable(tableName string, tableDefinition *db.TableDefinition, tableMigrationDDL string) error {
	// Determine primary key from table definition
	var primaryKey string
	for _, row := range tableDefinition.TableRows {
		if row.IsPrimaryKey() {
			primaryKey = row.GetName()
			break
		}
	}

	if primaryKey == "" && len(tableDefinition.PrimaryKey) > 0 {
		primaryKey = tableDefinition.PrimaryKey[0]
	}

	if primaryKey == "" {
		primaryKey = "id"
	}

	taskInfo, err := d.client.CreateIndex(&meilisearch.IndexConfig{
		Uid:        tableName,
		PrimaryKey: primaryKey,
	})
	if err != nil {
		return fmt.Errorf("failed to create meilisearch index %s: %v", tableName, err)
	}

	task, err := d.client.WaitForTask(taskInfo.TaskUID, 100*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to wait for meilisearch create index task: %v", err)
	}

	if task.Status == "failed" {
		return fmt.Errorf("meilisearch create index task failed: %s", task.Error.Message)
	}

	// Configure filterable and sortable attributes from column definitions
	var filterableAttrs []interface{}
	var sortableAttrs []string
	embedders := make(map[string]meilisearch.Embedder)

	for _, row := range tableDefinition.TableRows {
		// Vector columns are configured as embedders, not filterable/sortable
		switch row.GetType() {
		case db.DataTypeVector3Float32:
			embedders[row.GetName()] = meilisearch.Embedder{
				Source:     meilisearch.UserProvidedEmbedderSource,
				Dimensions: 3,
			}
			continue
		case db.DataTypeVector768Float32:
			embedders[row.GetName()] = meilisearch.Embedder{
				Source:     meilisearch.UserProvidedEmbedderSource,
				Dimensions: 768,
			}
			continue
		}

		// All non-vector columns are made filterable for benchmark flexibility
		filterableAttrs = append(filterableAttrs, interface{}(row.GetName()))

		// Numeric and datetime types are sortable (including primary key)
		switch row.GetType() {
		case db.DataTypeId, db.DataTypeInt, db.DataTypeBigInt, db.DataTypeBigIntAutoInc,
			db.DataTypeSmallInt, db.DataTypeTinyInt, db.DataTypeDateTime, db.DataTypeDateTime6,
			db.DataTypeTimestamp, db.DataTypeTimestamp6:
			sortableAttrs = append(sortableAttrs, row.GetName())
		}
	}

	index := d.client.Index(tableName)

	// Configure vector embedders if any vector columns exist
	if len(embedders) > 0 {
		// Enable vector store experimental feature if needed
		if err = enableVectorStore(d.host); err != nil {
			return fmt.Errorf("failed to enable vector store for index %s: %v", tableName, err)
		}

		taskInfo, err = index.UpdateEmbedders(embedders)
		if err != nil {
			return fmt.Errorf("failed to update embedders for index %s: %v", tableName, err)
		}

		task, err = d.client.WaitForTask(taskInfo.TaskUID, 100*time.Millisecond)
		if err != nil {
			return fmt.Errorf("failed to wait for embedders task: %v", err)
		}

		if task.Status == "failed" {
			return fmt.Errorf("update embedders task failed: %s", task.Error.Message)
		}
	}

	if len(filterableAttrs) > 0 {
		taskInfo, err = index.UpdateFilterableAttributes(&filterableAttrs)
		if err != nil {
			return fmt.Errorf("failed to update filterable attributes for index %s: %v", tableName, err)
		}

		task, err = d.client.WaitForTask(taskInfo.TaskUID, 100*time.Millisecond)
		if err != nil {
			return fmt.Errorf("failed to wait for filterable attributes task: %v", err)
		}

		if task.Status == "failed" {
			return fmt.Errorf("update filterable attributes task failed: %s", task.Error.Message)
		}
	}

	if len(sortableAttrs) > 0 {
		taskInfo, err = index.UpdateSortableAttributes(&sortableAttrs)
		if err != nil {
			return fmt.Errorf("failed to update sortable attributes for index %s: %v", tableName, err)
		}

		task, err = d.client.WaitForTask(taskInfo.TaskUID, 100*time.Millisecond)
		if err != nil {
			return fmt.Errorf("failed to wait for sortable attributes task: %v", err)
		}

		if task.Status == "failed" {
			return fmt.Errorf("update sortable attributes task failed: %s", task.Error.Message)
		}
	}

	return nil
}

// DropTable deletes a Meilisearch index
func (d *msDatabase) DropTable(tableName string) error {
	taskInfo, err := d.client.DeleteIndex(tableName)
	if err != nil {
		return fmt.Errorf("failed to delete meilisearch index %s: %v", tableName, err)
	}

	task, err := d.client.WaitForTask(taskInfo.TaskUID, 100*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to wait for meilisearch delete index task: %v", err)
	}

	if task.Status == "failed" {
		// Ignore "index not found" errors for idempotent cleanup
		if strings.Contains(task.Error.Message, "not found") {
			return nil
		}
		return fmt.Errorf("meilisearch delete index task failed: %s", task.Error.Message)
	}

	return nil
}
