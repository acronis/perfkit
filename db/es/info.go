package es

import "github.com/acronis/perfkit/db"

// GetVersion returns DB version and driver name
func getVersion(dia dialect) (db.DialectName, string, error) {
	return dia.name(), "", nil
}

// GetInfo returns DB info
func getInfo(q querier, version string) ([]string, *db.Info, error) {
	var ret []string

	ret = append(ret, "Elasticsearch")

	return ret, nil, nil
}

// GetTablesSchemaInfo returns the schema info for a given set of tables
func getTablesSchemaInfo(q querier, tableNames []string) ([]string, error) {
	return nil, nil
}

// GetTableSizeMB returns the size of a table in MB
func getTableSizeMB(q querier, tableName string) (int64, error) {
	return 0, nil
}

// GetTablesVolumeInfo returns the volume info for a given set of tables
func getTablesVolumeInfo(q querier, tableNames []string) ([]string, error) {
	return nil, nil
}
