// Package main provides benchmarking utilities for databases.
// It includes various tests and options for database performance analysis.
package main

import (
	_ "net/http/pprof"

	// List of database drivers
	_ "github.com/acronis/perfkit/db/es"  // es drivers
	_ "github.com/acronis/perfkit/db/sql" // sql drivers

	// List of scenarios
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/basic-scenarios"          // basic-scenarios
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/composite-retreival"      // composite-retreival
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/json-search"              // json-search
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/large-objects-operations" // large-objects-operations
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/logs-search"              // logs-search
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/ping"                     // ping
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/sample-dwh"               // sample-dwh
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/select-one"               // select-one
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/sequence-operations"      // sequence-operations
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/tenant-and-cti"           // tenant-and-cti
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/time-series-operations"   // time-series-operations
	_ "github.com/acronis/perfkit/acronis-db-bench/scenarios/vector-search"            // vector-search

	// Engine
	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func main() {
	engine.Main()
}
