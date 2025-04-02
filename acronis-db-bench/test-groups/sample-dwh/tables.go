package sample_dwh

import (
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestTableAdvmTasks is table to store tasks
var TestTableAdvmTasks = engine.TestTable{
	TableName: "acronis_db_bench_advm_tasks",
	Databases: engine.RELATIONAL,
	Columns: [][]interface{}{
		{"origin", "int", 20},
		{"uuid", "uuid", 0},
		{"tenant_id", "uuid", 32},
		{"type", "uuid", 32},
		{"queue", "string", 16, 32},
		{"created_at", "time_ns", 90},
		{"started_at", "time_ns", 90},
		{"completed_at", "time_ns", 90},
		{"duration", "int", 20},
		{"issuer_id", "uuid", 32},
		{"assigned_agent_id", "uuid", 1024},
		{"started_by", "string", 128, 32},
		{"policy_id", "uuid", 1024},
		{"resource_id", "uuid", 100000},
		{"result_code_indexed", "int", 8},
		{"result_code", "string", 8, 32},
		{"result_error_domain", "string", 8, 32},
		{"result_error_code", "string", 8, 32},
		{"backup_bytes_saved", "int", 0},
		{"backup_bytes_processed", "int", 0},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `create table acronis_db_bench_advm_tasks(
			origin                 INT         NOT NULL, -- CHAR(36)
			id                     {$bigint_autoinc} NOT NULL,
			uuid                   CHAR(36)    NOT NULL UNIQUE,

			tenant_id              VARCHAR(64) NOT NULL,
			type                   VARCHAR(64) NOT NULL,
			queue                  VARCHAR(64) NOT NULL,

			created_at             BIGINT      NOT NULL,
			started_at             BIGINT,
			completed_at           BIGINT,
			duration               BIGINT,

			issuer_id              VARCHAR(64) NOT NULL,
			assigned_agent_id      VARCHAR(64),
			started_by             VARCHAR(256),
			policy_id              VARCHAR(64),
			resource_id            VARCHAR(64),
			result_code_indexed    INTEGER,
			result_code            VARCHAR(64),
			result_error_domain    VARCHAR(64),
			result_error_code      VARCHAR(64),
			backup_bytes_saved     INTEGER,
			backup_bytes_processed INTEGER,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"created_at"}, {"result_code_indexed"}},
}

// TestTableAdvmResources is table to store resources
var TestTableAdvmResources = engine.TestTable{
	TableName: "acronis_db_bench_advm_resources",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	Columns: [][]interface{}{
		{"origin", "int", 20},
		{"resource_uuid", "uuid", 100000},
		{"tenant_id", "string", 10, 32},
		{"customer_id", "string", 10, 32},

		{"type", "int", 4},
		{"name", "string", 100000, 32},

		{"created_at", "time_ns", 90},

		{"os", "string", 4, 32},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_resources(
			origin        INT               NOT NULL, -- CHAR(36)
			resource_id   {$bigint_autoinc} NOT NULL,
			resource_uuid CHAR(36)          NOT NULL,
			tenant_id     CHAR(36),
			customer_id   CHAR(36),

			type          INTEGER          NOT NULL,
			name          VARCHAR(256),

			created_at    BIGINT           NOT NULL,
			deleted_at    BIGINT,

			os            VARCHAR(256),

			PRIMARY KEY (origin, resource_uuid, tenant_id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"type"}, {"name"}},
}

// TestTableAdvmResourcesStatuses is table to store resources statuses
// inspired by the Event Archive table
var TestTableAdvmResourcesStatuses = engine.TestTable{
	TableName: "acronis_db_bench_advm_resources_statuses",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	Columns: [][]interface{}{
		{"origin", "int", 20},
		{"state", "int", 4},
		{"severity", "int", 4},
		{"applied_policy_names", "string", 100, 32},
		{"last_successful_backup", "time_ns", 90},
		{"next_backup", "time_ns", 90},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_resources_statuses(
			origin                 CHAR(36),
			resource_id            {$bigint_autoinc} NOT NULL,

			state                  INTEGER  DEFAULT 0,
			severity               SMALLINT DEFAULT 0,
			applied_policy_names   VARCHAR(256),
			last_successful_backup BIGINT,
			next_backup            BIGINT,

			PRIMARY KEY (origin, resource_id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"state"}},
}

// TestTableAdvmAgentsResources is table to store agents
// inspired by the Event Archive table
var TestTableAdvmAgentsResources = engine.TestTable{
	TableName: "acronis_db_bench_advm_agent_resources",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	Columns: [][]interface{}{
		{"origin", "int", 20},
		{"agent_uuid", "uuid", 100000},
		{"resource_id", "int", 100000},
		{"tenant_id", "string", 10, 32},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_agent_resources(
			origin      CHAR(36),
			agent_uuid  CHAR(36) NOT NULL,
			resource_id BIGINT   NOT NULL,
			tenant_id   CHAR(36) NOT NULL,

			PRIMARY KEY (origin, agent_uuid, resource_id, tenant_id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}},
}

// TestTableAdvmAgents is table to store agents
// inspired by the Event Archive table
var TestTableAdvmAgents = engine.TestTable{
	TableName: "acronis_db_bench_advm_agents",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	Columns: [][]interface{}{
		{"origin", "int", 20},
		{"uuid", "uuid", 100000},

		{"tenant_id", "string", 10, 32},
		{"type", "int", 4},
		{"name", "string", 100000, 32},

		{"created_at", "time_ns", 90},

		{"is_active", "bool", 0},
		{"os_family", "string", 8, 32},
		{"os_name", "string", 8, 32},
		{"version", "string", 8, 32},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_agents(
			origin     CHAR(36),
			uuid       CHAR(36) NOT NULL,

			tenant_id  CHAR(36),
			type       VARCHAR(64)      NOT NULL,
			name       VARCHAR(128),

			created_at BIGINT           NOT NULL,
			deleted_at BIGINT,

			is_active  BIT,
			os_family  VARCHAR(64),
			os_name    VARCHAR(255),
			version    VARCHAR(36),

			PRIMARY KEY (origin, uuid)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"type"}, {"name"}},
}

// TestTableAdvmBackupResources is table to store backups
// inspired by the Event Archive table
var TestTableAdvmBackupResources = engine.TestTable{
	TableName: "acronis_db_bench_advm_backup_resources",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	Columns: [][]interface{}{
		{"origin", "int", 20},
		{"backup_id", "int", 400000},
		{"resource_uuid", "uuid", 100000},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_backup_resources(
			origin        CHAR(36),
			backup_id     BIGINT   NOT NULL,
			resource_uuid CHAR(36) NOT NULL DEFAULT '',

			PRIMARY KEY (origin, backup_id, resource_uuid)
			) {$engine};`,
	Indexes: [][]string{{"origin"}},
}

// TestTableAdvmBackups is table to store backups
// inspired by the Event Archive table
var TestTableAdvmBackups = engine.TestTable{
	TableName: "acronis_db_bench_advm_backups",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	Columns: [][]interface{}{
		{"origin", "int", 20},

		{"created_at", "time_ns", 90},

		{"archive_id", "int", 100000},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_backups(
			origin     CHAR(36),
			id         {$bigint_autoinc} NOT NULL,

			created_at BIGINT  NOT NULL,
			deleted_at BIGINT,

			archive_id BIGINT  NOT NULL,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"created_at"}, {"archive_id"}},
}

// TestTableAdvmArchives is table to store archives
// inspired by the Event Archive table
var TestTableAdvmArchives = engine.TestTable{
	TableName: "acronis_db_bench_advm_archives",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	Columns: [][]interface{}{
		{"origin", "int", 20},

		{"created_at", "time_ns", 90},

		{"vault_id", "int", 100000},
		{"size", "int", 100000},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_archives(
			origin     CHAR(36),
			id         {$bigint_autoinc} NOT NULL,

			created_at BIGINT NOT NULL,
			deleted_at BIGINT,

			vault_id   BIGINT NOT NULL,
			size       BIGINT NOT NULL,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"created_at"}, {"vault_id"}},
}

// TestTableAdvmVaults is table to store vaults
// inspired by the Event Archive table
var TestTableAdvmVaults = engine.TestTable{
	TableName: "acronis_db_bench_advm_vaults",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	Columns: [][]interface{}{
		{"origin", "int", 20},

		{"name", "string", 100000, 32},
		{"storage_type", "string", 4, 32},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `CREATE TABLE acronis_db_bench_advm_vaults(
			origin       CHAR(36),
			id           {$bigint_autoinc} NOT NULL,

			name         VARCHAR(128) NOT NULL,
			storage_type VARCHAR(64)  NOT NULL,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"name"}},
}

// TestTableAdvmDevices is table to store devices
// inspired by the Event Archive table
var TestTableAdvmDevices = engine.TestTable{
	TableName: "acronis_db_bench_advm_devices",
	Databases: []db.DialectName{db.POSTGRES, db.MSSQL},
	Columns: [][]interface{}{
		{"origin", "int", 20},
		{"uuid", "uuid", 0},

		{"name", "string", 100000, 32},
		{"type", "string", 4, 32},
		{"group_name", "string", 1000, 32},
		{"resource_os", "string", 4, 32},
		{"registered_at", "time_ns", 90},

		{"agent_name", "string", 100000, 32},
		{"agent_is_active", "bool", 0},
		{"agent_version", "string", 8, 32},

		{"customer_name", "string", 10, 32},
		{"unit_name", "string", 1000, 32},

		{"applied_policy", "string", 8, 32},
		{"state", "string", 4, 32},
		{"last_result", "string", 4, 32},
		{"last_backup", "time_ns", 90},
		{"next_backup", "time_ns", 90},

		{"archives_count", "int", 8},
		{"backups_count", "int", 16},
		{"oldest_backup", "time_ns", 90},
		{"latest_backup", "time_ns", 90},
		{"used_total", "int", 1000000},
		{"used_cloud", "int", 1000000},
		{"used_local", "int", 1000000},

		{"alerts_count", "int", 8},
	},
	InsertColumns: []string{}, // all
	CreateQuery: `create table acronis_db_bench_advm_devices(
			origin                 INT         NOT NULL, -- CHAR(36)
			id                     {$bigint_autoinc} NOT NULL,
			uuid                   CHAR(36)    NOT NULL UNIQUE,

			name                   VARCHAR(64) NOT NULL,
			type                   VARCHAR(64) NOT NULL,
			group_name             VARCHAR(64) NOT NULL,
			resource_os            VARCHAR(64) NOT NULL,
			registered_at          BIGINT,
	
			agent_name             VARCHAR(64) NOT NULL,
			agent_is_active        {$boolean},
			agent_version          VARCHAR(64) NOT NULL,
	
			customer_name          VARCHAR(64) NOT NULL,
			unit_name              VARCHAR(64) NOT NULL,
	
			applied_policy         VARCHAR(64) NOT NULL,
			state                  VARCHAR(64) NOT NULL,
			last_result            VARCHAR(64) NOT NULL,
	
			last_backup            BIGINT,
			next_backup            BIGINT,
			archives_count         BIGINT,
			backups_count          BIGINT,
			oldest_backup          BIGINT,
			latest_backup          BIGINT,
			used_total             BIGINT,
			used_cloud             BIGINT,
			used_local             BIGINT,

			alerts_count           BIGINT,

			PRIMARY KEY (origin, id)
			) {$engine};`,
	Indexes: [][]string{{"origin"}, {"name"}, {"type"}, {"group_name"}, {"registered_at"}, {"agent_name"}, {"agent_is_active"}},
}
