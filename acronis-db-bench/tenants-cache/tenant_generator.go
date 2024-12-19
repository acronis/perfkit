package tenants_cache

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	guuid "github.com/google/uuid"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
)

// TenantUUID is a type for tenant uuid
type TenantUUID string

// CTIUUID is a type for cti uuid
type CTIUUID string

// TenantObj is a struct for tenant object in DB table acronis_db_bench_cybercache_tenants
type TenantObj struct {
	ID   int64      `db:"id"`
	UUID TenantUUID `db:"uuid"`

	Name            string `db:"name"`
	Kind            string `db:"kind"`
	IsDeleted       bool   `db:"is_deleted"`
	ParentID        int64  `db:"parent_id"`
	ParentHasAccess bool   `db:"parent_has_access"`
	NestingLevel    int    `db:"nesting_level"`
}

// CtiEntityObj is a struct for cti entity object in DB table acronis_db_bench_cybercache_cti_entities
type CtiEntityObj struct {
	UUID              CTIUUID `db:"uuid"`
	CTI               string  `db:"cti"`
	Kind              int     `db:"kind"`
	Final             int     `db:"final"`
	Resilient         int     `db:"resilient"`
	GlobalState       int     `db:"global_state"`
	EntitySchema      string  `db:"entity_schema"`
	Annotations       string  `db:"annotations"`
	Traits            string  `db:"traits"`
	TraitsSchema      string  `db:"traits_schema"`
	TraitsAnnotations string  `db:"traits_annotations"`
}

const (
	TableNameTenants         = "acronis_db_bench_cybercache_tenants"          // TableNameTenants is a name of tenants table in DB
	TableNameTenantClosure   = "acronis_db_bench_cybercache_tenant_closure"   // TableNameTenantClosure is a name of tenant_closure table in DB
	TableNameCtiEntities     = "acronis_db_bench_cybercache_cti_entities"     // TableNameCtiEntities is a name of cti_entities table in DB
	TableNameCtiProvisioning = "acronis_db_bench_cybercache_cti_provisioning" // TableNameCtiProvisioning is a name of cti_provisioning table in DB
)

// TenantsCache is a struct for tenants cache
type TenantsCache struct {
	tenantsWorkingSetLimit    int
	ctisWorkingSetLimit       int
	logger                    *benchmark.Logger
	benchmark                 *benchmark.Benchmark
	uuids                     []TenantUUID
	customerUUIDS             []TenantUUID
	partnerUUIDS              []TenantUUID
	parentUUIDs               map[string][]TenantUUID
	ctiUuids                  []CTIUUID
	tenantStructureRandomizer *tenantStructureRandomizer
	exitLock                  sync.Mutex
}

// NewTenantsCache creates a new TenantsCache instance
func NewTenantsCache(bench *benchmark.Benchmark) *TenantsCache {
	var tenantCache = &TenantsCache{
		tenantsWorkingSetLimit: 0,
		logger:                 bench.Logger,
		benchmark:              bench,
		uuids:                  []TenantUUID{},
		customerUUIDS:          []TenantUUID{},
		partnerUUIDS:           []TenantUUID{},
		parentUUIDs:            map[string][]TenantUUID{},
	}

	if bench.Randomizer == nil {
		bench.Randomizer = benchmark.NewRandomizer(bench.CommonOpts.RandSeed, bench.CommonOpts.Workers)
	}

	bench.Randomizer.RegisterPlugin("tenant", tenantCache)

	return tenantCache
}

// SetTenantsWorkingSet allows to limit the number of effective tenants used for other tests queries
/*
 * keeping appropriate level of these tests data cardinality. Not all the tenants have some real
 * usage, so the tenantsWorkingSetLimit allows to simulate alive and dead tenants ratio.
 */
func (tc *TenantsCache) SetTenantsWorkingSet(limit int) {
	if limit < 1 {
		limit = 1
	}
	tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("adjust tenants working set to: %d", limit))
	tc.tenantsWorkingSetLimit = limit
}

// SetCTIsWorkingSet allows to limit the number of effective CTIs used for other tests queries
func (tc *TenantsCache) SetCTIsWorkingSet(limit int) {
	if limit < 1 {
		limit = 1
	}
	tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("adjust CTI working set to: %d", limit))
	tc.ctisWorkingSetLimit = limit
}

// Exit prints message and exits with -1 code
func (tc *TenantsCache) Exit(msg string) {
	tc.exitLock.Lock() // ugly, but prevents multiple messages on exit
	tc.benchmark.Exit(msg)
}

// TenantsDDLSQL is a DDL for tenants table for MySQL and PostgreSQL databases
var TenantsDDLSQL = fmt.Sprintf(`CREATE TABLE %s (
    id                BIGINT       NOT NULL PRIMARY KEY,
    uuid              VARCHAR(36)     NOT NULL,
    name              VARCHAR(255) NOT NULL,
    kind              CHAR(1)      NOT NULL,
    is_deleted        {$boolean}   NOT NULL DEFAULT {$boolean_false},
    parent_id         BIGINT       NOT NULL,
    parent_has_access {$boolean}   NOT NULL DEFAULT {$boolean_true},
    nesting_level     {$tinyint}   NOT NULL,
	constraint acronis_db_bench_cybercache_tenants_uuid
		unique (uuid)
)
{$engine}
{$ascii};`, TableNameTenants)

// TenantsDDLClickhouse is a DDL for tenants table for Clickhouse
var TenantsDDLClickhouse = fmt.Sprintf(`CREATE TABLE %s (
    id                BIGINT       NOT NULL,
    uuid              VARCHAR(36)  NOT NULL,
    name              VARCHAR(255) NOT NULL,
    kind              CHAR(1)      NOT NULL,
    is_deleted        {$boolean}   NOT NULL DEFAULT {$boolean_false},
    parent_id         BIGINT       NOT NULL,
    parent_has_access {$boolean}   NOT NULL DEFAULT {$boolean_true},
    nesting_level     {$tinyint}   NOT NULL,
)
engine = MergeTree() ORDER BY id;`, TableNameTenants)

// TenantsDDLCassandra is a DDL for tenants table for Cassandra
var TenantsDDLCassandra = fmt.Sprintf(`CREATE TABLE %s (
    id                bigint PRIMARY KEY,
    uuid              varchar,
    name              varchar,
    kind              varchar,
    is_deleted        {$boolean},
    parent_id         bigint,
    parent_has_access {$boolean},
    nesting_level     {$tinyint},
)`, TableNameTenants)

// TenantClosureDDLSQL is a DDL for tenant_closure table for MySQL and PostgreSQL databases
var TenantClosureDDLSQL = fmt.Sprintf(`CREATE TABLE %[1]s (
    parent_id         BIGINT     NOT NULL,
    child_id          BIGINT     NOT NULL,
    parent_kind       CHAR(1)    NOT NULL,
    barrier           {$tinyint} NOT NULL DEFAULT 0,
	primary key (parent_id, child_id)
)
{$engine}
{$ascii};

CREATE INDEX cybercache_tenants_closure_child_id_idx ON %[1]s (child_id);
`, TableNameTenantClosure)

// TenantClosureDDLClickhouse is a DDL for tenant_closure table for Clickhouse
var TenantClosureDDLClickhouse = fmt.Sprintf(`CREATE TABLE %[1]s (
    parent_id         BIGINT     NOT NULL,
    child_id          BIGINT     NOT NULL,
    parent_kind       CHAR(1)    NOT NULL,
    barrier           {$tinyint} NOT NULL DEFAULT 0,
)
engine = MergeTree() ORDER BY (parent_id, child_id);`, TableNameTenantClosure)

// TenantClosureDDLCassandra is a DDL for tenant_closure table for Cassandra
var TenantClosureDDLCassandra = fmt.Sprintf(`CREATE TABLE %s (
    parent_id         BIGINT,
    child_id          BIGINT,
    parent_kind       varchar,
    barrier           {$tinyint},
    PRIMARY KEY(parent_id, child_id),
)`, TableNameTenantClosure)

// ctiEntitiesDDLSQL is a DDL for cti_entities table for MySQL and PostgreSQL databases
var ctiEntitiesDDLSQL = fmt.Sprintf(`CREATE TABLE %s
(
    uuid               VARCHAR(36)                     NOT NULL
        PRIMARY KEY,
    cti                VARCHAR(1024) {$ascii}       NOT NULL,
    kind               {$tinyint}                   NOT NULL DEFAULT 0,
    final              {$tinyint}                   NOT NULL,
    resilient          {$tinyint}                   NOT NULL DEFAULT 0,
    global_state       {$tinyint}                   NULL,
    entity_schema      {$longtext}                  NOT NULL,
    annotations        {$longtext}                  NOT NULL,
    traits             {$longtext}                  NOT NULL,
    traits_schema      {$longtext}                  NOT NULL,
    traits_annotations {$longtext}                  NOT NULL
);`,
	TableNameCtiEntities)

// ctiEntitiesDDLSQLCassandra is a DDL for cti_entities table for Cassandra
var ctiEntitiesDDLSQLCassandra = fmt.Sprintf(`CREATE TABLE %s
(
    uuid               varchar PRIMARY KEY,
    cti                varchar,
    kind               {$tinyint},
    final              {$tinyint},
    resilient          {$tinyint},
    global_state       {$tinyint},
    entity_schema      {$longtext},
    annotations        {$longtext},
    traits             {$longtext},
    traits_schema      {$longtext},
    traits_annotations {$longtext}
);`, TableNameCtiEntities)

// ctiEntitiesDDLClickhouse is a DDL for cti_entities table for Clickhouse
var ctiEntitiesDDLClickhouse = fmt.Sprintf(`CREATE TABLE %s
(
    uuid               String                       NOT NULL,
    cti                String                       NOT NULL,
    kind               {$tinyint}                   NOT NULL DEFAULT 0,
    final              {$tinyint}                   NOT NULL,
    resilient          {$tinyint}                   NOT NULL DEFAULT 0,
    global_state       {$tinyint}                   NULL,
    entity_schema      {$longtext}                  NOT NULL,
    annotations        {$longtext}                  NOT NULL,
    traits             {$longtext}                  NOT NULL,
    traits_schema      {$longtext}                  NOT NULL,
    traits_annotations {$longtext}                  NOT NULL
)
engine = MergeTree() ORDER BY uuid;`, TableNameCtiEntities)

// ctiProvisioningDDLSQL is a DDL for cti_provisioning table for MySQL and PostgreSQL databases
var ctiProvisioningDDLSQL = fmt.Sprintf(`CREATE TABLE %[1]s
(
    tenant_id              BIGINT             NOT NULL,
    cti_entity_uuid        VARCHAR(36)           NOT NULL,
    state                  {$tinyint}         NOT NULL DEFAULT -1,
    default_partner_state  {$tinyint}         NULL,
    default_customer_state {$tinyint}         NULL,
    provisioning_tenant_id BIGINT             NULL,
    deployment_tenant_id   BIGINT             NULL,
    always_on              {$tinyint}         NULL,
    PRIMARY KEY (tenant_id, cti_entity_uuid)
);

CREATE INDEX cybercache_cti_provisioning_cti_entity_id_idx
    ON %[1]s (cti_entity_uuid);

CREATE INDEX cybercache_cti_provisioning_deployment_tenant_id_idx
    ON %[1]s (deployment_tenant_id);

CREATE INDEX cybercache_cti_provisioning_provisioning_tenant_id_idx
    ON %[1]s (provisioning_tenant_id);`,
	TableNameCtiProvisioning)

// ctiProvisioningDDLSQLCassandra is a DDL for cti_provisioning table for Cassandra
var ctiProvisioningDDLSQLCassandra = fmt.Sprintf(`CREATE TABLE %s
(
    tenant_id              BIGINT,
    cti_entity_uuid        uuid,
    state                  {$tinyint},
    default_partner_state  {$tinyint},
    default_customer_state {$tinyint},
    provisioning_tenant_id BIGINT,
    deployment_tenant_id   BIGINT,
    always_on              {$tinyint},
    PRIMARY KEY (tenant_id, cti_entity_uuid)
);`, TableNameCtiProvisioning)

// ctiProvisioningDDLClickhouse is a DDL for cti_provisioning table for Clickhouse
var ctiProvisioningDDLClickhouse = fmt.Sprintf(`CREATE TABLE %[1]s
(
    tenant_id              BIGINT             NOT NULL,
    cti_entity_uuid        String             NOT NULL,
    state                  {$tinyint}         NOT NULL DEFAULT -1,
    default_partner_state  {$tinyint}         NULL,
    default_customer_state {$tinyint}         NULL,
    provisioning_tenant_id BIGINT             NULL,
    deployment_tenant_id   BIGINT             NULL,
    always_on              {$tinyint}         NULL,
    PRIMARY KEY (tenant_id, cti_entity_uuid)
)
engine = MergeTree() ORDER BY (tenant_id, cti_entity_uuid);`, TableNameCtiProvisioning)

// Init initializes tenants cache and creates tables if needed
func (tc *TenantsCache) Init(database db.Database) error {
	var dialect = database.DialectName()
	if dialect != db.CLICKHOUSE && dialect != db.CASSANDRA && dialect != db.MYSQL && dialect != db.POSTGRES && dialect != db.SQLITE {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("unsupported dialect: %s", dialect))
		return fmt.Errorf("unsupported dialect: %s", dialect)
	}

	var eventData []tenantStructureData
	if err := json.Unmarshal(tenantStructure, &eventData); err != nil {
		return fmt.Errorf("error unmarshalling tenant_structure.json: %v", err)
	}

	tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("tenants probablity config: %v", eventData))
	tc.tenantStructureRandomizer = newTenantStructureRandomizer(eventData)

	tc.logger.Log(benchmark.LogTrace, 0, "init")
	tc.CreateTables(database)
	tc.PopulateUuidsFromDB(database)
	tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("loaded %d uuids", len(tc.uuids)))
	tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("loaded %d cti uuids", len(tc.ctiUuids)))

	return nil
}

// CreateTables checks if tables created, run migrations otherwise
func (tc *TenantsCache) CreateTables(database db.Database) {
	tc.logger.Log(benchmark.LogTrace, 0, "create tenant tables")

	if exists, existsErr := database.TableExists(TableNameTenants); existsErr != nil {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error checking table %s: %v", TableNameTenants, existsErr))
		return
	} else if !exists {
		var ddlQuery string
		switch database.DialectName() {
		case db.CLICKHOUSE:
			ddlQuery = TenantsDDLClickhouse
		case db.CASSANDRA:
			ddlQuery = TenantsDDLCassandra
		default:
			ddlQuery = TenantsDDLSQL
		}

		if err := database.ApplyMigrations("", ddlQuery); err != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error creating table %s: %v", TableNameTenants, err))
			return
		}

		var session = database.Session(database.Context(context.Background()))
		if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
			_, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (id, uuid, name, kind, parent_id, nesting_level) VALUES (1, '', '/', 'r', 1, 0)", TableNameTenants))
			return err
		}); txErr != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error inserting into table %s: %v", TableNameTenants, txErr))
			return
		}
	}

	if exists, existsErr := database.TableExists(TableNameTenantClosure); existsErr != nil {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error checking table %s: %v", TableNameTenantClosure, existsErr))
		return
	} else if !exists {
		var ddlQuery string
		switch database.DialectName() {
		case db.CLICKHOUSE:
			ddlQuery = TenantClosureDDLClickhouse
		case db.CASSANDRA:
			ddlQuery = TenantClosureDDLCassandra
		default:
			ddlQuery = TenantClosureDDLSQL
		}

		if err := database.ApplyMigrations("", ddlQuery); err != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error creating table %s: %v", TableNameTenantClosure, err))
			return
		}

		var session = database.Session(database.Context(context.Background()))
		if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
			_, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (parent_id, child_id, parent_kind, barrier) VALUES (1, 1, 'r', 0)", TableNameTenantClosure))
			return err
		}); txErr != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error inserting into table %s: %v", TableNameTenantClosure, txErr))
			return
		}
	}

	if exists, existsErr := database.TableExists(TableNameCtiEntities); existsErr != nil {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error checking table %s: %v", TableNameCtiEntities, existsErr))
		return
	} else if !exists {
		var ddlQuery string
		switch database.DialectName() {
		case db.CLICKHOUSE:
			ddlQuery = ctiEntitiesDDLClickhouse
		case db.CASSANDRA:
			ddlQuery = ctiEntitiesDDLSQLCassandra
		default:
			ddlQuery = ctiEntitiesDDLSQL
		}

		if err := database.ApplyMigrations("", ddlQuery); err != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error creating table %s: %v", TableNameCtiEntities, err))
			return
		}
	}

	if exists, existsErr := database.TableExists(TableNameCtiProvisioning); existsErr != nil {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error checking table %s: %v", TableNameCtiProvisioning, existsErr))
		return
	} else if !exists {
		var ddlQuery string
		switch database.DialectName() {
		case db.CLICKHOUSE:
			ddlQuery = ctiProvisioningDDLClickhouse
		case db.CASSANDRA:
			ddlQuery = ctiProvisioningDDLSQLCassandra
		default:
			ddlQuery = ctiProvisioningDDLSQL
		}

		if err := database.ApplyMigrations("", ddlQuery); err != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error creating table %s: %v", TableNameCtiProvisioning, err))
			return
		}
	}
}

// InitTables initializes tables for tenants cache
func (tc *TenantsCache) InitTables(database db.Database) {
	tc.logger.Log(benchmark.LogTrace, 0, "init tenant tables")

	var session = database.Session(database.Context(context.Background()))
	if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
		if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (id, uuid, name, kind, parent_id, nesting_level) VALUES (1, '', '/', 'r', 1, 0)", TableNameTenants)); err != nil {
			return err
		}

		if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (parent_id, child_id, parent_kind, barrier) VALUES (1, 1, 'r', 0)", TableNameTenantClosure)); err != nil {
			return err
		}

		return nil
	}); txErr != nil {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error creating table %s: %v", TableNameTenants, txErr))
	}
}

// DropTables drops all tables created by this test
func (tc *TenantsCache) DropTables(database db.Database) {
	tc.logger.Log(benchmark.LogTrace, 0, "drop tenant tables")

	for _, table := range []string{TableNameTenants, TableNameTenantClosure, TableNameCtiEntities, TableNameCtiProvisioning} {
		if droppedErr := database.DropTable(table); droppedErr != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error dropping table %s: %v", table, droppedErr))
		}
	}
	if database.UseTruncate() {
		tc.InitTables(database)
	}
}

// PopulateUuidsFromDB populates uuids from DB table acronis_db_bench_cybercache_tenants
func (tc *TenantsCache) PopulateUuidsFromDB(database db.Database) {
	tc.logger.Log(benchmark.LogTrace, 0, "populating tenant uuids from DB")

	var session = database.Session(database.Context(context.Background()))

	var rows, err = session.Query(fmt.Sprintf("SELECT uuid, id, kind, nesting_level FROM %s", TableNameTenants))
	if err != nil {
		return
	}

	rand := tc.tenantStructureRandomizer
	for rows.Next() {
		var t TenantObj
		if err = rows.Scan(&t.UUID, &t.ID, &t.Kind, &t.NestingLevel); err != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error scanning row: %v", err))
			return
		}
		tc.uuids = append(tc.uuids, t.UUID)

		if t.Kind == "c" {
			tc.customerUUIDS = append(tc.customerUUIDS, t.UUID)
		} else if t.Kind == "p" {
			tc.partnerUUIDS = append(tc.partnerUUIDS, t.UUID)
		}

		rand.storeCreatedTenant(&t)
	}

	for _, storedUUID := range tc.uuids {
		if storedUUID == "" {
			continue
		}

		rows, err = session.Query(fmt.Sprintf(`
			SELECT parents.uuid
			FROM %s ten
					 LEFT JOIN %s cl ON ten.id = cl.child_id
					 LEFT JOIN %s parents ON cl.parent_id = parents.id
			WHERE ten.uuid = '%s'
			ORDER BY parents.nesting_level DESC;
			`, TableNameTenants, TableNameTenantClosure, TableNameTenants, storedUUID))
		if err != nil {
			return
		}

		var parents []TenantUUID
		for rows.Next() {
			var parentUUID string
			if err = rows.Scan(&parentUUID); err != nil {
				tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error scanning row: %v", err))
				return
			}
			if parentUUID != "" {
				parents = append(parents, TenantUUID(parentUUID))
			}
		}

		tc.parentUUIDs[string(storedUUID)] = parents
	}

	rand.currentID = int64(getMax(database, "id"))
	rand.maxLevel = getMax(database, "nesting_level")
	if rand.maxLevel >= len(rand.levelTotal) {
		rand.maxLevel = len(rand.levelTotal) - 1
	}

	var ctiRows db.Rows
	if ctiRows, err = session.Query(fmt.Sprintf("SELECT uuid FROM %s;", TableNameCtiEntities)); err != nil {
		return
	}

	for ctiRows.Next() {
		var uuid CTIUUID
		err = ctiRows.Scan(&uuid)
		if err != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error scanning row: %v", err))
			return
		}
		tc.ctiUuids = append(tc.ctiUuids, uuid)
	}
}

// TenantClosureObj is a struct for tenant_closure object in DB table acronis_db_bench_cybercache_tenant_closure
type TenantClosureObj struct {
	ParentID   int64  `db:"parent_id"`
	ChildID    int64  `db:"child_id"`
	ParentKind string `db:"parent_kind"`
	Barrier    int    `db:"barrier"`
}

// CreateTenant creates a new tenant and inserts it into DB
func (tc *TenantsCache) CreateTenant(rw *benchmark.RandomizerWorker, tx db.DatabaseAccessor) (TenantUUID, error) {
	t, err := tc.createRandomTenant(rw)
	if err != nil {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error creating tenant: %v", err))
		return "", err
	}

	var tenantUuid, _ = guuid.ParseBytes([]byte(t.UUID))
	if err = tx.BulkInsert(TableNameTenants, [][]interface{}{
		{t.ID, tenantUuid, t.Name, t.Kind, t.ParentID, t.NestingLevel, t.IsDeleted, t.ParentHasAccess},
	}, []string{"id", "uuid", "name", "kind", "parent_id", "nesting_level", "is_deleted", "parent_has_access"}); err != nil {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error inserting into table %s: %v", TableNameTenants, err))
		return "", err
	}

	tc.uuids = append(tc.uuids, t.UUID)
	tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("creating a tenant: %v", t))

	var tcToCreate [][]interface{}
	newTenantClosure := TenantClosureObj{ParentID: t.ID, ChildID: t.ID, ParentKind: t.Kind, Barrier: 0}
	tcToCreate = append(tcToCreate, []interface{}{
		newTenantClosure.ParentID,
		newTenantClosure.ChildID,
		newTenantClosure.ParentKind,
		newTenantClosure.Barrier},
	)

	rows, err := tx.Query(fmt.Sprintf("SELECT parent_id, parent_kind, barrier FROM %s WHERE child_id = %d", TableNameTenantClosure, t.ParentID))
	if err != nil {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error selecting from table %s: %v", TableNameTenantClosure, err))
		return "", err
	}

	for rows.Next() {
		var tco TenantClosureObj
		err = rows.Scan(&tco.ParentID, &tco.ParentKind, &tco.Barrier)
		if err != nil {
			tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error scanning row: %v", err))
			return "", err
		}
		tco.ChildID = t.ID
		// get maximum of tc.Barrier, newTenantClosure.Barrier
		tco.Barrier = Max(tco.Barrier, newTenantClosure.Barrier)

		tcToCreate = append(tcToCreate, []interface{}{
			tco.ParentID,
			tco.ChildID,
			tco.ParentKind,
			tco.Barrier,
		})
	}

	if err = tx.BulkInsert(TableNameTenantClosure, tcToCreate,
		[]string{"parent_id", "child_id", "parent_kind", "barrier"}); err != nil {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("error inserting into table %s: %v", TableNameTenantClosure, err))
		return "", err
	}

	tc.tenantStructureRandomizer.storeCreatedTenant(t)

	return t.UUID, nil
}

// CreateCTIEntity creates a new CTI entity and inserts it into DB
func (tc *TenantsCache) CreateCTIEntity(rw *benchmark.RandomizerWorker, tx db.DatabaseAccessor) error {
	cti, err := tc.createRandomCtiEntity(rw)
	tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("creating a cti entity: %v", cti))
	if err != nil {
		return fmt.Errorf("error creating cti entity: %v", err)
	}

	cti.GlobalState = 1

	var ctiUUID, _ = guuid.ParseBytes([]byte(cti.UUID))
	if err = tx.BulkInsert(TableNameCtiEntities, [][]interface{}{
		{ctiUUID, cti.CTI, cti.Final, cti.GlobalState, cti.EntitySchema, cti.Annotations, cti.Traits, cti.TraitsSchema, cti.TraitsAnnotations},
	}, []string{"uuid", "cti", "final", "global_state", "entity_schema", "annotations", "traits", "traits_schema", "traits_annotations"}); err != nil {
		return fmt.Errorf("error inserting into table %s: %v", TableNameCtiEntities, err)
	}

	tc.ctiUuids = append(tc.ctiUuids, cti.UUID)

	return nil
}

// letterBytesCTI is a set of letters for generating random CTI
const letterBytesCTI = "abcdefghijklmnopqrstuvwxyz....._____~~~~~-0123456789"

// genRandCtiStr generates random CTI string with prefix
func (tc *TenantsCache) genRandCtiStr(rw *benchmark.RandomizerWorker) string {
	prefix := "cti.a.p."
	prefixLen := len(prefix)

	bytes := make([]byte, prefixLen+rw.Intn(512-prefixLen))
	for i := range bytes {
		bytes[i] = letterBytesCTI[rw.Intn(len(letterBytesCTI))]
	}

	return prefix + string(bytes)
}

// createRandomCtiEntity creates a new CTI entity and inserts it into DB
func (tc *TenantsCache) createRandomCtiEntity(rw *benchmark.RandomizerWorker) (*CtiEntityObj, error) { //nolint:unparam
	cti := CtiEntityObj{
		UUID: CTIUUID(rw.UUID().String()),
		CTI:  tc.genRandCtiStr(rw),
	}

	return &cti, nil
}

// getMax returns max value of given field from DB table acronis_db_bench_cybercache_tenants
func getMax(database db.Database, field string) int {
	var session = database.Session(database.Context(context.Background()))

	var maxRows, err = session.Query(fmt.Sprintf("SELECT COALESCE(MAX(%s),0) FROM %s;", field, TableNameTenants))
	if err != nil {
		return 0
	}

	var vMax int
	for maxRows.Next() {
		err = maxRows.Scan(&vMax)
		if err != nil {
			maxRows.Close() //nolint:errcheck,gosec
			return 0
		}
		maxRows.Close() //nolint:errcheck,gosec

		break //nolint:staticcheck
	}

	return vMax
}

/*
file contains result of query:
SELECT nesting_level, kind, COUNT(*) AS weight
FROM groups GROUP BY nesting_level, kind
WHERE nesting_level > 0;
from us2 db
*/
//go:embed tenant_structure.json
var tenantStructure []byte

// tenantStructureData is a struct for tenant structure data
type tenantStructureData struct {
	Kind         int `json:"kind"`
	NestingLevel int `json:"nesting_level"`
	Weight       int `json:"weight"`
}

// tenantStructureRandomizer is a struct for tenant structure randomizer
type tenantStructureRandomizer struct {
	weightSums                 []int
	levelTotal                 []int
	weightSumToTenantStructure map[int]tenantStructureData
	totalWeight                int
	maxLevel                   int
	currentID                  int64
	levelKindIDMap             sync.Map
}

// newTenantStructureRandomizer creates a new tenant structure randomizer
func newTenantStructureRandomizer(data []tenantStructureData) *tenantStructureRandomizer {
	r := &tenantStructureRandomizer{}
	currentWeightSum := 0
	currentLevel := 1
	r.weightSumToTenantStructure = make(map[int]tenantStructureData)
	for _, d := range data {
		if d.NestingLevel > currentLevel {
			r.levelTotal = append(r.levelTotal, currentWeightSum)
			currentLevel = d.NestingLevel
		}
		currentWeightSum += d.Weight
		r.weightSums = append(r.weightSums, currentWeightSum)
		r.weightSumToTenantStructure[currentWeightSum] = d
	}
	r.levelTotal = append(r.levelTotal, currentWeightSum)
	r.totalWeight = currentWeightSum
	r.maxLevel = 0
	r.levelKindIDMap = sync.Map{}

	return r
}

// getRandomTenantStructure returns random tenant structure
func (r *tenantStructureRandomizer) getRandomTenantStructure(rw *benchmark.RandomizerWorker) tenantStructureData {
	randomWeight := rw.Intn(r.levelTotal[r.maxLevel])
	// use binary search to find the first element in weightSums that is greater than randomWeight
	// then return the corresponding tenant structure
	index := sort.Search(len(r.weightSums), func(i int) bool { return r.weightSums[i] >= randomWeight })
	randomStructure := r.weightSumToTenantStructure[r.weightSums[index]]

	return randomStructure
}

// ConcurrentIDList is a struct for concurrent ID list
type ConcurrentIDList struct {
	sync.RWMutex
	items []int64
}

// storeCreatedTenant stores created tenant in tenantStructureRandomizer
func (r *tenantStructureRandomizer) storeCreatedTenant(t *TenantObj) {
	_, ok := r.levelKindIDMap.Load(t.NestingLevel)
	if !ok {
		r.levelKindIDMap.Store(t.NestingLevel, &sync.Map{})
	}
	levelMap, _ := r.levelKindIDMap.LoadOrStore(t.NestingLevel, &sync.Map{})
	kindMap, _ := (levelMap.(*sync.Map)).LoadOrStore(t.Kind, &ConcurrentIDList{})
	idList, _ := kindMap.(*ConcurrentIDList)
	idList.Lock()
	idList.items = append(idList.items, t.ID)
	if t.NestingLevel > r.maxLevel && r.maxLevel < len(r.levelTotal)-1 {
		r.maxLevel = t.NestingLevel
	}
	idList.Unlock()
}

// findParent finds parent for tenant
func (r *tenantStructureRandomizer) findParent(rw *benchmark.RandomizerWorker, level int, kind string) int64 {
	possibleParents, ok := r.levelKindIDMap.Load(level - 1)
	if !ok {
		return -1
	}
	var filteredParents []int64
	var possibleParentKind []string
	if kind == "u" {
		possibleParentKind = []string{"c", "u"}
	} else {
		possibleParentKind = []string{"r", "p", "f"}
	}
	for _, k := range possibleParentKind {
		possibleParent, ok := possibleParents.(*sync.Map).Load(k)
		if !ok {
			continue
		}
		idList, _ := possibleParent.(*ConcurrentIDList)
		filteredParents = append(filteredParents, idList.items...)
	}
	if len(filteredParents) == 0 {
		return -1
	}
	index := rw.Intn(len(filteredParents))

	return filteredParents[index]
}

// createRandomTenant creates a new tenant and inserts it into DB
func (tc *TenantsCache) createRandomTenant(rw *benchmark.RandomizerWorker) (*TenantObj, error) {
	rnd := tc.tenantStructureRandomizer
	var kind string
	var err error
	var parentID int64 = -1
	var r tenantStructureData
	canRecureLevel := true
	for canRecureLevel {
		for i := 0; i < 10; i++ {
			r = rnd.getRandomTenantStructure(rw)
			kind, err = convertIntToKind(r.Kind)
			if err != nil {
				return nil, err
			}
			parentID = rnd.findParent(rw, r.NestingLevel, kind)
			if parentID != -1 {
				break
			}
		}
		if parentID != -1 {
			break
		}
		if rnd.maxLevel > 1 {
			rnd.maxLevel--
		} else {
			canRecureLevel = false
		}
	}
	if parentID == -1 {
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("could not find parent for kind %s, nesting level: %d, randomizer: %+v", kind, r.NestingLevel, rnd))
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("weightSums: %+v", rnd.weightSums))
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("maxLevel: %+v", rnd.maxLevel))
		tc.logger.Log(benchmark.LogTrace, 0, fmt.Sprintf("currentID: %+v", rnd.currentID))

		return nil, errors.New("could not find parent")
	}

	uuid := guuid.New().String()
	newID := atomic.AddInt64(&rnd.currentID, 1)
	t := TenantObj{
		ID:              newID,
		UUID:            TenantUUID(uuid),
		Name:            uuid,
		Kind:            kind,
		ParentID:        parentID,
		NestingLevel:    r.NestingLevel,
		IsDeleted:       false,
		ParentHasAccess: true,
	}

	return &t, nil
}

// Min returns min value of two integers
func Min(x, y int) int {
	if x > y {
		return y
	}

	return x
}

// Max returns max value of two integers
func Max(x, y int) int {
	if x < y {
		return y
	}

	return x
}

// convertIntToKind converts integer to tenant kind
func convertIntToKind(kind int) (string, error) {
	switch kind {
	case 0:
		// root
		return "r", nil
	case 31:
		// partner
		return "p", nil
	case 35:
		// folder
		return "f", nil
	case 40:
		// customer
		return "c", nil
	case 50:
		// unit
		return "u", nil
	default:
		return "", fmt.Errorf("unknown tenant kind %d", kind)
	}
}

// GetRandomTenantUUID returns random tenant uuid from cache
func (tc *TenantsCache) GetRandomTenantUUID(rw *benchmark.RandomizerWorker, testCardinality int, kind string) (guuid.UUID, error) {
	var cardinality int
	if testCardinality == 0 {
		cardinality = tc.tenantsWorkingSetLimit
	} else {
		cardinality = Min(testCardinality, tc.tenantsWorkingSetLimit)
	}
	cardinality = Max(1, cardinality)

	var tenantList []TenantUUID
	switch kind {
	case "c":
		tenantList = tc.customerUUIDS
	case "p":
		tenantList = tc.partnerUUIDS
	default:
		tenantList = tc.uuids
	}

	limit := len(tenantList)

	if limit < cardinality {
		msg := fmt.Sprintf("TEST ABORTED: The tenants hierarchy has %d tenants, while at least %d required for working set\n", limit, tc.tenantsWorkingSetLimit)
		if testCardinality == 0 {
			msg += "Add tenants by '-t insert-tenant' first or use --tenants-working-set to reduce used working set\n"
		} else {
			msg += "Add tenants by '-t insert-tenant' first\n"
		}
		tc.Exit(msg)
	}

	var value, _ = guuid.ParseBytes([]byte(tenantList[rw.IntnExp(cardinality)]))
	return value, nil
}

// GetTenantUuidBoundId returns tenant uuid bound id
/*
 * Generate a new UUID based on the UUID prefix coming from the TenantUUID
 * This logic is required to simulate a low cardinality objects associated with a tenant
 */
func (tc *TenantsCache) GetTenantUuidBoundId(rw *benchmark.RandomizerWorker, tenantUuid guuid.UUID, cardinality int) guuid.UUID { //nolint:revive
	var s = tenantUuid.String()

	var res string
	if len(s) > 0 {
		res = s[:len(s)-12] + fmt.Sprintf("%012d", rw.Intn(cardinality))
	} else {
		res = fmt.Sprintf("00000000-0000-0000-0000-%012d", rw.Intn(cardinality))
	}

	var value, _ = guuid.ParseBytes([]byte(res))
	return value
}

// GetRandomCTIUUID returns random CTI uuid from cache
func (tc *TenantsCache) GetRandomCTIUUID(rw *benchmark.RandomizerWorker, testCardinality int) (guuid.UUID, error) {
	var cardinality int
	if testCardinality == 0 {
		cardinality = tc.ctisWorkingSetLimit
	} else {
		cardinality = Min(testCardinality, tc.ctisWorkingSetLimit)
	}

	cardinality = Max(1, cardinality)
	limit := len(tc.ctiUuids)

	if limit < cardinality {
		msg := fmt.Sprintf("TEST ABORTED: The CTI entity cache has %d entities, while at least %d required\n", len(tc.ctiUuids), cardinality)
		if cardinality == 0 {
			msg += "Add CTI entities by the '-t insert-cti' first or use --ctis-working-set to reduce used working set\n"
		} else {
			msg += "Add CTI entities by the '-t insert-cti' test first\n"
		}
		tc.Exit(msg)
	}

	var value, _ = guuid.ParseBytes([]byte(tc.ctiUuids[rw.IntnExp(cardinality)]))
	return value, nil
}

func (tc *TenantsCache) GenCommonFakeValue(columnType string, rw *benchmark.RandomizerWorker, cardinality int) (bool, interface{}) {
	if columnType != "tenant_uuid" && columnType != "customer_uuid" && columnType != "partner_uuid" {
		return false, nil
	}

	var kind string
	switch columnType {
	case "customer_uuid":
		kind = "c"
	case "partner_uuid":
		kind = "p"
	}

	var tenantUUID, err = tc.GetRandomTenantUUID(rw, cardinality, kind)
	if err != nil {
		tc.benchmark.Exit(err.Error())
	}

	return true, tenantUUID
}

func (tc *TenantsCache) GenFakeValue(columnType string, rw *benchmark.RandomizerWorker, cardinality int, preGenerated map[string]interface{}) (bool, interface{}) {
	var tenantUUID guuid.UUID
	if preGenerated != nil {
		if rawTenantUUID, ok := preGenerated["tenant_uuid"]; ok {
			tenantUUID = rawTenantUUID.(guuid.UUID)
		} else if rawTenantUUID, ok = preGenerated["customer_uuid"]; ok {
			tenantUUID = rawTenantUUID.(guuid.UUID)
		} else if rawTenantUUID, ok = preGenerated["partner_uuid"]; ok {
			tenantUUID = rawTenantUUID.(guuid.UUID)
		}
	}

	switch columnType {
	case "tenant_uuid", "customer_uuid", "partner_uuid":
		return true, tenantUUID
	case "tenant_uuid_parents":
		return true, tc.parentUUIDs[tenantUUID.String()]
	case "tenant_uuid_bound_id":
		return true, tc.GetTenantUuidBoundId(rw, tenantUUID, cardinality)
	case "cti_uuid":
		ret, err := tc.GetRandomCTIUUID(rw, cardinality)
		if err != nil {
			tc.benchmark.Exit(err.Error())
		}

		return true, ret
	default:
		return false, nil
	}
}
