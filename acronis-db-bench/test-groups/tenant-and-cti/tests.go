package tenant_and_cti

import (
	"context"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var tg = engine.NewTestGroup("Tenant and CTI tests group")

	// Tenant and CTI tests
	tg.Add(&TestInsertTenant)
	tg.Add(&TestInsertCTI)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}

// TestTableTenants is table to store tenants
var TestTableTenants = engine.TestTable{}

// TestTableTenantsClosure is table to store tenants closure
var TestTableTenantsClosure = engine.TestTable{}

// TestTableCTIEntities is table to store CTI entities
var TestTableCTIEntities = engine.TestTable{}

/*
 * Tenant-specific tests
 */

// TestInsertTenant inserts into the 'tenants' table
var TestInsertTenant = engine.TestDesc{
	Name:        "insert-tenant",
	Metric:      "tenants/sec",
	Description: "insert a tenant into the 'tenants' table",
	Category:    engine.TestInsert,
	Databases:   engine.ALL,
	Table:       TestTableTenants,
	IsReadonly:  false,
	IsDBRTest:   false,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, CreateTenantWorker, 0)
	},
}

// CreateTenantWorker creates a tenant and optionally inserts an event into the event bus
func CreateTenantWorker(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
	var session = c.Database.Session(c.Database.Context(context.Background(), false))
	if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
		for i := 0; i < batch; i++ {
			var tenantUUID, err = b.Vault.(*engine.DBTestData).TenantsCache.CreateTenant(b.Randomizer, tx)
			if err != nil {
				return err
			}

			if b.TestOpts.(*engine.TestOpts).BenchOpts.Events {
				if err = b.Vault.(*engine.DBTestData).EventBus.InsertEvent(b.Randomizer, tx, string(tenantUUID)); err != nil {
					return err
				}
			}
		}

		return nil
	}); txErr != nil {
		c.Exit(txErr.Error())
	}

	return batch
}

// TestInsertCTI inserts into the 'cti' table
var TestInsertCTI = engine.TestDesc{
	Name:        "insert-cti",
	Metric:      "ctiEntity/sec",
	Description: "insert a CTI entity into the 'cti' table",
	Category:    engine.TestInsert,
	Databases:   engine.ALL,
	Table:       TestTableCTIEntities,
	IsReadonly:  false,
	IsDBRTest:   false,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, CreateCTIEntityWorker, 0)
	},
}

func CreateCTIEntityWorker(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
	var session = c.Database.Session(c.Database.Context(context.Background(), false))
	if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
		for i := 0; i < batch; i++ {
			if err := b.Vault.(*engine.DBTestData).TenantsCache.CreateCTIEntity(b.Randomizer, tx); err != nil {
				return err
			}
		}

		return nil
	}); txErr != nil {
		c.Exit(txErr.Error())
	}

	return batch
}
