package basic_scenarios

import (
	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	tests := []*engine.TestDesc{
		// Light table tests
		&TestInsertLight,
		&TestInsertLightPrepared,
		&TestInsertLightMultiValue,
		&TestCopyLight,
		&TestInsertLightDBR,

		// Medium table tests
		&TestInsertMedium,
		&TestInsertMediumPrepared,
		&TestInsertMediumMultiValue,
		&TestCopyMedium,
		&TestInsertMediumDBR,
		&TestUpdateMedium,
		&TestUpdateMediumDBR,
		&TestSelectMediumLastDBR,
		&TestSelectMediumRand,
		&TestSelectMediumRandDBR,

		// Heavy table tests
		&TestInsertHeavy,
		&TestInsertHeavyPrepared,
		&TestInsertHeavyMultivalue,
		&TestCopyHeavy,
		&TestInsertHeavyDBR,
		&TestUpdateHeavy,
		&TestUpdateHeavyDBR,
		&TestUpdateHeavyBulk,
		&TestUpdateHeavyBulkDBR,
		&TestUpdateHeavySameVal,
		&TestUpdateHeavyPartialSameVal,
		&TestSelectHeavyLast,
		&TestSelectHeavyLastDBR,
		&TestSelectHeavyRand,
		&TestSelectHeavyMinMaxTenant,
		&TestSelectHeavyMinMaxTenantAndState,
		&TestSelectHeavyLastTenant,
		&TestSelectHeavyLastTenantCTI,

		// Blob table tests
		&TestInsertBlob,
		&TestCopyBlob,
	}

	tables := map[string]engine.TestTable{
		TestTableLight.TableName:  TestTableLight,
		TestTableMedium.TableName: TestTableMedium,
		TestTableHeavy.TableName:  TestTableHeavy,
		TestTableBlob.TableName:   TestTableBlob,
	}

	scenario := &engine.TestScenario{
		Name:   "basic-scenarios",
		Tests:  tests,
		Tables: tables,
	}

	if err := engine.RegisterTestScenario(scenario); err != nil {
		panic(err)
	}
}
