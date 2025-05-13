package basic_scenarios

import "github.com/acronis/perfkit/acronis-db-bench/engine"

func init() {
	var tg = engine.NewTestGroup("Base tests group")

	tg.Add(&TestInsertLight)
	tg.Add(&TestInsertLightPrepared)
	tg.Add(&TestInsertLightMultiValue)
	tg.Add(&TestCopyLight)
	tg.Add(&TestInsertMedium)
	tg.Add(&TestInsertMediumPrepared)
	tg.Add(&TestInsertMediumMultiValue)
	tg.Add(&TestCopyMedium)
	tg.Add(&TestUpdateMedium)
	tg.Add(&TestSelectMediumLastTenant)
	tg.Add(&TestSelectMediumLast)
	tg.Add(&TestSelectMediumRand)
	tg.Add(&TestInsertHeavy)
	tg.Add(&TestInsertHeavyPrepared)
	tg.Add(&TestInsertHeavyMultivalue)
	tg.Add(&TestCopyHeavy)
	tg.Add(&TestUpdateHeavy)
	tg.Add(&TestUpdateHeavyBulk)
	tg.Add(&TestUpdateHeavySameVal)
	tg.Add(&TestUpdateHeavyPartialSameVal)
	tg.Add(&TestSelectHeavyLast)
	tg.Add(&TestSelectHeavyRand)
	tg.Add(&TestSelectHeavyRandTenantLike)
	tg.Add(&TestSelectHeavyMinMaxTenant)
	tg.Add(&TestSelectHeavyMinMaxTenantAndState)
	tg.Add(&TestSelectHeavyForUpdateSkipLocked)
	tg.Add(&TestSelectHeavyLastTenant)
	tg.Add(&TestSelectHeavyLastTenantCTI)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}
