package nested_objects_search

import (
	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var tg = engine.NewTestGroup("Nested objects search tests group")

	// Elasticsearch nested tests
	tg.Add(&TestInsertEmailNested)
	tg.Add(&TestAddHistoryToEmailNested)
	tg.Add(&TestDeleteHistoryToEmailNested)
	tg.Add(&TestSearchEmailNested)
	tg.Add(&TestListEmailNested)

	// Elasticsearch parent-child tests
	tg.Add(&TestInsertEmailParentChild)
	tg.Add(&TestAddHistoryToEmailParentChild)
	tg.Add(&TestDeleteHistoryToEmailParentChild)
	tg.Add(&TestSearchEmailPC)
	tg.Add(&TestListEmailPC)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}

// TestInsertEmailNested inserts email data into the 'email_nested' table
var TestInsertEmailNested = engine.TestDesc{
	Name:        "insert-email-nested",
	Metric:      "emails/sec",
	Description: "insert an email data into the 'email_nested' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailNested,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testInsertElasticsearch(b, testDesc)
	},
}

// TestAddHistoryToEmailNested update email data with history into the 'email_nested' table
var TestAddHistoryToEmailNested = engine.TestDesc{
	Name:        "add-email-history-nested",
	Metric:      "histories/sec",
	Description: "add history of an email in the 'email_nested' table",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailNested,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testAddHistoryNestedElasticsearch(b, testDesc)
	},
}

// TestDeleteHistoryToEmailNested delete history of email data into the 'email_nested' table
var TestDeleteHistoryToEmailNested = engine.TestDesc{
	Name:        "delete-email-history-nested",
	Metric:      "histories/sec",
	Description: "add history of an email in the 'email_nested' table",
	Category:    engine.TestDelete,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailNested,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testDeleteHistoryNestedES(b, testDesc)
	},
}

// TestSearchEmailNested selects all emails with a specific history type on nested mapping
var TestSearchEmailNested = engine.TestDesc{
	Name:        "search-email-nested",
	Metric:      "emails/sec",
	Description: "select all emails with a specific history type",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailNested,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testSearchNestedElasticsearch(b, testDesc)
	},
}

// TestListEmailNested list all emails with their history list on nested mapping
var TestListEmailNested = engine.TestDesc{
	Name:        "list-email-nested",
	Metric:      "emails/sec",
	Description: "list all emails with its history details",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailNested,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testListNestedElasticsearch(b, testDesc)
	},
}

// TestInsertEmailParentChild inserts email data into the 'email_parent_child' table
var TestInsertEmailParentChild = engine.TestDesc{
	Name:        "insert-email-parent-child",
	Metric:      "emails/sec",
	Description: "insert an email data into the 'email_pc' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailParentChild,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testInsertPCElasticsearch(b, testDesc)
	},
}

// TestAddHistoryToEmailParentChild update email data with history into the 'email_pc' table
var TestAddHistoryToEmailParentChild = engine.TestDesc{
	Name:        "add-email-history-parent-child",
	Metric:      "histories/sec",
	Description: "add history of an email in the 'email_pc' table",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailParentChild,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testAddHistoryPCElasticsearch(b, testDesc)
	},
}

// TestDeleteHistoryToEmailParentChild delete history of email data into the 'email_pc' table
var TestDeleteHistoryToEmailParentChild = engine.TestDesc{
	Name:        "delete-email-history-parent-child",
	Metric:      "histories/sec",
	Description: "add history of an email in the 'email_pc' table",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailParentChild,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testDeleteHistoryPCES(b, testDesc)
	},
}

// TestSearchEmailPC selects all emails with a specific history type on parent-child mapping
var TestSearchEmailPC = engine.TestDesc{
	Name:        "search-email-parent-child",
	Metric:      "emails/sec",
	Description: "select all emails with a specific history type",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailParentChild,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testSearchPCElasticsearch(b, testDesc)
	},
}

// TestListEmailPC list all emails with their history list on parent-child mapping
var TestListEmailPC = engine.TestDesc{
	Name:        "list-email-parent-child",
	Metric:      "emails/sec",
	Description: "list all emails with its history details",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	Databases:   []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Table:       TestTableEmailParentChild,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testListPCElasticsearch(b, testDesc)
	},
}
