package engine

import (
	"fmt"
	"os"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
)

// CreateQueryPatchFunc is a function to patch create query for specific DB
type CreateQueryPatchFunc func(table string, query string, dialect db.DialectName) (string, error)

// TestTable represents table to be used in tests and benchmarks
type TestTable struct {
	TableName             string
	Databases             []db.DialectName
	ColumnsConf           []benchmark.DBFakeColumnConf
	Columns               [][]interface{}
	InsertColumns         []string
	UpdateColumns         []string
	TableDefinition       func(dialect db.DialectName) *db.TableDefinition
	CreateQuery           string
	CreateQueryPatchFuncs []CreateQueryPatchFunc
	Indexes               [][]string
	DisableAutoCreation   bool

	// runtime information
	RowsCount uint64
}

// dbIsSupported returns true if the database is supported by the test
func (t *TestTable) dbIsSupported(db db.DialectName) bool {
	for _, b := range t.Databases {
		if b == db {
			return true
		}
	}

	return false
}

func exit(fmts string, args ...interface{}) {
	fmt.Printf(fmts, args...)
	fmt.Println()
	os.Exit(127)
}

func castInterface2ColumnsConf(columns [][]interface{}) []benchmark.DBFakeColumnConf {
	// columns - is an array of fields:
	// {
	//   "column name", # mandatory, represents real DB colum name
	//   "column type", # mandatory, represents data type supported by benchmark/faker.go
	//   "cardinality", # optional, represents data cardinality (e.g. number of combinations)
	//   "max size",    # optional, represents max data field value length (e.g. max string length)
	//   "min size",    # optional, represents min data field value length (e.g. min string length)
	// }
	ret := make([]benchmark.DBFakeColumnConf, 0, len(columns))
	for _, c := range columns {
		var cc benchmark.DBFakeColumnConf
		var ok bool

		cc.ColumnName, ok = c[0].(string)
		if !ok {
			exit("can't cast value %v to ColumnName", c[0])
		}

		cc.ColumnType, ok = c[1].(string)
		if !ok {
			exit("can't cast value %v to ColumnType", c[1])
		}

		l := len(c)
		if l > 2 {
			cc.Cardinality, ok = c[2].(int)
			if !ok {
				exit("can't cast value %v to Cardinality", c[2])
			}
		}
		if l > 3 {
			cc.MaxSize, ok = c[3].(int)
			if !ok {
				exit("can't cast value %v to MaxSize", c[3])
			}
		}
		if l > 4 {
			cc.MinSize, ok = c[4].(int)
			if !ok {
				exit("can't cast value %v to MinSize", c[4])
			}
		}
		ret = append(ret, cc)
	}

	return ret
}

// InitColumnsConf initializes ColumnsConf field based on provided columns
func (t *TestTable) InitColumnsConf() {
	if t.ColumnsConf == nil {
		t.ColumnsConf = castInterface2ColumnsConf(t.Columns)
	}
}

// GetColumnsConf returns columns for insert or update operations based on provided list of columns
func (t *TestTable) GetColumnsConf(columns []string, withAutoInc bool) *[]benchmark.DBFakeColumnConf {
	t.InitColumnsConf()

	var colConfs []benchmark.DBFakeColumnConf

	// empty list means any column is required
	if len(columns) == 0 {
		if withAutoInc {
			return &t.ColumnsConf
		}

		for _, c := range t.ColumnsConf {
			// skip autoinc
			if c.ColumnType == "autoinc" {
				continue
			}
			colConfs = append(colConfs, c)
		}
	}

	for _, c := range t.ColumnsConf {
		if c.ColumnType == "autoinc" && !withAutoInc {
			continue
		}
		for _, v := range columns {
			if v == c.ColumnName {
				colConfs = append(colConfs, c)
			}
		}
	}

	return &colConfs
}

// GetColumnsForInsert returns columns for insert
func (t *TestTable) GetColumnsForInsert(withAutoInc bool) *[]benchmark.DBFakeColumnConf {
	return t.GetColumnsConf(t.InsertColumns, withAutoInc)
}

// GetColumnsForUpdate returns columns for update
func (t *TestTable) GetColumnsForUpdate(withAutoInc bool) *[]benchmark.DBFakeColumnConf {
	return t.GetColumnsConf(t.UpdateColumns, withAutoInc)
}

// Create creates table in DB using provided DBConnector
func (t *TestTable) Create(c *DBConnector, b *benchmark.Benchmark) {
	if t.TableName == "" {
		return
	}

	if t.TableDefinition != nil {
		var table = t.TableDefinition(c.Database.DialectName())
		if err := c.Database.CreateTable(t.TableName, table, ""); err != nil {
			b.Exit(err.Error())
		}
	} else {
		if t.CreateQuery == "" {
			b.Logger.Error("no create query for '%s'", t.TableName)
			// b.Exit("internal error: no migration provided for table %s creation", t.TableName)
			return
		}
		tableCreationQuery := t.CreateQuery

		var err error

		for _, patch := range t.CreateQueryPatchFuncs {
			tableCreationQuery, err = patch(t.TableName, tableCreationQuery, c.Database.DialectName())
			if err != nil {
				b.Exit(err.Error())
			}
		}

		if err = c.Database.CreateTable(t.TableName, nil, tableCreationQuery); err != nil {
			b.Exit(err.Error())
		}
	}

	for _, columns := range t.Indexes {
		c.Database.CreateIndex(fmt.Sprintf("%s_%s_idx", t.TableName, strings.Join(columns, "_")), t.TableName, columns, db.IndexTypeBtree)
	}
}
