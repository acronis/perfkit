package benchmark

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

/*
 * Random value generators
 */

// letterBytes is used for random string generation
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// cardinalityCacheType is a struct for storing cardinality cache data (for random string generation)
type cardinalityCacheType struct {
	lock     sync.RWMutex
	entities map[string][]string
}

// cardinalityCache is a global variable for storing cardinality cache data (for random string generation)
var cardinalityCache = &cardinalityCacheType{entities: make(map[string][]string)}

// randStringWithCardinality generates random string with cardinality
func (cc *cardinalityCacheType) randStringWithCardinality(randID int, pfx string, cardinality int, maxsize int, minsize int) string {
	index := fmt.Sprintf("%s-%d-%d-%d", pfx, cardinality, maxsize, minsize)

	cc.lock.RLock()
	_, exists := cc.entities[index]
	if !exists {
		cc.lock.RUnlock()

		rs := rand.NewSource(int64(cardinality + maxsize - minsize))
		rr := rand.New(rs)

		cc.lock.Lock()

		_, exists = cc.entities[index]
		if !exists {
			var entities []string
			for n := 0; n < cardinality; n++ {
				bytes := make([]byte, rr.Intn(maxsize-minsize-len(pfx))+minsize)
				l := len(letterBytes)
				for i := range bytes {
					bytes[i] = letterBytes[rr.Intn(l)]
				}
				entities = append(entities, pfx+string(bytes))
			}
			cc.entities[index] = entities
		}

		cc.lock.Unlock()
		cc.lock.RLock()
	}
	ret := cc.entities[index][randID]
	cc.lock.RUnlock()

	return ret
}

// RandStringBytes generates random string with given length and other parameters
func (rz *Randomizer) RandStringBytes(pfx string, cardinality int, maxsize int, minsize int, seeded bool) string {
	if maxsize == minsize {
		return ""
	}
	if cardinality != 0 {
		return cardinalityCache.randStringWithCardinality(rz.Intn(cardinality), pfx, cardinality, maxsize, minsize)
	}

	var bytes []byte
	l := len(letterBytes)

	if seeded {
		bytes = make([]byte, rz.Seeded().Intn(maxsize-minsize)+minsize)
		for i := range bytes {
			bytes[i] = letterBytes[rz.Seeded().Intn(l)]
		}
	} else {
		bytes = make([]byte, rz.Unique().Intn(maxsize-minsize)+minsize)
		for i := range bytes {
			bytes[i] = letterBytes[rz.Unique().Intn(l)]
		}
	}

	return string(bytes)
}

/*
 * Randomizer
 */

// Fixed returns fixed randomizer (always returns the same values)
func (rz *Randomizer) Fixed() *rand.Rand {
	return rz.fixed
}

// Seeded returns seeded randomizer (seed'able)
func (rz *Randomizer) Seeded() *rand.Rand {
	return rz.seeded
}

// Unique returns unique randomizer (always unique)
func (rz *Randomizer) Unique() *rand.Rand {
	return rz.unique
}

// Intn returns random int value within the 0...max range
func (rz *Randomizer) Intn(max int) int {
	if max == 0 {
		return 0
	}

	return rz.Seeded().Intn(max)
}

// Uintn64 returns random uint64 value within the 0...max range
func (rz *Randomizer) Uintn64(max uint64) uint64 {
	if max == 0 {
		return 0
	}

	return rz.Seeded().Uint64() % max //nolint:gosec
}

// UUID returns random UUID	v4 value (RFC 4122)
func (rz *Randomizer) UUID() string {
	r := rz.Unique()

	return fmt.Sprintf("%04x%04x-%04x-%04x-%04x-%04x%04x%04x",
		r.Int31n(0xffff), r.Int31n(0xffff),
		r.Int31n(0xffff),
		r.Int31n(0xffff)&0x0fff|0x4000,
		r.Int31n(0xffff)&0x3fff|0x8000,
		r.Int31n(0xffff), r.Int31n(0xffff), r.Int31n(0xffff),
	)
}

// UUIDn returns random UUID v4 value (RFC 4122) with given limit
func (rz *Randomizer) UUIDn(limit int) string {
	r := rz.Unique()

	return fmt.Sprintf("01234567-89ab-cdef-0123-0000%08x", r.Intn(limit))
}

// RandTime returns random time within the given limit
func (rz *Randomizer) RandTime(daysAgoLimit int) time.Time {
	now := time.Now()

	days := time.Duration(daysAgoLimit) * 24 * time.Hour
	from := now.Add(-days)

	randomDays := time.Duration(rz.Intn(90)) * 24 * time.Hour
	randomHours := time.Duration(rz.Intn(24)) * time.Hour
	randomMinutes := time.Duration(rz.Intn(60)) * time.Minute
	randomDuration := randomDays + randomHours + randomMinutes

	return from.Add(randomDuration)
}

// Read fills the blob with random data
func (rz *Randomizer) Read(blob []byte) error {
	_, err := rz.Seeded().Read(blob)
	if err != nil {
		err = fmt.Errorf("error reading random data: %s", err)
	}

	return err
}

// IntnExp returns random int value within the 0...max range with exponential probability
/*
 * Return a value within the 0...max range with exponential probability
 * For example, for 10K range and 1M calls the fucntion would return:
 * - 0 value is returned ~1K times
 * - max returned value is ~10K
 */
func (rz *Randomizer) IntnExp(max int) int {
	return rz.Intn(rz.Intn(max) + 1)
}

func NewRandomizer(seed int64, workerID int) *Randomizer {
	rz := Randomizer{}

	if seed == 0 {
		seed = time.Now().UnixNano()
	} else {
		seed += 1 + int64(workerID)
	}

	rz.fixed = rand.New(rand.NewSource(0))
	rz.seeded = rand.New(rand.NewSource(seed))
	rz.unique = rand.New(rand.NewSource(time.Now().UnixNano()))

	return &rz
}

// RandomizerPlugin is a
type RandomizerPlugin interface {
	GenCommonFakeValue(columnType string, rz *Randomizer, cardinality int) (bool, interface{})
	GenFakeValue(columnType string, rz *Randomizer, cardinality int, preGenerated map[string]interface{}) (bool, interface{})
}

// Randomizer is a struct for storing randomizer data
type Randomizer struct {
	plugins map[string]RandomizerPlugin

	fixed  *rand.Rand // fixed randomizer
	seeded *rand.Rand // seeded seed'able randomizer
	unique *rand.Rand // unique always unique randomizer
}

func (rz *Randomizer) RegisterPlugin(name string, plugin RandomizerPlugin) { //nolint:revive
	if rz.plugins == nil {
		rz.plugins = make(map[string]RandomizerPlugin)
	}

	rz.plugins[name] = plugin
}

/*
 * Database fake value generators
 */

// DBFakeColumnConf is a struct for storing DB fake column configuration
type DBFakeColumnConf struct {
	ColumnName  string
	ColumnType  string
	Cardinality int
	MaxSize     int
	MinSize     int
}

// GenFakeValue generates fake value for given column type
func (rz *Randomizer) GenFakeValue(columnType string, columnName string, cardinality int, maxsize int, minsize int, preGenerated map[string]interface{}) (interface{}, error) {

	switch columnType {
	case "autoinc":
		// the best motonic autoincrement simulation
		return time.Now().UnixNano(), nil
	case "now_sec":
		return time.Now().Unix(), nil
	case "now_ms":
		return time.Now().UnixMilli(), nil
	case "now_mcs":
		return time.Now().UnixMicro(), nil
	case "now_ns":
		return time.Now().UnixNano(), nil
	case "now":
		return time.Now(), nil
	case "int":
		return rz.Intn(cardinality), nil
	case "bigint":
		return rand.Int63(), nil
	case "string":
		return rz.RandStringBytes(columnName+"_", cardinality, maxsize, minsize, true), nil
	case "rstring":
		return rz.RandStringBytes(columnName+"_", cardinality, maxsize, minsize, false), nil
	case "uuid":
		if cardinality == 0 {
			return rz.UUID(), nil
		} else {
			return rz.UUIDn(cardinality), nil
		}
	case "time":
		if cardinality == 0 {
			return time.Now(), nil
		} else {
			return rz.RandTime(cardinality), nil
		}
	case "time_string":
		if cardinality == 0 {
			return time.Now().String(), nil
		} else {
			return rz.RandTime(cardinality).String(), nil
		}
	case "time_ns":
		// fmt.Printf("dt: %s\n", rw.RandTime(cardinality).UTC().Format("2006-01-02 15:04:05.000000"))
		if cardinality == 0 {
			return time.Now().Unix(), nil
		} else {
			return rz.RandTime(cardinality).Unix(), nil
		}
	case "timestamp":
		if cardinality == 0 {
			return time.Now().UTC().Format("2006-01-02 15:04:05.000000"), nil
		} else {
			return rz.RandTime(cardinality).UTC().Format("2006-01-02 15:04:05.000000"), nil
		}
	case "byte":
		return []byte(rz.RandStringBytes("", cardinality, maxsize, minsize, true)), nil
	case "rbyte":
		return []byte(rz.RandStringBytes("", cardinality, maxsize, minsize, false)), nil
	case "json":
		return rz.GenRandomJson(1024), nil
	case "bool":
		return rz.Intn(2) == 1, nil
	case "blob":
		size := rz.Intn(maxsize-minsize) + minsize
		blob := make([]byte, size)
		err := rz.Read(blob)
		if err != nil {
			return nil, err
		}

		return blob, nil
	default:
		for _, plugin := range rz.plugins {
			if ok, value := plugin.GenFakeValue(columnType, rz, cardinality, preGenerated); ok {
				return value, nil
			}
		}

		return "", fmt.Errorf("generateParameter: unsupported parameter '%s'", columnType)
	}
}

// columnRequired returns true if given column is required
func columnRequired(column string, columns []string) bool { //nolint:unused
	if len(columns) == 0 {
		// empty list means any column is required
		return true
	}
	for _, v := range columns {
		if v == column {
			return true
		}
	}

	return false
}

// GenFakeData generates fake data for given column configuration
func (rz *Randomizer) GenFakeData(colConfs *[]DBFakeColumnConf, WithAutoInc bool) ([]string, []interface{}, error) {
	columns := make([]string, 0, len(*colConfs))
	values := make([]interface{}, 0, len(*colConfs))

	var preGenerated map[string]interface{}
	for _, plugin := range rz.plugins {
		for _, c := range *colConfs {
			if exists, value := plugin.GenCommonFakeValue(c.ColumnType, rz, c.Cardinality); exists {
				if preGenerated == nil {
					preGenerated = make(map[string]interface{})
				}
				preGenerated[c.ColumnType] = value
			}
		}
	}

	for _, c := range *colConfs {
		if c.ColumnType == "autoinc" && !WithAutoInc {
			continue
		}
		columns = append(columns, c.ColumnName)

		value, err := rz.GenFakeValue(c.ColumnType, c.ColumnName, c.Cardinality, c.MaxSize, c.MinSize, preGenerated)
		if err != nil {
			return nil, nil, err
		}

		values = append(values, value)
	}

	return columns, values, nil
}

// GenFakeDataAsMap generates fake data for given column configuration as map
func (rz *Randomizer) GenFakeDataAsMap(colConfs *[]DBFakeColumnConf, WithAutoInc bool) (*map[string]interface{}, error) {
	ret := make(map[string]interface{}, len(*colConfs))

	var preGenerated map[string]interface{}
	for _, plugin := range rz.plugins {
		for _, c := range *colConfs {
			if exists, value := plugin.GenCommonFakeValue(c.ColumnType, rz, c.Cardinality); exists {
				if preGenerated == nil {
					preGenerated = make(map[string]interface{})
				}
				preGenerated[c.ColumnType] = value
			}
		}
	}

	for _, c := range *colConfs {
		if c.ColumnType == "autoinc" && !WithAutoInc {
			continue
		}

		value, err := rz.GenFakeValue(c.ColumnType, c.ColumnName, c.Cardinality, c.MaxSize, c.MinSize, preGenerated)
		if err != nil {
			return nil, fmt.Errorf("genFakeDataAsMap: %s", err)
		}

		ret[c.ColumnName] = value
	}

	return &ret, nil
}
