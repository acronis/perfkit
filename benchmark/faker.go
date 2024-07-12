package benchmark

import (
	"fmt"
	"math/rand"
	"os"
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
func (b *Benchmark) RandStringBytes(workerID int, pfx string, cardinality int, maxsize int, minsize int, seeded bool) string {
	if maxsize == minsize {
		return ""
	}

	rw := b.Randomizer.GetWorker(workerID)

	if cardinality != 0 {
		return cardinalityCache.randStringWithCardinality(rw.Intn(cardinality), pfx, cardinality, maxsize, minsize)
	}

	var bytes []byte
	l := len(letterBytes)

	if seeded {
		bytes = make([]byte, rw.Seeded().Intn(maxsize-minsize)+minsize)
		for i := range bytes {
			bytes[i] = letterBytes[rw.Seeded().Intn(l)]
		}
	} else {
		bytes = make([]byte, rw.Unique().Intn(maxsize-minsize)+minsize)
		for i := range bytes {
			bytes[i] = letterBytes[rw.Unique().Intn(l)]
		}
	}

	return string(bytes)
}

/*
 * Randomizer
 */

// RandomizerWorker is a struct for storing randomizer data
type RandomizerWorker struct {
	fixed  *rand.Rand // fixed randomizer
	seeded *rand.Rand // seeded seed'able randomizer
	unique *rand.Rand // unique always unique randomizer
}

// Fixed returns fixed randomizer (always returns the same values)
func (rw *RandomizerWorker) Fixed() *rand.Rand {
	return rw.fixed
}

// Seeded returns seeded randomizer (seed'able)
func (rw *RandomizerWorker) Seeded() *rand.Rand {
	return rw.seeded
}

// Unique returns unique randomizer (always unique)
func (rw *RandomizerWorker) Unique() *rand.Rand {
	return rw.unique
}

// Intn returns random int value within the 0...max range
func (rw *RandomizerWorker) Intn(max int) int {
	if max == 0 {
		return 0
	}

	return rw.Seeded().Intn(max)
}

// Uintn64 returns random uint64 value within the 0...max range
func (rw *RandomizerWorker) Uintn64(max uint64) uint64 {
	if max == 0 {
		return 0
	}

	return rw.Seeded().Uint64() % max //nolint:gosec
}

// UUID returns random UUID	v4 value (RFC 4122)
func (rw *RandomizerWorker) UUID() string {
	r := rw.Unique()

	return fmt.Sprintf("%04x%04x-%04x-%04x-%04x-%04x%04x%04x",
		r.Int31n(0xffff), r.Int31n(0xffff),
		r.Int31n(0xffff),
		r.Int31n(0xffff)&0x0fff|0x4000,
		r.Int31n(0xffff)&0x3fff|0x8000,
		r.Int31n(0xffff), r.Int31n(0xffff), r.Int31n(0xffff),
	)
}

// UUIDn returns random UUID v4 value (RFC 4122) with given limit
func (rw *RandomizerWorker) UUIDn(limit int) string {
	r := rw.Unique()

	return fmt.Sprintf("01234567-89ab-cdef-0123-0000%08x", r.Intn(limit))
}

// RandTime returns random time within the given limit
func (rw *RandomizerWorker) RandTime(daysAgoLimit int) time.Time {
	now := time.Now()

	days := time.Duration(daysAgoLimit) * 24 * time.Hour
	from := now.Add(-days)

	randomDays := time.Duration(rw.Intn(90)) * 24 * time.Hour
	randomHours := time.Duration(rw.Intn(24)) * time.Hour
	randomMinutes := time.Duration(rw.Intn(60)) * time.Minute
	randomDuration := randomDays + randomHours + randomMinutes

	return from.Add(randomDuration)
}

// Read fills the blob with random data
func (rw *RandomizerWorker) Read(blob []byte) error {
	_, err := rw.Seeded().Read(blob)
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
func (rw *RandomizerWorker) IntnExp(max int) int {
	return rw.Intn(rw.Intn(max) + 1)
}

// NewRandomizerWorker returns new RandomizerWorker object with given seed and workerID
func NewRandomizerWorker(seed int64, workerID int) *RandomizerWorker {
	rw := RandomizerWorker{}
	if seed == 0 {
		seed = time.Now().UnixNano()
	} else {
		seed += 1 + int64(workerID)
	}

	rw.fixed = rand.New(rand.NewSource(0))
	rw.seeded = rand.New(rand.NewSource(seed))
	rw.unique = rand.New(rand.NewSource(time.Now().UnixNano()))

	return &rw
}

// RandomizerPlugin is a
type RandomizerPlugin interface {
	GenCommonFakeValue(columnType string, rw *RandomizerWorker, cardinality int) (bool, interface{})
	GenFakeValue(columnType string, rw *RandomizerWorker, cardinality int, preGenerated map[string]interface{}) (bool, interface{})
}

// Randomizer is a struct for storing randomizer data
type Randomizer struct {
	worker  map[int]*RandomizerWorker // worker is a map, id -> RandomizerWorker
	plugins map[string]RandomizerPlugin
}

// NewRandomizer returns new Randomizer object with given seed and workers count
func NewRandomizer(seed int64, workers int) *Randomizer {
	rz := Randomizer{}
	rz.worker = make(map[int]*RandomizerWorker)

	for w := 0; w <= workers; w++ {
		rz.worker[w] = NewRandomizerWorker(seed, w)
	}
	rz.worker[-1] = NewRandomizerWorker(seed, -1)

	return &rz
}

// GetWorker returns RandomizerWorker object for given workerID
func (rz *Randomizer) GetWorker(workerID int) *RandomizerWorker {
	rw, exists := rz.worker[workerID]
	if !exists {
		fmt.Printf("fatal error: %v", fmt.Sprintf("random generator for worker %d has not been initialized, probably NewRandomizer() was not initilized properly", workerID))
		os.Exit(127)
	}

	return rw
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
func (b *Benchmark) GenFakeValue(workerID int, columnType string, columnName string, cardinality int, maxsize int, minsize int, preGenerated map[string]interface{}) interface{} {
	rw := b.Randomizer.GetWorker(workerID)

	switch columnType {
	case "autoinc":
		// the best motonic autoincrement simulation
		return time.Now().UnixNano()
	case "now_sec":
		return time.Now().Unix()
	case "now_ms":
		return time.Now().UnixMilli()
	case "now_mcs":
		return time.Now().UnixMicro()
	case "now_ns":
		return time.Now().UnixNano()
	case "now":
		return time.Now()
	case "int":
		return rw.Intn(cardinality)
	case "bigint":
		return rand.Int63()
	case "string":
		return b.RandStringBytes(workerID, columnName+"_", cardinality, maxsize, minsize, true)
	case "rstring":
		return b.RandStringBytes(workerID, columnName+"_", cardinality, maxsize, minsize, false)
	case "uuid":
		if cardinality == 0 {
			return rw.UUID()
		} else {
			return rw.UUIDn(cardinality)
		}
	case "time":
		if cardinality == 0 {
			return time.Now().String()
		} else {
			return rw.RandTime(cardinality).String()
		}
	case "time_ns":
		// fmt.Printf("dt: %s\n", rw.RandTime(cardinality).UTC().Format("2006-01-02 15:04:05.000000"))
		if cardinality == 0 {
			return time.Now().Unix()
		} else {
			return rw.RandTime(cardinality).Unix()
		}
	case "timestamp":
		if cardinality == 0 {
			return time.Now().UTC().Format("2006-01-02 15:04:05.000000")
		} else {
			return rw.RandTime(cardinality).UTC().Format("2006-01-02 15:04:05.000000")
		}
	case "byte":
		return []byte(b.RandStringBytes(workerID, "", cardinality, maxsize, minsize, true))
	case "rbyte":
		return []byte(b.RandStringBytes(workerID, "", cardinality, maxsize, minsize, false))
	case "json":
		return b.GenRandomJson(rw, 1024)
	case "bool":
		return rw.Intn(2) == 1
	case "blob":
		size := rw.Intn(maxsize-minsize) + minsize
		blob := make([]byte, size)
		err := rw.Read(blob)
		if err != nil {
			b.Exit(err.Error())
		}

		return blob
	default:
		for _, plugin := range b.Randomizer.plugins {
			if ok, value := plugin.GenFakeValue(columnType, rw, cardinality, preGenerated); ok {
				return value
			}
		}

		b.Exit("generateParameter: unsupported parameter '%s'", columnType)

		return ""
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
func (b *Benchmark) GenFakeData(workerID int, colConfs *[]DBFakeColumnConf, WithAutoInc bool) ([]string, []interface{}) {
	columns := make([]string, 0, len(*colConfs))
	values := make([]interface{}, 0, len(*colConfs))
	rw := b.Randomizer.GetWorker(workerID)

	var preGenerated map[string]interface{}
	for _, plugin := range b.Randomizer.plugins {
		for _, c := range *colConfs {
			if exists, value := plugin.GenCommonFakeValue(c.ColumnType, rw, c.Cardinality); exists {
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
		values = append(values, b.GenFakeValue(workerID, c.ColumnType, c.ColumnName, c.Cardinality, c.MaxSize, c.MinSize, preGenerated))
	}

	return columns, values
}

// GenFakeDataAsMap generates fake data for given column configuration as map
func (b *Benchmark) GenFakeDataAsMap(workerID int, colConfs *[]DBFakeColumnConf, WithAutoInc bool) *map[string]interface{} {
	ret := make(map[string]interface{}, len(*colConfs))
	rw := b.Randomizer.GetWorker(workerID)

	var preGenerated map[string]interface{}
	for _, plugin := range b.Randomizer.plugins {
		for _, c := range *colConfs {
			if exists, value := plugin.GenCommonFakeValue(c.ColumnType, rw, c.Cardinality); exists {
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
		ret[c.ColumnName] = b.GenFakeValue(workerID, c.ColumnType, c.ColumnName, c.Cardinality, c.MaxSize, c.MinSize, preGenerated)
	}

	return &ret
}
