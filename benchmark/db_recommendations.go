package benchmark

import (
	"fmt"
	"strings"
)

var mbyte = 1024 * 1024
var gbyte = 1024 * mbyte
var prefix = " - "

// DBInfo is a struct for storing DB info
type DBInfo struct {
	conn     *DBConnector
	driver   string
	version  string
	settings map[string]string
}

// NewDBInfo creates new DBInfo object
func NewDBInfo(c *DBConnector, version string) *DBInfo {
	return &DBInfo{conn: c, driver: c.DbOpts.Driver, version: version, settings: make(map[string]string)}
}

// AddSetting adds setting to DBInfo object
func (i *DBInfo) AddSetting(setting string, value string) {
	i.settings[setting] = value
}

// IsMySQL56 checks if DB is MySQL 5.6
func (i *DBInfo) IsMySQL56() bool {
	if i.driver == MYSQL && strings.HasPrefix(i.version, "5.6") {
		return true
	}

	return false
}

// IsGaleraCluster checks if DB is Galera Cluster
func (i *DBInfo) IsGaleraCluster() bool {
	if i.driver == MYSQL && strings.Contains(i.version, "Percona") && strings.Contains(i.version, "Cluster") {
		return true
	}

	return false
}

// Recommendation is a struct for storing DB recommendation
type Recommendation struct {
	setting string
	meaning string

	expectedValue  string
	minVal         int64
	recommendedVal int64
}

// CheckSetting checks if DB setting is correct
func (i *DBInfo) CheckSetting(r *Recommendation) {
	val, exists := i.settings[r.setting]
	if !exists {
		i.Printf(r.setting, r.meaning, "ERR: setting is not found!", "")

		return
	}

	var hint string

	if r.expectedValue != "" {
		if r.expectedValue != val {
			hint = fmt.Sprintf("WRN: expected to have '%s'", r.expectedValue)
		} else {
			hint = "OK"
		}
	} else {
		intVal, err := StringToBytes(val)
		if err != nil {
			hint = fmt.Sprintf("ERR: can't parse value: %v: error: %s", val, err)
		} else {
			if intVal >= r.recommendedVal {
				hint = "OK"
			} else if intVal >= r.minVal {
				hint = fmt.Sprintf("WRN: recommended value should be at least %d", r.recommendedVal)
			} else {
				hint = fmt.Sprintf("ERR: min value should be at least %d", r.minVal)
			}
		}
	}
	i.Printf(r.setting, r.meaning, val, hint)
}

// Printf prints DB setting info to stdout
func (i *DBInfo) Printf(setting string, meaning string, value string, hint string) {
	maxlen := 70
	parameter := fmt.Sprintf("%s (aka %s)", setting, meaning)
	l := len(parameter)

	if l < maxlen {
		parameter += strings.Repeat(".", maxlen-l)
	}

	fmt.Printf("%s%s %-11s   %s\n", prefix, parameter, value, hint)
}

// mysqlRecommendations returns MySQL recommendations for DB settings
func (i *DBInfo) mysqlRecommendations() *[]Recommendation {
	var r []Recommendation

	if i.IsMySQL56() {
		r = []Recommendation{
			{setting: "innodb_buffer_pool_size", meaning: "primary DB cache per pool", minVal: int64(2 * gbyte), recommendedVal: int64(3 * gbyte)},
			{setting: "innodb_buffer_pool_instances", meaning: "number of DB cache pools", minVal: int64(4), recommendedVal: int64(8)},
			{setting: "innodb_log_file_size", meaning: "InnoDB redo log size", minVal: int64(256 * mbyte), recommendedVal: int64(512 * mbyte)},
			{setting: "innodb_log_files_in_group", meaning: "number of redo log files per group", minVal: int64(2), recommendedVal: int64(2)},
		}

		if i.IsGaleraCluster() {
			r = append(r, Recommendation{setting: "wsrep_slave_threads", meaning: "number of threads replicating write sets", minVal: int64(4), recommendedVal: int64(8)})
		}
	} else {
		r = []Recommendation{
			{setting: "innodb_buffer_pool_size", meaning: "primary DB cache", minVal: int64(8 * gbyte), recommendedVal: int64(12 * gbyte)},
			{setting: "innodb_log_file_size", meaning: "InnoDB redo log size", minVal: int64(512 * mbyte), recommendedVal: int64(2 * gbyte)},
		}
	}

	r = append(r, Recommendation{setting: "max_connections", meaning: "max allowed number of DB connections", minVal: int64(512), recommendedVal: int64(2048)})
	r = append(r, Recommendation{setting: "query_cache_type", meaning: "query cache policy", expectedValue: "OFF"})
	r = append(r, Recommendation{setting: "performance_schema", meaning: "performance-related server metrics", expectedValue: "ON"})

	return &r
}

// postgresRecommendations returns PostgreSQL recommendations for DB settings
func (i *DBInfo) postgresRecommendations() *[]Recommendation {
	return &[]Recommendation{
		{setting: "shared_buffers", meaning: "primary DB cache", minVal: int64(1 * gbyte), recommendedVal: int64(4 * gbyte)},
		{setting: "effective_cache_size", meaning: "OS cache", minVal: int64(2 * gbyte), recommendedVal: int64(8 * gbyte)},
		{setting: "work_mem", meaning: "Mem for internal sorting & hash tables", minVal: int64(8 * mbyte), recommendedVal: int64(16 * mbyte)},
		{setting: "maintenance_work_mem", meaning: "Mem for VACUUM, CREATE INDEX, etc", minVal: int64(128 * mbyte), recommendedVal: int64(256 * mbyte)},
		{setting: "wal_buffers", meaning: "Mem used in shared memory for WAL data", minVal: int64(8 * mbyte), recommendedVal: int64(16 * mbyte)},
		{setting: "max_wal_size", meaning: "max WAL size", minVal: int64(512 * mbyte), recommendedVal: int64(1 * gbyte)},
		{setting: "min_wal_size", meaning: "min WAL size", minVal: int64(32 * mbyte), recommendedVal: int64(64 * mbyte)},
		{setting: "max_connections", meaning: "max allowed number of DB connections", minVal: int64(512), recommendedVal: int64(2048)},
		{setting: "random_page_cost", meaning: "it should be 1.1 as it is typical for SSD", expectedValue: "1.1"},
		{setting: "track_activities", meaning: "collects session activities info", expectedValue: "on"},
		{setting: "track_counts", meaning: "track tables/indexes access count", expectedValue: "on"},
		{setting: "log_checkpoints", meaning: "logs information about each checkpoint", expectedValue: "on"},
		{setting: "jit", meaning: "JIT compilation feature", expectedValue: "off"},
		// effective_io_concurrency = 2 # For SSDs, this might be set to the number of separate SSDs or channels.
	}
}

// ShowRecommendations prints DB recommendations to stdout
func (i *DBInfo) ShowRecommendations() {
	var recommendations *[]Recommendation

	switch i.driver {
	case POSTGRES:
		recommendations = i.postgresRecommendations()
	case MYSQL:
		recommendations = i.mysqlRecommendations()
	default:
		recommendations = &[]Recommendation{}
	}

	if len(*recommendations) == 0 {
		return
	}

	fmt.Printf("\n%s database settings checks:\n", i.driver)
	for _, r := range *recommendations {
		i.CheckSetting(&r)
	}

	switch i.driver {
	case POSTGRES:
		rows := i.conn.GetRowsCount("pg_stat_replication", "")
		r := Recommendation{setting: "wal_level", meaning: "amount of information written to WAL", expectedValue: "replica"}
		if rows == 0 {
			r.expectedValue = "minimal"
		}
		i.CheckSetting(&r)

		if rows == 0 {
			r = Recommendation{setting: "max_wal_senders", meaning: "max num of simultaneous replication connections", expectedValue: "0"}
			i.CheckSetting(&r)
		}
	default:
	}

	fmt.Printf("\n")
}
