package benchmark

import (
	"testing"
)

// TestNewDBInfoCreation tests NewDBInfo() function
func TestNewDBInfoCreation(t *testing.T) {
	c := &DBConnector{DbOpts: &DatabaseOpts{Driver: MYSQL}}
	version := "5.6"
	dbInfo := NewDBInfo(c, version)
	if dbInfo == nil {
		t.Errorf("NewDBInfo() error, dbInfo is nil")
	}
}

// TestAddSettingToDBInfo tests AddSetting() function
func TestAddSettingToDBInfo(t *testing.T) {
	c := &DBConnector{DbOpts: &DatabaseOpts{Driver: MYSQL}}
	version := "5.6"
	dbInfo := NewDBInfo(c, version)
	dbInfo.AddSetting("test", "value")
	if dbInfo.settings["test"] != "value" {
		t.Errorf("AddSetting() error, setting value mismatch")
	}
}

// TestIsMySQL56True tests IsMySQL56() function
func TestIsMySQL56True(t *testing.T) {
	c := &DBConnector{DbOpts: &DatabaseOpts{Driver: MYSQL}}
	version := "5.6"
	dbInfo := NewDBInfo(c, version)
	if !dbInfo.IsMySQL56() {
		t.Errorf("IsMySQL56() error, expected true but got false")
	}
}

// TestIsMySQL56False tests IsMySQL56() function
func TestIsMySQL56False(t *testing.T) {
	c := &DBConnector{DbOpts: &DatabaseOpts{Driver: MYSQL}}
	version := "5.7"
	dbInfo := NewDBInfo(c, version)
	if dbInfo.IsMySQL56() {
		t.Errorf("IsMySQL56() error, expected false but got true")
	}
}

// TestIsGaleraClusterTrue tests IsGaleraCluster() function
func TestIsGaleraClusterTrue(t *testing.T) {
	c := &DBConnector{DbOpts: &DatabaseOpts{Driver: MYSQL}}
	version := "Percona Cluster"
	dbInfo := NewDBInfo(c, version)
	if !dbInfo.IsGaleraCluster() {
		t.Errorf("IsGaleraCluster() error, expected true but got false")
	}
}

// TestIsGaleraClusterFalse tests IsGaleraCluster() function
func TestIsGaleraClusterFalse(t *testing.T) {
	c := &DBConnector{DbOpts: &DatabaseOpts{Driver: MYSQL}}
	version := "5.6"
	dbInfo := NewDBInfo(c, version)
	if dbInfo.IsGaleraCluster() {
		t.Errorf("IsGaleraCluster() error, expected false but got true")
	}
}

// TestCheckSettingWithExpectedValue tests CheckSetting() function
func TestCheckSettingWithExpectedValue(t *testing.T) { //nolint:revive
	c := &DBConnector{DbOpts: &DatabaseOpts{Driver: MYSQL}}
	version := "5.6"
	dbInfo := NewDBInfo(c, version)
	dbInfo.AddSetting("test", "value")
	r := Recommendation{setting: "test", meaning: "test meaning", expectedValue: "value"}
	dbInfo.CheckSetting(&r)
}

// TestCheckSettingWithMinVal tests CheckSetting() function
func TestCheckSettingWithMinVal(t *testing.T) { //nolint:revive
	c := &DBConnector{DbOpts: &DatabaseOpts{Driver: MYSQL}}
	version := "5.6"
	dbInfo := NewDBInfo(c, version)
	dbInfo.AddSetting("test", "1")
	r := Recommendation{setting: "test", meaning: "test meaning", minVal: 1, recommendedVal: 2}
	dbInfo.CheckSetting(&r)
}
