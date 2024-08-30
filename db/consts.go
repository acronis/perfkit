package db

import "strings"

type DialectName string

const ()

var (
	// SupportedDrivers is a string containing all supported drivers
	SupportedDrivers = strings.Join([]string{string(SQLITE), string(POSTGRES), string(MYSQL), string(MSSQL)}, "|")
)
