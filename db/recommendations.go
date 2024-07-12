package db

import (
	"fmt"
	"strings"
)

var MByte = 1024 * 1024
var GByte = 1024 * MByte
var prefix = " - "

type RecommendationsSource interface {
	Recommendations() ([]Recommendation, error)
}

// Info is a struct for storing DB info
type Info struct {
	source   RecommendationsSource
	version  string
	settings map[string]string
}

// NewDBInfo creates new DBInfo object
func NewDBInfo(s RecommendationsSource, version string) *Info {
	return &Info{source: s, version: version, settings: make(map[string]string)}
}

// AddSetting adds setting to DBInfo object
func (i *Info) AddSetting(setting string, value string) {
	i.settings[setting] = value
}

// CheckSetting checks if DB setting is correct
func (i *Info) CheckSetting(r *Recommendation) {
	val, exists := i.settings[r.Setting]
	if !exists {
		i.Printf(r.Setting, r.Meaning, "ERR: setting is not found!", "")

		return
	}

	var hint string

	if r.ExpectedValue != "" {
		if r.ExpectedValue != val {
			hint = fmt.Sprintf("WRN: expected to have '%s'", r.ExpectedValue)
		} else {
			hint = "OK"
		}
	} else {
		intVal, err := StringToBytes(val)
		if err != nil {
			hint = fmt.Sprintf("ERR: can't parse value: %v: error: %s", val, err)
		} else {
			if intVal >= r.RecommendedVal {
				hint = "OK"
			} else if intVal >= r.MinVal {
				hint = fmt.Sprintf("WRN: recommended value should be at least %d", r.RecommendedVal)
			} else {
				hint = fmt.Sprintf("ERR: min value should be at least %d", r.MinVal)
			}
		}
	}
	i.Printf(r.Setting, r.Meaning, val, hint)
}

// Printf prints DB setting info to stdout
func (i *Info) Printf(setting string, meaning string, value string, hint string) {
	maxlen := 70
	parameter := fmt.Sprintf("%s (aka %s)", setting, meaning)
	l := len(parameter)

	if l < maxlen {
		parameter += strings.Repeat(".", maxlen-l)
	}

	fmt.Printf("%s%s %-11s   %s\n", prefix, parameter, value, hint)
}

// ShowRecommendations prints DB recommendations to stdout
func (i *Info) ShowRecommendations() {
	if i.source == nil {
		return
	}

	var recommendations, err = i.source.Recommendations()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("\ndatabase settings checks:\n")
	for _, r := range recommendations {
		i.CheckSetting(&r)
	}

	fmt.Printf("\n")
}
