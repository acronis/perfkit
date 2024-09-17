package search

import (
	"encoding/base64"
	"encoding/json"

	"github.com/acronis/perfkit/db"
)

type PageToken struct {
	Fields  []string            `json:"fields"`
	Filter  map[string][]string `json:"filter"`
	Order   []string            `json:"order"`
	Offsets map[string]int64    `json:"offsets"`
	Cursor  map[string]string   `json:"cursor"`
}

func (pt *PageToken) HasFilter(key string) bool {
	if pt.Filter == nil {
		return false
	}
	f, ok := pt.Filter[key]
	return ok && len(f) > 0
}

func (pt *PageToken) HasOrder(key string) bool {
	if pt.Order == nil {
		return false
	}
	for _, o := range pt.Order {
		_, field, err := db.ParseFunc(o)
		if err != nil {
			return false
		}
		if key == field {
			return true
		}
	}
	return false
}

func (pt *PageToken) Pack() (string, error) {
	var b, err = json.Marshal(&pt)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

func (pt *PageToken) Unpack(s string) error {
	var b, err = base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, pt)
}

func newSelectCtrl(pt PageToken, limit int64, offset int64) *db.SelectCtrl {
	return &db.SelectCtrl{
		Fields: pt.Fields,
		Page:   db.Page{Limit: limit, Offset: offset},
		Where:  pt.Filter,
		Order:  pt.Order,
	}
}
