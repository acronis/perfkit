package ms

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/meilisearch/meilisearch-go"

	"github.com/acronis/perfkit/db"
)

// @cpt-perfkit-db-algo-ms-adapter-build-filter
func buildMeilisearchFilter(where map[string][]string) (string, error) {
	if len(where) == 0 {
		return "", nil
	}

	var parts []string

	for _, c := range db.SortFields(where) {
		if c.Col == "" {
			return "", fmt.Errorf("empty condition field")
		}

		for _, v := range c.Vals {
			if v == db.SpecialConditionIsNull {
				parts = append(parts, fmt.Sprintf("%s IS NULL", c.Col))
				continue
			}
			if v == db.SpecialConditionIsNotNull {
				parts = append(parts, fmt.Sprintf("%s IS NOT NULL", c.Col))
				continue
			}

			fnc, val, err := db.ParseFunc(v)
			if err != nil {
				return "", fmt.Errorf("%v on field '%v'", err, c.Col)
			}

			switch fnc {
			case "":
				parts = append(parts, fmt.Sprintf("%s = %s", c.Col, quoteFilterValue(val)))
			case "lt":
				parts = append(parts, fmt.Sprintf("%s < %s", c.Col, quoteFilterValue(val)))
			case "le":
				parts = append(parts, fmt.Sprintf("%s <= %s", c.Col, quoteFilterValue(val)))
			case "gt":
				parts = append(parts, fmt.Sprintf("%s > %s", c.Col, quoteFilterValue(val)))
			case "ge":
				parts = append(parts, fmt.Sprintf("%s >= %s", c.Col, quoteFilterValue(val)))
			case "ne":
				parts = append(parts, fmt.Sprintf("%s != %s", c.Col, quoteFilterValue(val)))
			case "like", "hlike", "tlike":
				return "", fmt.Errorf("like functions are not supported by Meilisearch on field '%v'", c.Col)
			default:
				return "", fmt.Errorf("unsupported function '%v' on field '%v'", fnc, c.Col)
			}
		}
	}

	return strings.Join(parts, " AND "), nil
}

// quoteFilterValue returns the value quoted for Meilisearch filter expressions.
// Numeric values are left unquoted; string values are single-quoted.
func quoteFilterValue(val string) string {
	// Try to detect numeric values — if it parses as a number, leave unquoted
	dotCount := 0
	for i, ch := range val {
		if ch == '-' || ch == '+' {
			if i != 0 {
				return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "\\'"))
			}
			continue
		}
		if ch == '.' {
			dotCount++
			if dotCount > 1 {
				return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "\\'"))
			}
			continue
		}
		if ch < '0' || ch > '9' {
			return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "\\'"))
		}
	}
	return val
}

type vectorSearch struct {
	field  string
	vector []float32
}

func buildSortParams(order []string) ([]string, *vectorSearch, error) {
	var sortParams []string
	var vs *vectorSearch

	for _, value := range order {
		fnc, args, err := db.ParseFuncMultipleArgs(value, ";")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse order function: %v", err)
		}

		if len(args) == 0 {
			return nil, nil, fmt.Errorf("empty order field")
		}

		switch fnc {
		case "asc":
			if len(args) != 1 {
				return nil, nil, fmt.Errorf("asc expects 1 argument, got %d", len(args))
			}
			sortParams = append(sortParams, fmt.Sprintf("%s:asc", args[0]))
		case "desc":
			if len(args) != 1 {
				return nil, nil, fmt.Errorf("desc expects 1 argument, got %d", len(args))
			}
			sortParams = append(sortParams, fmt.Sprintf("%s:desc", args[0]))
		case "nearest":
			if len(args) != 3 {
				return nil, nil, fmt.Errorf("nearest expects 3 arguments (field;metric;vector), got %d", len(args))
			}
			if vs != nil {
				return nil, nil, fmt.Errorf("only one nearest() function is allowed")
			}

			rawVector, err := db.ParseVector(args[2], ",")
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse vector: %v", err)
			}

			vector := make([]float32, 0, len(rawVector))
			for _, v := range rawVector {
				f, err := strconv.ParseFloat(strings.TrimSpace(v), 32)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to parse vector value '%s': %v", v, err)
				}
				vector = append(vector, float32(f))
			}

			vs = &vectorSearch{field: args[0], vector: vector}
		default:
			return nil, nil, fmt.Errorf("unsupported order function '%v'", fnc)
		}
	}

	if len(sortParams) != 0 && vs != nil {
		return nil, nil, fmt.Errorf("sort and nearest() are mutually exclusive")
	}

	return sortParams, vs, nil
}

// @cpt-perfkit-db-algo-ms-adapter-select
func (g *msGateway) Select(tableName string, sc *db.SelectCtrl) (db.Rows, error) {
	if sc == nil {
		return &db.EmptyRows{}, nil
	}

	// Handle COUNT query
	if len(sc.Fields) == 1 && sc.Fields[0] == "COUNT(0)" {
		return g.selectCount(tableName, sc)
	}

	filter, err := buildMeilisearchFilter(sc.Where)
	if err != nil {
		return nil, fmt.Errorf("failed to build filter: %v", err)
	}

	sortParams, vs, err := buildSortParams(sc.Order)
	if err != nil {
		return nil, fmt.Errorf("failed to build sort: %v", err)
	}

	var limit int64 = 20
	if sc.Page.Limit > 0 {
		limit = sc.Page.Limit
	}

	searchReq := &meilisearch.SearchRequest{
		Filter:               filter,
		Sort:                 sortParams,
		Limit:                limit,
		Offset:               sc.Page.Offset,
		AttributesToRetrieve: sc.Fields,
	}

	if vs != nil {
		searchReq.Vector = vs.vector
		searchReq.Hybrid = &meilisearch.SearchRequestHybrid{
			Embedder: vs.field,
		}
		searchReq.RetrieveVectors = true
	}

	index := g.client.Index(tableName)

	resp, err := index.Search("", searchReq)
	if err != nil {
		return nil, fmt.Errorf("failed to search meilisearch index %s: %v", tableName, err)
	}

	if resp.Hits.Len() == 0 {
		return &db.EmptyRows{}, nil
	}

	var data []map[string]interface{}
	for _, hit := range resp.Hits {
		m := make(map[string]interface{})
		for k, raw := range hit {
			var v interface{}
			if err := json.Unmarshal(raw, &v); err != nil {
				m[k] = string(raw)
			} else {
				m[k] = v
			}
		}

		// Extract vectors from _vectors field if present
		if vectorsRaw, ok := m["_vectors"]; ok {
			if vectorsMap, ok := vectorsRaw.(map[string]interface{}); ok {
				for embedderName, embedderData := range vectorsMap {
					if embedderObj, ok := embedderData.(map[string]interface{}); ok {
						if embeddings, ok := embedderObj["embeddings"]; ok {
							if embList, ok := embeddings.([]interface{}); ok && len(embList) > 0 {
								m[embedderName] = embList[0]
							}
						}
					}
				}
			}
			delete(m, "_vectors")
		}

		data = append(data, m)
	}

	rows := &msRows{data: data, requestedColumns: sc.Fields}

	if g.readRowsLogger != nil {
		return &wrappedRows{
			rows:           rows,
			logTime:        g.logTime,
			readRowsLogger: g.readRowsLogger,
		}, nil
	}

	return rows, nil
}

func (g *msGateway) selectCount(tableName string, sc *db.SelectCtrl) (db.Rows, error) {
	filter, err := buildMeilisearchFilter(sc.Where)
	if err != nil {
		return nil, fmt.Errorf("failed to build filter for count: %v", err)
	}

	searchReq := &meilisearch.SearchRequest{
		Filter: filter,
		Limit:  0,
	}

	index := g.client.Index(tableName)

	resp, err := index.Search("", searchReq)
	if err != nil {
		return nil, fmt.Errorf("failed to count meilisearch index %s: %v", tableName, err)
	}

	return &db.CountRows{Count: resp.EstimatedTotalHits}, nil
}
