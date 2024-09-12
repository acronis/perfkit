package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/acronis/perfkit/db"
)

type BulkIndexRequest struct {
	data []json.RawMessage
}

type bulkIndexHeaderMetadata struct {
	ID    int64  `json:"_id"`
	Index string `json:"_index"`
}

type bulkIndexHeader struct {
	Index bulkIndexHeaderMetadata `json:"create"`
}

func (r *BulkIndexRequest) With(index indexName, id int64, entity json.RawMessage) *BulkIndexRequest {
	var serializedMetaData, _ = json.Marshal(bulkIndexHeader{
		Index: bulkIndexHeaderMetadata{
			ID:    id,
			Index: string(index),
		}})
	r.data = append(r.data, serializedMetaData, entity)

	return r
}

func (r *BulkIndexRequest) Reader() io.Reader {
	// Marshals to NDJson
	var buf bytes.Buffer
	for _, obj := range r.data {
		buf.Write(obj)
		buf.WriteByte('\n')
	}
	return &buf
}

type IndexResult struct {
	ID          string      `json:"_id"`
	Index       string      `json:"_index"`
	PrimaryTerm int64       `json:"_primary_term"`
	SeqNo       int64       `json:"_seq_no"`
	Shards      ShardResult `json:"_shards"`
	Type        string      `json:"_type"`
	Version     int64       `json:"_version"`
	Result      string      `json:"result"`
	Status      int64       `json:"status"`
}

type BulkIndexResult struct {
	Errors       bool  `json:"errors"`
	TimeTaken    int64 `json:"took"`
	IndexedItems []struct {
		IndexResult IndexResult `json:"create"`
	} `json:"items"`
}

func (bi *BulkIndexResult) GetInsertStats(expectedSuccesses int) db.InsertStats {
	var dbs = db.InsertStats{ExpectedSuccesses: int64(expectedSuccesses)}
	for _, ir := range bi.IndexedItems {
		dbs.Successful += ir.IndexResult.Shards.Successful
		dbs.Failed += ir.IndexResult.Shards.Failed
		dbs.Total += ir.IndexResult.Shards.Total
	}
	return dbs
}

func (g *esGateway) InsertInto(tableName string, data interface{}, columnNames []string) error {
	var valuesList []reflect.Value
	var v = reflect.ValueOf(data)
	var fields reflect.Type

	// var
	if v.Kind() == reflect.Slice {
		s := reflect.ValueOf(data)
		for i := 0; i < s.Len(); i++ {
			valuesList = append(valuesList, s.Index(i))
			if i == 0 {
				fields = s.Index(i).Type()
			}
		}
	} else {
		valuesList = append(valuesList, reflect.ValueOf(data))
		fields = reflect.TypeOf(data)
	}

	if len(valuesList) == 0 {
		return nil
	}

	var numFields = fields.NumField()
	var column2val = make(map[string]interface{})

	var idxName = indexName(tableName)
	var req = &BulkIndexRequest{
		data: []json.RawMessage{},
	}

	var columnValues []json.RawMessage
	for _, values := range valuesList {
		for i := 0; i < numFields; i++ {
			columnName := fields.Field(i).Tag.Get("db")
			if columnName == "" {
				continue
			}
			column2val[columnName] = values.Field(i).Interface()
		}

		var serialized, err = json.Marshal(column2val)
		if err != nil {
			return fmt.Errorf("failed to marshal elem: %v", err)
		}

		columnValues = append(columnValues, serialized)

		var id int64
		if idVal, ok := column2val["id"]; ok {
			id = idVal.(int64)
		}

		req.With(idxName, id, serialized)
	}

	res, expectedSuccesses, err := g.q.insert(g.ctx.Ctx, idxName, req)
	if err != nil {
		return fmt.Errorf("error in bulk insert: %v", err)
	}

	var _ = res.GetInsertStats(expectedSuccesses)

	return nil
}
