package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/acronis/perfkit/db"
)

type BulkIndexRequest struct {
	data []json.RawMessage
}

type bulkIndexHeaderMetadata struct {
	ID    int64  `json:"_id,omitempty"`
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

func (g *esGateway) BulkInsert(tableName string, rows [][]interface{}, columnNames []string) error {
	if len(rows) == 0 {
		return nil
	}

	var idxName = indexName(tableName)
	var req = &BulkIndexRequest{
		data: []json.RawMessage{},
	}

	var timeStampFieldPresented bool
	for _, columnName := range columnNames {
		if columnName == "@timestamp" {
			timeStampFieldPresented = true
			break
		}
	}

	for _, row := range rows {
		if len(row) != len(columnNames) {
			return fmt.Errorf("row length doesn't match column names length")
		}

		var column2val = make(map[string]interface{})
		if !timeStampFieldPresented {
			column2val["@timestamp"] = time.Now().UTC().Format(timeStoreFormatPrecise)
		}

		for i, col := range row {
			column2val[columnNames[i]] = col
		}

		var serialized, err = json.Marshal(column2val)
		if err != nil {
			return fmt.Errorf("failed to marshal elem: %v", err)
		}

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
