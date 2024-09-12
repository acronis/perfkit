package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	es8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/acronis/perfkit/db"
)

const (
	timeStoreFormatPrecise = time.RFC3339Nano
)

type esQuerier struct {
	es *es8.Client
}

func (q *esQuerier) ping(context.Context) error {
	if ping, err := q.es.Ping(); err != nil {
		return fmt.Errorf("db: failed ping es db, err: %v", err)
	} else if ping.IsError() {
		return fmt.Errorf("error %s res=%+v", ping.Status(), ping)
	}
	return nil
}

func (q *esQuerier) stats() db.Stats {
	return db.Stats{}
}

func (q *esQuerier) rawSession() interface{} {
	return q.es
}

func (q *esQuerier) close() error {
	return nil
}

func (q *esQuerier) getShardAndNodeInfo(idx indexName) (numNodes int, shards []shardInfo, err error) {
	shards, err = q.shardInfo(idx)
	if err != nil {
		return
	}
	numNodes, err = q.nodeInfo()
	return
}

func (q *esQuerier) expectedSuccesses(idx indexName) int {
	numNodes, shards, err := q.getShardAndNodeInfo(idx)
	if err != nil {
		return 0
	}
	nShards := 0
	for _, sh := range shards {
		if sh.State == "STARTED" {
			nShards += 1
		}
	}
	return nShards / numNodes
}

type shardInfo struct {
	Index string
	Shard int
	State string
}

func parseShardData(s string) ([]shardInfo, error) {
	s = strings.TrimPrefix(s, "[200 OK] ")
	si := make([]shardInfo, 0)
	for _, line := range strings.Split(s, "\n") {
		if line == "" {
			break
		}
		ls := strings.Split(line, " ")
		shard, err := strconv.Atoi(ls[1])
		if err != nil {
			return nil, fmt.Errorf("failed to parse shard number %s: %v", ls[1], err)
		}
		si = append(si, shardInfo{
			Index: ls[0],
			Shard: shard,
			State: ls[3],
		})
	}
	return si, nil
}

func (q *esQuerier) shardInfo(idx indexName) ([]shardInfo, error) {
	resp, err := esapi.CatShardsRequest{
		Index:  []string{string(idx)},
		Pretty: true,
		Human:  true,
	}.Do(context.Background(), q.es)

	if resp.IsError() {
		return nil, fmt.Errorf("error %s", resp.Status())
	}
	if err != nil {
		return nil, fmt.Errorf("error while getting shard info: %v", err)
	}
	s := resp.String()
	shards, err := parseShardData(s)
	if err != nil {
		return nil, fmt.Errorf("shard info parsing error: %v", err)
	}
	return shards, nil
}

func parseNodeData(s string) int {
	return len(strings.Split(s, "\n")) - 1 // The -1 accounts for the \n last line of the response
}

func (q *esQuerier) nodeInfo() (int, error) {
	resp, err := esapi.CatNodesRequest{
		Pretty: true,
		Human:  true,
	}.Do(context.Background(), q.es)

	if resp.IsError() {
		return 0, fmt.Errorf("error %s", resp.Status())
	}
	if err != nil {
		return 0, fmt.Errorf("error while getting node info: %v", err)
	}
	s := resp.String()
	return parseNodeData(s), nil
}

func (q *esQuerier) insert(ctx context.Context, idxName indexName, query *BulkIndexRequest) (*BulkIndexResult, int, error) {
	var res, err = q.es.Bulk(query.Reader(),
		q.es.Bulk.WithContext(ctx),
		q.es.Bulk.WithIndex(string(idxName)),
		q.es.Bulk.WithRefresh("wait_for"))
	if err != nil {
		return nil, 0, fmt.Errorf("error from elasticsearch while performing bulk insert: %v", err)
	} else if res.IsError() {
		return nil, 0, fmt.Errorf("bulk insert error %s", res.String())
	}

	var rv = new(BulkIndexResult)
	if err = json.NewDecoder(res.Body).Decode(rv); err != nil {
		return nil, 0, fmt.Errorf("error decoding request: %v", err)
	}
	if rv.Errors {
		var errList []string
		for _, item := range rv.IndexedItems {
			if item.IndexResult.Status != 201 && item.IndexResult.Status != 409 {
				errList = append(errList, fmt.Sprintf("id: %s, status: %d", item.IndexResult.ID, item.IndexResult.Status))
			}
		}

		if len(errList) != 0 {
			return rv, q.expectedSuccesses(idxName), fmt.Errorf("bulk insert error: %v, query: %s", errList, query)
		}
	}

	return rv, q.expectedSuccesses(idxName), nil
}

func (q *esQuerier) search(ctx context.Context, idxName indexName, request *SearchRequest) (*SearchResponse, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return nil, fmt.Errorf("request encode error: %v", err)
	}

	var res, err = q.es.Search(
		q.es.Search.WithContext(ctx),
		q.es.Search.WithIndex(string(idxName)),
		q.es.Search.WithBody(&buf))
	if err != nil {
		return nil, fmt.Errorf("failed to perform search: %v", err)
	}

	// nolint: errcheck // Need to have logger here for deferred errors
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode != 404 {
			return nil, fmt.Errorf("failed to perform search: %s", res.String())
		} else { // This case should be executed only when schema was created but data stream is empty yet
			return nil, nil
		}
	}

	var resp = SearchResponse{}
	var decoder = json.NewDecoder(res.Body)
	decoder.UseNumber()
	if err = decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("search response decode err: %v", err)
	}

	return &resp, nil
}

func (q *esQuerier) count(ctx context.Context, idxName indexName, request *CountRequest) (*SearchResponse, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return nil, fmt.Errorf("request encode error: %v", err)
	}

	var res, err = q.es.Count(
		q.es.Count.WithContext(ctx),
		q.es.Count.WithIndex(string(idxName)),
		q.es.Count.WithBody(&buf))
	if err != nil {
		return nil, fmt.Errorf("failed to perform count: %v", err)
	}

	// nolint: errcheck // Need to have logger here for deferred errors
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("failed to perform count: %s", res.String())
	}

	var resp = SearchResponse{}
	var decoder = json.NewDecoder(res.Body)
	decoder.UseNumber()
	if err = decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("count response decode err: %v", err)
	}

	return &resp, nil
}
