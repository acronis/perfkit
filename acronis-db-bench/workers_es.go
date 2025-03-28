package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	es8 "github.com/elastic/go-elasticsearch/v8"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
)

var (
	esDataHistoryType = [...]string{"audit", "log", "error", "info", "warning", "debug", "trace", "security", "performance", "network"}
	esDataEventType   = [...]string{"modify", "delete", "create", "update", "read", "write"}
)

const (
	maxHistoryPerItem = 10
)

func createESNestedTable(c *DBConnector, b *benchmark.Benchmark, table *TestTable) {
	mapping := `{
  "mappings": {
    "properties": {
      "@timestamp": {
        "type": "date"
      },
      "euc_id": {
        "type": "long"
      },
      "history": {
        "type": "nested",
        "properties": {
          "event": {
            "type": "keyword"
          },
          "history_type": {
            "type": "keyword"
          },
          "timestamp": {
            "type": "date_nanos"
          }
        }
      },
      "sender": {
        "type": "keyword"
      },
      "recipient": {
        "type": "keyword"
      },
      "subject": {
        "type": "keyword"
      },
      "body": {
        "type": "text"
      },
      "progress": {
        "type": "long",
        "index": false
      },
      "uuid": {
        "type": "keyword"
      }
    }
  },
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "codec": "best_compression",
    "index": {
      "refresh_interval": "1m",
      "mapping": {
        "ignore_malformed": true
      }
    }
  }
}}`

	switch rawSession := c.database.RawSession().(type) {
	case *es8.Client:
		existRes, err := rawSession.Indices.Exists([]string{table.TableName})
		if err != nil {
			b.Exit(fmt.Sprintf("error checking if index exist: %v", err))
		}

		defer existRes.Body.Close()

		if existRes.IsError() {
			if existRes.StatusCode != http.StatusNotFound {
				b.Exit("failed to check if index exist: %s", existRes.String())
			}
		}

		if existRes.StatusCode == http.StatusOK {
			return
		}

		res, err := rawSession.Indices.Create(
			table.TableName,
			rawSession.Indices.Create.WithBody(strings.NewReader(mapping)),
		)
		if err != nil {
			b.Exit(fmt.Sprintf("error creating index: %v", err))
		}

		defer res.Body.Close()

		if res.IsError() {
			if res.StatusCode != http.StatusNotFound {
				b.Exit("failed to create index: %s", res.String())
			}
		}

		if res.StatusCode >= 300 {
			b.Exit("failed to create index: %s", res.String())
		}
	}
}

func testInsertElasticsearch(b *benchmark.Benchmark, testDesc *TestDesc) {
	colConfs := testDesc.table.GetColumnsForInsert(db.WithAutoInc(getDBDriver(b)))

	if len(*colConfs) == 0 {
		b.Exit(fmt.Sprintf("internal error: no columns eligible for INSERT found in '%s' configuration", testDesc.table.TableName))
	}

	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	b.Worker = func(workerId int) (loops int) {
		workerData := b.WorkerData[workerId].(*DBWorkerData)

		var c = workerData.workingConn

		createESNestedTable(c, b, table)

		// Prepare the buffer to store payload
		//
		var buf bytes.Buffer

		for i := range batch {
			columns, values := b.GenFakeData(workerId, colConfs, db.WithAutoInc(getDBDriver(b)))

			queryMap := make(map[string]interface{})
			for k, col := range columns {
				fields := strings.Split(col, ".")

				if len(fields) == 1 {
					queryMap[col] = values[k]
				} else {
					var m = queryMap
					for j, f := range fields {
						if j == len(fields)-1 {
							m[f] = values[k]
						} else {
							if _, ok := m[f]; !ok {
								m[f] = make(map[string]interface{})
							}
							m = m[f].(map[string]interface{})
						}
					}
				}
			}

			// Add nested fields
			history := make([]interface{}, 0, maxHistoryPerItem)
			for _ = range b.Randomizer.GetWorker(workerId).Intn(maxHistoryPerItem) {
				history = append(history, map[string]interface{}{
					"event":        esDataEventType[b.Randomizer.GetWorker(workerId).Intn(len(esDataEventType))],
					"history_type": esDataHistoryType[b.Randomizer.GetWorker(workerId).Intn(len(esDataHistoryType))],
					"timestamp":    time.Now().Format(time.RFC3339),
				})
			}

			queryMap["history"] = history

			meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, i, "\n"))

			data, err := json.Marshal(queryMap)
			if err != nil {
				b.Exit(fmt.Sprintf("error encoding document: %v", err))
			}

			// Append newline to the data payload
			//
			data = append(data, "\n"...) // <-- Comment out to trigger failure for batch

			// Append payloads to the buffer (ignoring write errors)
			//
			buf.Grow(len(meta) + len(data))
			buf.Write(meta)
			buf.Write(data)
		}

		switch rawSession := c.database.RawSession().(type) {
		case *es8.Client:
			res, err := rawSession.Bulk(
				bytes.NewReader(buf.Bytes()),
				rawSession.Bulk.WithIndex(table.TableName),
				rawSession.Bulk.WithRefresh(`true`),
			)
			if err != nil {
				b.Exit(fmt.Sprintf("error indexing document: %v", err))
			}

			defer res.Body.Close()

			if res.IsError() {
				if res.StatusCode != http.StatusNotFound {
					b.Exit("failed to perform indexing: %s", res.String())
				}
			}

			if res.StatusCode >= 300 {
				b.Exit("failed to perform indexing: %s", res.String())
			}

			testFlushIndex(b, table.TableName, rawSession)
		}

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

func testAddHistoryNestedElasticsearch(b *benchmark.Benchmark, testDesc *TestDesc) {
	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	b.Worker = func(workerId int) (loops int) {
		workerData := b.WorkerData[workerId].(*DBWorkerData)

		var c = workerData.workingConn

		updateScript := `{"script": {"source": "ctx._source.history.add(params.history)","lang": "painless","params": {"history": {` +
			`"event": %q,` +
			`"history_type": %q,` +
			`"timestamp": %q` +
			`}}}}`

		// Prepare the buffer to store payload
		//
		var buf bytes.Buffer

		for _ = range batch {
			// Add nested fields
			finalUpdateScript := fmt.Sprintf(updateScript,
				esDataEventType[b.Randomizer.GetWorker(workerId).Intn(len(esDataEventType))],
				esDataHistoryType[b.Randomizer.GetWorker(workerId).Intn(len(esDataHistoryType))],
				time.Now().Format(time.RFC3339),
			)

			meta := []byte(fmt.Sprintf(`{ "update" : { "_id" : "%d" } }%s`, b.Randomizer.GetWorker(workerId).Intn(batch), "\n"))

			data := []byte(finalUpdateScript)

			// Append newline to the data payload
			//
			data = append(data, "\n"...) // <-- Comment out to trigger failure for batch

			// Append payloads to the buffer (ignoring write errors)
			//
			buf.Grow(len(meta) + len(data))
			buf.Write(meta)
			buf.Write(data)
		}

		switch rawSession := c.database.RawSession().(type) {
		case *es8.Client:
			res, err := rawSession.Bulk(
				bytes.NewReader(buf.Bytes()),
				rawSession.Bulk.WithIndex(table.TableName),
				rawSession.Bulk.WithRefresh(`true`),
			)
			if err != nil {
				b.Exit(fmt.Sprintf("error updating document: %v", err))
			}

			defer res.Body.Close()

			if res.IsError() {
				if res.StatusCode != http.StatusNotFound {
					b.Exit("failed to perform bulk update: %s", res.String())
				}
			}

			if res.StatusCode >= 300 {
				b.Exit("failed to perform bulk update: %s", res.String())
			}

			testFlushIndex(b, table.TableName, rawSession)
		}

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

func testDeleteHistoryNestedES(b *benchmark.Benchmark, testDesc *TestDesc) {
	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	b.Worker = func(workerId int) (loops int) {
		workerData := b.WorkerData[workerId].(*DBWorkerData)

		var c = workerData.workingConn

		updateScript := `{
  "script": {
    "source": "if (ctx._source.history != null) {ctx._source.history.removeIf(item -> item.history_type == params.history_type)}",
    "lang": "painless",
    "params": {
      "history_type": %q
    }
  },
  "query": {
    "match_all": {}
  }
}`

		toDelete := esDataHistoryType[b.Randomizer.GetWorker(workerId).Intn(len(esDataHistoryType))]

		finalUpdateScript := fmt.Sprintf(updateScript, toDelete)

		// Delete nested fields
		switch rawSession := c.database.RawSession().(type) {
		case *es8.Client:
			res, err := rawSession.UpdateByQuery(
				[]string{table.TableName},
				rawSession.UpdateByQuery.WithBody(strings.NewReader(finalUpdateScript)),
				rawSession.UpdateByQuery.WithConflicts(`proceed`),
				rawSession.UpdateByQuery.WithRefresh(true),
			)
			if err != nil {
				b.Exit(fmt.Sprintf("error updating document: %v", err))
			}

			defer res.Body.Close()

			if res.IsError() {
				if res.StatusCode != http.StatusNotFound {
					b.Exit("failed to perform indexing: %s", res.String())
				}
			}

			if res.StatusCode >= 300 {
				b.Exit("failed to perform indexing: %s", res.String())
			}

			testFlushIndex(b, table.TableName, rawSession)
		}

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

func testSearchNestedElasticsearch(b *benchmark.Benchmark, testDesc *TestDesc) {
	getQuery := func(workerId int) string {
		return `{
  "query": {
    "nested": {
      "path": "history",
      "query": {
        "bool": {
          "must": [
            { "term": { "history.history_type": "` + esDataHistoryType[b.Randomizer.GetWorker(workerId).Intn(len(esDataHistoryType))] + `" } }
          ]
        }
      }
    }
  }
}`
	}

	testSearchElasticsearch(b, testDesc, getQuery)
}

func testListNestedElasticsearch(b *benchmark.Benchmark, testDesc *TestDesc) {
	getQuery := func(_ int) string {
		return `{
  "query": {
    "match_all": {}
  }
}`
	}

	testSearchElasticsearch(b, testDesc, getQuery)
}

func createESPCTable(c *DBConnector, b *benchmark.Benchmark, table *TestTable) {
	mapping := `{
  "mappings": {
    "properties": {
      "@timestamp": {
        "type": "date"
      },
      "historyJoin": {
        "type": "join",
        "relations": {
          "document": "history"
        }
      },
      "euc_id": {
        "type": "long"
      },
			"sender": {
				"type": "keyword"
			},
			"recipient": {
				"type": "keyword"
			},
			"subject": {
				"type": "keyword"
			},
			"body": {
				"type": "text"
			},
      "doc_type": {
        "type": "keyword"
      },
      "history_event": {
        "type": "keyword"
      },
      "history_type": {
        "type": "keyword"
      },
      "history_timestamp": {
        "type": "date_nanos"
      },
      "progress": {
        "type": "long",
        "index": false
      },
      "uuid": {
        "type": "keyword"
      }
    }
  },
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "codec": "best_compression",
    "index": {
      "refresh_interval": "1m",
      "mapping": {
        "ignore_malformed": true
      }
    }
  }
}`

	switch rawSession := c.database.RawSession().(type) {
	case *es8.Client:
		existRes, err := rawSession.Indices.Exists([]string{table.TableName})
		if err != nil {
			b.Exit(fmt.Sprintf("error checking if index exist: %v", err))
		}

		defer existRes.Body.Close()

		if existRes.IsError() {
			if existRes.StatusCode != http.StatusNotFound {
				b.Exit("failed to check if index exist: %s", existRes.String())
			}
		}

		if existRes.StatusCode == http.StatusOK {
			return
		}

		res, err := rawSession.Indices.Create(
			table.TableName,
			rawSession.Indices.Create.WithBody(strings.NewReader(mapping)),
		)
		if err != nil {
			b.Exit(fmt.Sprintf("error creating index: %v", err))
		}

		defer res.Body.Close()

		if res.IsError() {
			if res.StatusCode != http.StatusNotFound {
				b.Exit("failed to create index: %s", res.String())
			}
		}

		if res.StatusCode >= 300 {
			b.Exit("failed to create index: %s", res.String())
		}
	}
}

func testInsertPCElasticsearch(b *benchmark.Benchmark, testDesc *TestDesc) {
	colConfs := testDesc.table.GetColumnsForInsert(db.WithAutoInc(getDBDriver(b)))

	if len(*colConfs) == 0 {
		b.Exit(fmt.Sprintf("internal error: no columns eligible for INSERT found in '%s' configuration", testDesc.table.TableName))
	}

	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	b.Worker = func(workerId int) (loops int) {
		workerData := b.WorkerData[workerId].(*DBWorkerData)

		var c = workerData.workingConn

		createESPCTable(c, b, table)

		// Prepare the buffer to store payload
		//
		var buf bytes.Buffer

		for i := range batch {
			columns, values := b.GenFakeData(workerId, colConfs, db.WithAutoInc(getDBDriver(b)))

			queryMap := make(map[string]interface{})
			for k, col := range columns {
				fields := strings.Split(col, ".")

				if len(fields) == 1 {
					queryMap[col] = values[k]
				} else {
					var m = queryMap
					for j, f := range fields {
						if j == len(fields)-1 {
							m[f] = values[k]
						} else {
							if _, ok := m[f]; !ok {
								m[f] = make(map[string]interface{})
							}
							m = m[f].(map[string]interface{})
						}
					}
				}
			}

			// Add parent child join on parent
			queryMap["historyJoin"] = "document"
			queryMap["doc_type"] = "email"

			meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, i, "\n"))

			data, err := json.Marshal(queryMap)
			if err != nil {
				b.Exit(fmt.Sprintf("error encoding document: %v", err))
			}

			// Append newline to the data payload
			//
			data = append(data, "\n"...) // <-- Comment out to trigger failure for batch

			// Append payloads to the buffer (ignoring write errors)
			//
			buf.Grow(len(meta) + len(data))
			buf.Write(meta)
			buf.Write(data)

			// Add child documents
			for _ = range b.Randomizer.GetWorker(workerId).Intn(maxHistoryPerItem) {
				history := map[string]interface{}{
					"doc_type":          "history",
					"history_event":     esDataEventType[b.Randomizer.GetWorker(workerId).Intn(len(esDataEventType))],
					"history_type":      esDataHistoryType[b.Randomizer.GetWorker(workerId).Intn(len(esDataHistoryType))],
					"history_timestamp": time.Now().Format(time.RFC3339),
					"historyJoin": map[string]interface{}{
						"name":   "history",
						"parent": fmt.Sprintf("%d", i),
					},
				}

				historyMeta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d_%s", "routing": "%d" } }%s`, i, b.Randomizer.GetWorker(workerId).UUID(), i, "\n"))

				historyData, err := json.Marshal(history)
				if err != nil {
					b.Exit(fmt.Sprintf("error encoding document: %v", err))
				}

				// Append newline to the data payload
				//
				historyData = append(historyData, "\n"...) // <-- Comment out to trigger failure for batch

				// Append payloads to the buffer (ignoring write errors)
				//
				buf.Grow(len(historyMeta) + len(historyData))
				buf.Write(historyMeta)
				buf.Write(historyData)
			}
		}

		switch rawSession := c.database.RawSession().(type) {
		case *es8.Client:
			res, err := rawSession.Bulk(
				bytes.NewReader(buf.Bytes()),
				rawSession.Bulk.WithIndex(table.TableName),
				rawSession.Bulk.WithRefresh(`true`),
			)
			if err != nil {
				b.Exit(fmt.Sprintf("error indexing document: %v", err))
			}

			defer res.Body.Close()

			if res.IsError() {
				if res.StatusCode != http.StatusNotFound {
					b.Exit("failed to perform indexing: %s", res.String())
				}
			}

			if res.StatusCode >= 300 {
				b.Exit("failed to perform indexing: %s", res.String())
			}

			testFlushIndex(b, table.TableName, rawSession)
		}

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}
func testAddHistoryPCElasticsearch(b *benchmark.Benchmark, testDesc *TestDesc) {
	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	b.Worker = func(workerId int) (loops int) {
		workerData := b.WorkerData[workerId].(*DBWorkerData)

		var c = workerData.workingConn

		// Prepare the buffer to store payload
		//
		var buf bytes.Buffer

		for _ = range batch {
			// Add child documents
			itemID := fmt.Sprintf("%d", b.Randomizer.GetWorker(workerId).Intn(batch))

			for _ = range b.Randomizer.GetWorker(workerId).Intn(maxHistoryPerItem) {
				history := map[string]interface{}{
					"doc_type":          "history",
					"history_event":     esDataEventType[b.Randomizer.GetWorker(workerId).Intn(len(esDataEventType))],
					"history_type":      esDataHistoryType[b.Randomizer.GetWorker(workerId).Intn(len(esDataHistoryType))],
					"history_timestamp": time.Now().Format(time.RFC3339),
					"historyJoin": map[string]interface{}{
						"name":   "history",
						"parent": itemID,
					},
				}

				meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s_%s", "routing": "%s" } }%s`, itemID, b.Randomizer.GetWorker(workerId).UUID(), itemID, "\n"))

				data, err := json.Marshal(history)
				if err != nil {
					b.Exit(fmt.Sprintf("error encoding document: %v", err))
				}

				// Append newline to the data payload
				//
				data = append(data, "\n"...) // <-- Comment out to trigger failure for batch

				// Append payloads to the buffer (ignoring write errors)
				//
				buf.Grow(len(meta) + len(data))
				buf.Write(meta)
				buf.Write(data)
			}
		}

		switch rawSession := c.database.RawSession().(type) {
		case *es8.Client:
			res, err := rawSession.Bulk(
				bytes.NewReader(buf.Bytes()),
				rawSession.Bulk.WithIndex(table.TableName),
				rawSession.Bulk.WithRefresh(`true`),
			)
			if err != nil {
				b.Exit(fmt.Sprintf("error updating document: %v", err))
			}

			defer res.Body.Close()

			if res.IsError() {
				if res.StatusCode != http.StatusNotFound {
					b.Exit("failed to perform bulk update: %s", res.String())
				}
			}

			if res.StatusCode >= 300 {
				b.Exit("failed to perform bulk update: %s", res.String())
			}

			testFlushIndex(b, table.TableName, rawSession)
		}

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

func testDeleteHistoryPCES(b *benchmark.Benchmark, testDesc *TestDesc) {
	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	b.Worker = func(workerId int) (loops int) {
		workerData := b.WorkerData[workerId].(*DBWorkerData)

		var c = workerData.workingConn

		deleteQuery := `{
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "history_type": %q
          }
        },
        {
          "has_parent": {
            "parent_type": "document",
            "query": {
              "match_all": {}
            }
          }
        }
      ]
    }
  }
}`

		toDelete := esDataHistoryType[b.Randomizer.GetWorker(workerId).Intn(len(esDataHistoryType))]

		finalUpdateScript := fmt.Sprintf(deleteQuery, toDelete)

		// Delete all matching children
		switch rawSession := c.database.RawSession().(type) {
		case *es8.Client:
			res, err := rawSession.DeleteByQuery(
				[]string{table.TableName},
				strings.NewReader(finalUpdateScript),
				rawSession.DeleteByQuery.WithConflicts(`proceed`),
				rawSession.DeleteByQuery.WithRefresh(true),
			)
			if err != nil {
				b.Exit(fmt.Sprintf("error deleting by query: %v", err))
			}

			defer res.Body.Close()

			if res.IsError() {
				if res.StatusCode != http.StatusNotFound {
					b.Exit("failed to delete by query: %s", res.String())
				}
			}

			if res.StatusCode >= 300 {
				b.Exit("failed to delete by query: %s", res.String())
			}

			// Flush index
			testFlushIndex(b, table.TableName, rawSession)
		}

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

func testSearchPCElasticsearch(b *benchmark.Benchmark, testDesc *TestDesc) {
	getQuery := func(workerId int) string {
		return `{
	"query": {
		"bool": {
			"should": [
				{
					"has_child": {
						"type": "history",
						"query": {
							"bool": {
								"must": [
									{
										"term": {
											"history_type": "` + esDataHistoryType[b.Randomizer.GetWorker(workerId).Intn(len(esDataHistoryType))] + `"
										}
									}
								]
							}
						},
						"inner_hits": {}
					}
				}
			]
		}
	}
}`
	}

	testSearchElasticsearch(b, testDesc, getQuery)
}

func testListPCElasticsearch(b *benchmark.Benchmark, testDesc *TestDesc) {
	getQuery := func(_ int) string {
		return `{
  "query": {
    "bool": {
      "should": [
        {
          "has_child": {
            "type": "history",
            "query": {
              "match_all": {}
            },
            "inner_hits": {}
          }
        },
        {
          "bool": {
            "filter": {
              "term": {
                "historyJoin": "document"
              }
            },
            "must_not": {
              "has_child": {
                "type": "history",
                "query": {
                  "match_all": {}
                }
              }
            }
          }
        }
      ]
    }
  }
}`
	}

	testSearchElasticsearch(b, testDesc, getQuery)
}

func testSearchElasticsearch(b *benchmark.Benchmark, testDesc *TestDesc, getQuery func(workerId int) string) {
	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	b.Worker = func(workerId int) (loops int) {
		c := b.WorkerData[workerId].(*DBWorkerData).workingConn

		switch rawSession := c.database.RawSession().(type) {
		case *es8.Client:
			res, err := rawSession.Search(
				rawSession.Search.WithIndex(table.TableName),
				rawSession.Search.WithBody(strings.NewReader(getQuery(workerId))),
				rawSession.Search.WithSize(batch),
			)
			if err != nil {
				b.Exit(fmt.Sprintf("error searching document: %v", err))
			}

			defer res.Body.Close()

			if res.IsError() {
				if res.StatusCode != http.StatusNotFound {
					b.Exit("failed to perform search: %s", res.String())
				}
			}
		}

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

func testFlushIndex(b *benchmark.Benchmark, indexName string, rawSession *es8.Client) {
	// Flush index
	res, err := rawSession.Indices.Flush(
		rawSession.Indices.Flush.WithIndex(indexName),
	)
	if err != nil {
		b.Exit(fmt.Sprintf("error flushing indexes: %v", err))
	}

	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode != http.StatusNotFound {
			b.Exit("failed flushing indexes: %s", res.String())
		}
	}

	if res.StatusCode >= 300 {
		b.Exit("failed flushing indexes: %s", res.String())
	}
}
