package es

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/acronis/perfkit/db"
)

// nolint: gochecknoinits // remove init() when we will have a better way to register connectors
func init() {
	for _, openSearchNameStyle := range []string{"os", "opensearch"} {
		if err := db.Register(openSearchNameStyle, &openSearchConnector{}); err != nil {
			panic(err)
		}
	}
}

type openSearchConnector struct{}

func (c *openSearchConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	var adds []string
	var username, password, cs string
	var err error

	if s := os.Getenv(magicEsEnvVar); s == "" {
		username, password, cs, err = elasticCredentialsAndConnString(cfg.ConnString, cfg.TLSEnabled)
		if err != nil {
			return nil, fmt.Errorf("db: openSearch: %v", err)
		}

		adds = append(adds, cs)
	}

	var tlsConfig tls.Config
	if len(cfg.TLSCACert) == 0 {
		tlsConfig = tls.Config{
			InsecureSkipVerify: true, // nolint:gosec // TODO: InsecureSkipVerify is true
		}
	} else {
		var caCertPool = x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(cfg.TLSCACert)

		// nolint:gosec // TODO: TLS MinVersion too low
		tlsConfig = tls.Config{
			RootCAs: caCertPool,
		}
	}

	var conf = opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: adds,
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   cfg.MaxOpenConns,
				ResponseHeaderTimeout: cfg.MaxConnLifetime,
				DialContext:           (&net.Dialer{Timeout: cfg.MaxConnLifetime}).DialContext,
				TLSClientConfig:       &tlsConfig,
			},
			Username: username,
			Password: password,
		},
	}

	var openSearchClient *opensearchapi.Client
	if openSearchClient, err = opensearchapi.NewClient(conf); err != nil {
		return nil, fmt.Errorf("db: cannot connect to es db at %v, err: %v", cs, err)
	}

	var ping *opensearch.Response
	if ping, err = openSearchClient.Ping(context.Background(), nil); err != nil {
		if err.Error() == "EOF" {
			return nil, fmt.Errorf("db: failed ping OpenSearch db at %s, TLS CA required", cs)
		}
		return nil, fmt.Errorf("db: failed ping OpenSearch db at %v, err: %v", cs, err)
	} else if ping != nil && ping.IsError() {
		return nil, fmt.Errorf("db: failed ping OpenSearch db at %v, OpenSearch err: %v", cs, ping.String())
	}

	var rw = &openSearchQuerier{client: openSearchClient}
	return &esDatabase{
		rw:          rw,
		queryLogger: cfg.QueryLogger,
	}, nil
}

func (c *openSearchConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.ELASTICSEARCH, nil
}

type openSearchQuerier struct {
	client *opensearchapi.Client
}

func (q *openSearchQuerier) ping(ctx context.Context) error {
	if ping, err := q.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("db: failed ping OpenSearch db, err: %v", err)
	} else if ping.IsError() {
		return fmt.Errorf("error %s res=%+v", ping.Status(), ping)
	}
	return nil
}

func (q *openSearchQuerier) stats() db.Stats {
	return db.Stats{}
}

func (q *openSearchQuerier) rawSession() interface{} {
	return q.client
}

func (q *openSearchQuerier) close() error {
	return nil
}

func (q *openSearchQuerier) insert(ctx context.Context, idxName indexName, query *BulkIndexRequest) (*BulkIndexResult, int, error) {
	var resp, err = q.client.Bulk(ctx, opensearchapi.BulkReq{
		Index: string(idxName),
		Body:  query.Reader(),
	})

	if err != nil {
		return nil, 0, fmt.Errorf("failed to perform insert: %v", err)
	}

	return &BulkIndexResult{
		Errors:    resp.Errors,
		TimeTaken: int64(resp.Took),
	}, 0, nil
}

func (q *openSearchQuerier) search(ctx context.Context, idxName indexName, request *SearchRequest) ([]map[string]interface{}, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return nil, fmt.Errorf("request encode error: %v", err)
	}

	var resp, err = q.client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{string(idxName)},
		Body:    &buf,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to perform search: %v", err)
	}

	var fields []map[string]interface{}
	for _, hit := range resp.Hits.Hits {
		var documentFields = make(map[string]interface{})
		if err = json.Unmarshal(hit.Source, &documentFields); err != nil {
			return nil, fmt.Errorf("failed to unmarshal source: %v", err)
		}

		fields = append(fields, documentFields)
	}

	return fields, nil
}

func (q *openSearchQuerier) count(ctx context.Context, idxName indexName, request *CountRequest) (int64, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return 0, fmt.Errorf("request encode error: %v", err)
	}

	var resp, err = q.client.Indices.Count(ctx, &opensearchapi.IndicesCountReq{
		Indices: []string{string(idxName)},
		Body:    &buf,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to perform count: %v", err)
	}

	return int64(resp.Count), nil
}
