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
	"net/url"
	"os"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/plugins/ism"

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

type openSearchDialect struct{}

func (d *openSearchDialect) name() db.DialectName {
	return db.OPENSEARCH
}

func (d *openSearchDialect) getVectorType() fieldType {
	return "knn_vector"
}

// nolint:gocritic //TODO refactor unnamed returns
func openSearchCredentialsAndConnString(cs string, tlsEnabled bool) (string, string, string, error) {
	var u, err = url.Parse(cs)
	if err != nil {
		return "", "", "", fmt.Errorf("cannot parse connection url %v, err: %v", cs, err)
	}

	var username = u.User.Username()
	var password, _ = u.User.Password()

	var scheme string
	if tlsEnabled {
		scheme = "https"
	} else {
		scheme = "http"
	}

	var finalURL = url.URL{
		Scheme: scheme,
		Host:   u.Host,
	}
	cs = finalURL.String()

	return username, password, cs, nil
}

type openSearchConnector struct{}

func (c *openSearchConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	var adds []string
	var username, password, cs string
	var err error

	if s := os.Getenv(magicEsEnvVar); s == "" {
		username, password, cs, err = openSearchCredentialsAndConnString(cfg.ConnString, cfg.TLSEnabled)
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

	var ismClient *ism.Client
	if ismClient, err = ism.NewClient(ism.Config{Client: conf.Client}); err != nil {
		return nil, fmt.Errorf("db: cannot connect to ism client at %v, err: %v", cs, err)
	}

	var rw = &openSearchQuerier{client: openSearchClient}
	var mig = &openSearchMigrator{client: openSearchClient, ismClient: ismClient}
	return &esDatabase{
		rw:             rw,
		mig:            mig,
		dialect:        &openSearchDialect{},
		logTime:        cfg.LogOperationsTime,
		queryLogger:    cfg.QueryLogger,
		readRowsLogger: cfg.ReadRowsLogger,
	}, nil
}

func (c *openSearchConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.OPENSEARCH, nil
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
		if err = json.Unmarshal(hit.Fields, &documentFields); err != nil {
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
		if osStructError, ok := err.(*opensearch.StructError); ok {
			if osStructError.Err.Type == "index_not_found_exception" ||
				(osStructError.Err.Type == "status_exception" && osStructError.Err.Reason == "Policy not found") {
				return 0, nil
			}
		}

		return 0, fmt.Errorf("failed to perform count: %v", err)
	}

	return int64(resp.Count), nil
}

type openSearchMigrator struct {
	client    *opensearchapi.Client
	ismClient *ism.Client
}

func (m *openSearchMigrator) checkILMPolicyExists(policyName string) (bool, error) {
	var _, err = m.ismClient.Policies.Get(context.Background(), &ism.PoliciesGetReq{
		Policy: policyName,
	})

	if err != nil {
		if osStructError, ok := err.(*opensearch.StructError); ok {
			if osStructError.Err.Type == "index_not_found_exception" ||
				(osStructError.Err.Type == "status_exception" && osStructError.Err.Reason == "Policy not found") {
				return false, nil
			}
		}

		return false, fmt.Errorf("failed to check policy exists: %v", err)
	}

	return true, nil
}

func translateIlMPolicytoISMPolicy(policy indexLifecycleManagementPolicy) ism.PolicyBody {
	var states []ism.PolicyState
	for phase, phaseDef := range policy.Phases {
		if phase != indexPhaseHot {
			continue
		}

		for action, actionDef := range phaseDef.Actions {
			if action != indexActionRollover {
				continue
			}

			if indexRolloverAction, ok := actionDef.(indexRolloverActionSettings); ok {
				states = append(states, ism.PolicyState{
					Name: "rollover",
					Actions: []ism.PolicyStateAction{
						{
							Rollover: &ism.PolicyStateRollover{
								MinSize:             indexRolloverAction.MinSize,
								MinPrimaryShardSize: indexRolloverAction.MinPrimaryShardSize,
								MinIndexAge:         indexRolloverAction.MinAge,
							},
						},
					},
				})
			}
		}
	}

	return ism.PolicyBody{
		Description:  "Auto-generated policy",
		DefaultState: "rollover",
		States:       states,
	}
}

func (m *openSearchMigrator) initILMPolicy(policyName string, policyDefinition indexLifecycleManagementPolicy) error {
	var _, err = m.ismClient.Policies.Put(context.Background(), ism.PoliciesPutReq{
		Policy: policyName,
		Body:   ism.PoliciesPutBody{Policy: translateIlMPolicytoISMPolicy(policyDefinition)},
	})

	if err != nil {
		return fmt.Errorf("failed to init ISM policy: %v", err)
	}

	return nil
}

func (m *openSearchMigrator) deleteILMPolicy(policyName string) error {
	var _, err = m.ismClient.Policies.Delete(context.Background(), ism.PoliciesDeleteReq{
		Policy: policyName,
	})

	if err != nil {
		return fmt.Errorf("failed to delete ISM policy: %v", err)
	}

	return nil
}

func (m *openSearchMigrator) initComponentTemplate(templateName string, template componentTemplate) error {
	type openSearchIndexSettings struct {
		NumberOfShards     int    `json:"number_of_shards,omitempty"`
		NumberOfReplicas   int    `json:"number_of_replicas,omitempty"`
		IndexLifeCycleName string `json:"opendistro.index_state_management.policy_id,omitempty"`
	}

	type openSearchComponentTemplate struct {
		Settings *openSearchIndexSettings `json:"settings,omitempty"`
		Mappings *mappings                `json:"mappings,omitempty"`
	}

	var openSearchSettings *openSearchIndexSettings
	if template.Settings != nil {
		openSearchSettings = &openSearchIndexSettings{
			NumberOfShards:     template.Settings.NumberOfShards,
			NumberOfReplicas:   template.Settings.NumberOfReplicas,
			IndexLifeCycleName: template.Settings.IndexLifeCycleName,
		}
	}

	var query = struct {
		Template openSearchComponentTemplate `json:"template"`
	}{
		Template: openSearchComponentTemplate{
			Settings: openSearchSettings,
			Mappings: template.Mappings,
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("failed to encode template request: %v", err)
	}

	if _, err := m.client.ComponentTemplate.Create(context.Background(), opensearchapi.ComponentTemplateCreateReq{
		ComponentTemplate: templateName,
		Body:              &buf,
	}); err != nil {
		return fmt.Errorf("error while trying to put component template %s: %v", templateName, err)
	}

	return nil
}

func (m *openSearchMigrator) deleteComponentTemplate(templateName string) error {
	if _, err := m.client.ComponentTemplate.Delete(context.Background(), opensearchapi.ComponentTemplateDeleteReq{
		ComponentTemplate: templateName,
	}); err != nil {
		return fmt.Errorf("error while trying to delete component template %s: %v", templateName, err)
	}

	return nil
}

func (m *openSearchMigrator) initIndexTemplate(templateName string, indexPattern string, components []string) error {
	var initDataStreamQuery = struct {
		IndexPatters []string `json:"index_patterns"`
		DataStream   struct{} `json:"data_stream"`
		ComposedOf   []string `json:"composed_of"`
		Priority     int      `json:"priority"`
	}{
		IndexPatters: []string{indexPattern},
		DataStream:   struct{}{},
		ComposedOf:   components,
		Priority:     500,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(initDataStreamQuery); err != nil {
		return fmt.Errorf("failed to MarshalJSON index template %s body: %v", templateName, err)
	}

	if _, err := m.client.IndexTemplate.Create(context.Background(), opensearchapi.IndexTemplateCreateReq{
		IndexTemplate: templateName,
		Body:          &buf,
	}); err != nil {
		return fmt.Errorf("error while trying to create index template %s: %v", templateName, err)
	}

	return nil
}

func (m *openSearchMigrator) deleteIndexTemplate(templateName string) error {
	if _, err := m.client.IndexTemplate.Delete(context.Background(), opensearchapi.IndexTemplateDeleteReq{
		IndexTemplate: templateName,
	}); err != nil {
		return fmt.Errorf("error while trying to delete component template %s: %v", templateName, err)
	}

	return nil
}

func (m *openSearchMigrator) deleteDataStream(dataStreamName string) error {
	if _, err := m.client.DataStream.Delete(context.Background(), opensearchapi.DataStreamDeleteReq{
		DataStream: dataStreamName,
	}); err != nil {
		return fmt.Errorf("error while trying to delete data stream %s: %v", dataStreamName, err)
	}

	return nil
}
