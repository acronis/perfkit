package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/acronis/perfkit/db"

	es8 "github.com/elastic/go-elasticsearch/v8"
)

type indexPhase string

const (
	indexPhaseHot    indexPhase = "hot"
	indexPhaseWarm   indexPhase = "warm"
	indexPhaseCold   indexPhase = "cold"
	indexPhaseFrozen indexPhase = "frozen"
	indexPhaseDelete indexPhase = "delete"
)

type indexAction string

const (
	indexActionRollover           indexAction = "rollover"
	indexActionShrink             indexAction = "shrink"
	indexActionForceMerge         indexAction = "forcemerge"
	indexActionSearchableSnapshot indexAction = "searchable_snapshot"
	indexActionDelete             indexAction = "delete"
)

type indexRolloverActionSettings struct {
	MaxAge              string `json:"max_age,omitempty"`
	MaxDocs             string `json:"max_docs,omitempty"`
	MaxSize             string `json:"max_size,omitempty"`
	MaxPrimaryShardSize string `json:"max_primary_shard_size,omitempty"`
	MaxPrimaryShardDocs string `json:"max_primary_shard_docs,omitempty"`

	MinAge              string `json:"min_age,omitempty"`
	MinDocs             string `json:"min_docs,omitempty"`
	MinSize             string `json:"min_size,omitempty"`
	MinPrimaryShardSize string `json:"min_primary_shard_size,omitempty"`
	MinPrimaryShardDocs string `json:"min_primary_shard_docs,omitempty"`
}

type indexPhaseDefinition struct {
	MinAge  string                      `json:"min_age,omitempty"`
	Actions map[indexAction]interface{} `json:"actions,omitempty"`
}

type indexLifecycleManagementPolicy struct {
	Phases map[indexPhase]indexPhaseDefinition `json:"phases,omitempty"`
}

type indexSettings struct {
	NumberOfShards     int    `json:"number_of_shards,omitempty"`
	NumberOfReplicas   int    `json:"number_of_replicas,omitempty"`
	IndexLifeCycleName string `json:"index.lifecycle.name,omitempty"`
}

type fieldType string

const (
	fieldTypeLong     fieldType = "long"
	fieldTypeKeyword  fieldType = "keyword"
	fieldTypeBoolean  fieldType = "boolean"
	fieldTypeDateNano fieldType = "date_nanos"
)

func convertToEsType(t db.DataType) fieldType {
	switch t {
	case db.DataTypeId:
		return fieldTypeLong
	case db.DataTypeUUID:
		return fieldTypeKeyword
	case db.DataTypeString:
		return fieldTypeKeyword
	case db.DataTypeDateTime:
		return fieldTypeDateNano
	case db.DataTypeBoolean:
		return fieldTypeBoolean
	default:
		return fieldTypeKeyword
	}
}

type fieldSpec struct {
	Type    fieldType
	Indexed bool
}

func (s fieldSpec) MarshalJSON() ([]byte, error) {
	if s.Indexed {
		return []byte(fmt.Sprintf(`{"type":%q}`, s.Type)), nil
	}
	return []byte(fmt.Sprintf(`{"type":%q, "index": false}`, s.Type)), nil
}

type mapping map[string]fieldSpec
type indexName string

type mappings struct {
	Properties mapping `json:"properties"`
}

type componentTemplate struct {
	Settings *indexSettings `json:"settings,omitempty"`
	Mappings *mappings      `json:"mappings,omitempty"`
}

func checkPolicyExists(client *es8.Client, policyName string) (bool, error) {
	var res, err = client.ILM.GetLifecycle(
		client.ILM.GetLifecycle.WithContext(context.Background()),
		client.ILM.GetLifecycle.WithPolicy(policyName))
	if err != nil {
		return false, fmt.Errorf("error while trying to get lifecycle %s: %v", policyName, err)
	} else if res.IsError() {
		if res.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("error code [%d] in get lifecycle response: %s", res.StatusCode, res.String())
	}

	var b map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&b); err != nil {
		return false, fmt.Errorf("failed to decode ILM response: %v", err)
	}

	if _, ok := b[policyName].(map[string]interface{}); !ok {
		return false, fmt.Errorf("ILM missing %s", policyName)
	}

	return true, nil
}

func indexExists(client *es8.Client, tableName string) (bool, error) {
	var ilmPolicyName = "ilm-data-5gb"
	return checkPolicyExists(client, ilmPolicyName)
}

func initILMPolicy(client *es8.Client, policyName string, policyDefinition indexLifecycleManagementPolicy) error {
	var query = struct {
		Policy indexLifecycleManagementPolicy `json:"policy"`
	}{
		Policy: policyDefinition,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("failed to encode policy request: %v", err)
	}

	if res, err := client.ILM.PutLifecycle(policyName,
		client.ILM.PutLifecycle.WithContext(context.Background()),
		client.ILM.PutLifecycle.WithBody(&buf)); err != nil {
		return fmt.Errorf("error while trying to put lifecycle %s: %v", policyName, err)
	} else if res.IsError() {
		return fmt.Errorf("error code [%d] in put lifecycle response: %s", res.StatusCode, res.String())
	}

	return nil
}

func initComponentTemplate(client *es8.Client, templateName string, template componentTemplate) error {
	var query = struct {
		Template componentTemplate `json:"template"`
	}{
		Template: template,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("failed to encode template request: %v", err)
	}

	if res, err := client.Cluster.PutComponentTemplate(templateName, &buf,
		client.Cluster.PutComponentTemplate.WithContext(context.Background())); err != nil {
		return fmt.Errorf("error while trying to put component template %s: %v", templateName, err)
	} else if res.IsError() {
		return fmt.Errorf("error code [%d] in put component template response: %s", res.StatusCode, res.String())
	}

	return nil
}

func initIndexTemplate(client *es8.Client, idxTemplateName string, indexPattern string, components []string) error {
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
		return fmt.Errorf("failed to MarshalJSON index template %s body: %v", idxTemplateName, err)
	}

	if res, err := client.Indices.PutIndexTemplate(idxTemplateName, &buf,
		client.Indices.PutIndexTemplate.WithContext(context.Background())); err != nil {
		return fmt.Errorf("error while trying to create index template %s: %v", idxTemplateName, err)
	} else if res.IsError() {
		return fmt.Errorf("error code [%d] in create index template response: %s", res.StatusCode, res.String())
	}

	return nil
}

func createIndex(client *es8.Client, indexName string, indexDefinition *db.TableDefinition, tableMigrationDDL string) error {
	var ilmPolicyName = "ilm-data-5gb"
	var ilmPolicy = indexLifecycleManagementPolicy{
		Phases: map[indexPhase]indexPhaseDefinition{
			indexPhaseHot: {
				Actions: map[indexAction]interface{}{
					indexActionRollover: indexRolloverActionSettings{
						MaxPrimaryShardSize: "5gb",
					},
				},
			},
			indexPhaseDelete: {
				MinAge: "90d",
				Actions: map[indexAction]interface{}{
					indexActionDelete: struct{}{},
				},
			},
		},
	}
	var ilmSettingName = "ilm-settings"

	if exists, err := checkPolicyExists(client, ilmPolicyName); err != nil {
		return err
	} else if exists {
		return nil
	}

	if err := initILMPolicy(client, ilmPolicyName, ilmPolicy); err != nil {
		return err
	}

	if err := initComponentTemplate(client, ilmSettingName, componentTemplate{
		Settings: &indexSettings{
			IndexLifeCycleName: ilmPolicyName,
		},
	}); err != nil {
		return err
	}

	var numberOfShards = indexDefinition.Resilience.NumberOfShards
	if numberOfShards == 0 {
		numberOfShards = 1
	}

	var indexResilienceSettings = indexSettings{
		NumberOfShards:   numberOfShards,
		NumberOfReplicas: indexDefinition.Resilience.NumberOfReplicas,
	}

	var mappingTemplateName = fmt.Sprintf("mapping-%s", indexName)
	if len(indexDefinition.TableRows) == 0 {
		return fmt.Errorf("empty mapping")
	}

	if err := createSearchQueryBuilder(indexName, indexDefinition.TableRows); err != nil {
		return err
	}

	var mp = make(mapping)
	for _, row := range indexDefinition.TableRows {
		mp[row.Name] = fieldSpec{
			Type:    convertToEsType(row.Type),
			Indexed: row.Indexed,
		}
	}

	if err := initComponentTemplate(client, mappingTemplateName, componentTemplate{
		Settings: &indexResilienceSettings,
		Mappings: &mappings{Properties: mp},
	}); err != nil {
		return err
	}

	var indexPattern = fmt.Sprintf("%s*", indexName)
	if err := initIndexTemplate(client, indexName, indexPattern, []string{ilmSettingName, mappingTemplateName}); err != nil {
		return err
	}

	return nil
}

func dropIndex(client *es8.Client, indexName string) error {
	var dataStreamName = indexName
	if res, err := client.Indices.DeleteDataStream([]string{dataStreamName},
		client.Indices.DeleteDataStream.WithContext(context.Background())); err != nil {
		return fmt.Errorf("error while trying to delete data stream %s: %v", dataStreamName, err)
	} else if res.IsError() {
		if res.StatusCode != 404 {
			return fmt.Errorf("error code [%d] in delete data stream response: %s", res.StatusCode, res.String())
		}
	}

	if res, err := client.Indices.DeleteIndexTemplate(dataStreamName,
		client.Indices.DeleteIndexTemplate.WithContext(context.Background())); err != nil {
		return fmt.Errorf("error while trying to delete index template %s: %v", dataStreamName, err)
	} else if res.IsError() {
		return fmt.Errorf("error code [%d] in delete index template response: %s", res.StatusCode, res.String())
	}

	var ilmSettingName = "ilm-settings"
	if res, err := client.Cluster.DeleteComponentTemplate(ilmSettingName, client.Cluster.DeleteComponentTemplate.WithContext(context.Background())); err != nil {
		return fmt.Errorf("error while trying to delete ilm settings template %s: %v", ilmSettingName, err)
	} else if res.IsError() {
		return fmt.Errorf("error code [%d] in delete ilm settings template response: %s", res.StatusCode, res.String())
	}

	var ilmPolicyName = "ilm-data-5gb"
	if res, err := client.ILM.DeleteLifecycle(ilmPolicyName, client.ILM.DeleteLifecycle.WithContext(context.Background())); err != nil {
		return fmt.Errorf("error while trying to delete policy lifecycle %v: %v", ilmPolicyName, err)
	} else if res.IsError() {
		return fmt.Errorf("error code [%d] in ILM Lifecycle Delete response: %s", res.StatusCode, res.String())
	}

	return nil
}
