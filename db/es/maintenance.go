package es

import (
	"fmt"

	"github.com/acronis/perfkit/db"
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
	case db.DataTypeBigIntAutoInc, db.DataTypeId, db.DataTypeInt:
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

type migrator interface {
	checkILMPolicyExists(policyName string) (bool, error)
	initILMPolicy(policyName string, policyDefinition indexLifecycleManagementPolicy) error
	deleteILMPolicy(policyName string) error

	initComponentTemplate(templateName string, template componentTemplate) error
	deleteComponentTemplate(templateName string) error

	initIndexTemplate(templateName string, indexPattern string, components []string) error
	deleteIndexTemplate(templateName string) error

	deleteDataStream(dataStreamName string) error
}

func indexExists(mig migrator, tableName string) (bool, error) {
	var ilmPolicyName = fmt.Sprintf("ilm-data-5gb-%s", tableName)
	return mig.checkILMPolicyExists(ilmPolicyName)
}

func createIndex(mig migrator, indexName string, indexDefinition *db.TableDefinition, tableMigrationDDL string) error {
	if err := createSearchQueryBuilder(indexName, indexDefinition.TableRows); err != nil {
		return err
	}

	var ilmPolicyName = fmt.Sprintf("ilm-data-5gb-%s", indexName)
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
	var ilmSettingName = fmt.Sprintf("ilm-settings-%s", indexName)

	if exists, err := mig.checkILMPolicyExists(ilmPolicyName); err != nil {
		return err
	} else if exists {
		return nil
	}

	if err := mig.initILMPolicy(ilmPolicyName, ilmPolicy); err != nil {
		return err
	}

	if err := mig.initComponentTemplate(ilmSettingName, componentTemplate{
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
		if row.Name == "id" {
			continue
		}

		mp[row.Name] = fieldSpec{
			Type:    convertToEsType(row.Type),
			Indexed: row.Indexed,
		}
	}

	if err := mig.initComponentTemplate(mappingTemplateName, componentTemplate{
		Settings: &indexResilienceSettings,
		Mappings: &mappings{Properties: mp},
	}); err != nil {
		return err
	}

	var indexPattern = fmt.Sprintf("%s*", indexName)
	if err := mig.initIndexTemplate(indexName, indexPattern, []string{ilmSettingName, mappingTemplateName}); err != nil {
		return err
	}

	return nil
}

func dropIndex(mig migrator, indexName string) error {
	var dataStreamName = indexName
	if err := mig.deleteDataStream(dataStreamName); err != nil {
		return err
	}

	if err := mig.deleteIndexTemplate(dataStreamName); err != nil {
		return err
	}

	var ilmSettingName = fmt.Sprintf("ilm-settings-%s", indexName)
	if err := mig.deleteComponentTemplate(ilmSettingName); err != nil {
		return err
	}

	var ilmPolicyName = fmt.Sprintf("ilm-data-5gb-%s", indexName)
	if err := mig.deleteILMPolicy(ilmPolicyName); err != nil {
		return err
	}

	return nil
}
