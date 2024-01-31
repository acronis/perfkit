package benchmark

import (
	"testing"
)

func TestGenRandomJsonWithExistingSchema(t *testing.T) {
	b := New()
	rw := NewRandomizerWorker(1, 1)
	schemaCardinality := 1
	schemaID2Schema[0] = Schema{"field0": "string"}

	json := b.GenRandomJson(rw, schemaCardinality)

	if json == "" {
		t.Errorf("GenRandomJson() error, json is empty")
	}
}

func TestGenRandomJsonWithNewSchema(t *testing.T) {
	b := New()
	rw := NewRandomizerWorker(1, 1)
	schemaCardinality := 1

	json := b.GenRandomJson(rw, schemaCardinality)

	if json == "" {
		t.Errorf("GenRandomJson() error, json is empty")
	}
}

func TestGenerateRandomSchema(t *testing.T) {
	rw := NewRandomizerWorker(1, 1)
	depth := 2

	schema := generateRandomSchema(rw, depth)

	if len(schema) != depth {
		t.Errorf("generateRandomSchema() error, schema depth = %v, want %v", len(schema), depth)
	}
}

func TestGenerateRandomDataWithStringField(t *testing.T) {
	rw := NewRandomizerWorker(1, 1)
	schema := Schema{"field0": "string"}

	data := generateRandomData(rw, schema)

	if _, ok := data.(map[string]interface{})["field0"].(string); !ok {
		t.Errorf("generateRandomData() error, field0 is not a string")
	}
}

func TestGenerateRandomDataWithIntegerField(t *testing.T) {
	rw := NewRandomizerWorker(1, 1)
	schema := Schema{"field0": "integer"}

	data := generateRandomData(rw, schema)

	if _, ok := data.(map[string]interface{})["field0"].(int); !ok {
		t.Errorf("generateRandomData() error, field0 is not an integer")
	}
}

func TestGenerateRandomDataWithNestedSchema(t *testing.T) {
	rw := NewRandomizerWorker(1, 1)
	schema := Schema{"field0": Schema{"field1": "string"}}

	data := generateRandomData(rw, schema)

	nestedData, ok := data.(map[string]interface{})["field0"].(map[string]interface{})
	if !ok {
		t.Errorf("generateRandomData() error, field0 is not a nested schema")
	}

	if _, ok := nestedData["field1"].(string); !ok {
		t.Errorf("generateRandomData() error, field1 is not a string")
	}
}
