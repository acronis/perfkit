package benchmark

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Schema represents a JSON schema.
type Schema map[string]interface{}

var schemaID2Schema = make(map[int]Schema)
var jsonLock sync.RWMutex

// GenRandomJson generates a random JSON string based on the given schema cardinality.
func (rz *Randomizer) GenRandomJson(schemaCardinality int) string { //nolint:revive
	// Generate a random schema with nested objects
	var schema Schema

	schemaID := rz.Intn(schemaCardinality)

	jsonLock.RLock()

	if val, ok := schemaID2Schema[schemaID]; ok {
		schema = val
		jsonLock.RUnlock()
	} else {
		jsonLock.RUnlock()
		jsonLock.Lock()
		if _, ok := schemaID2Schema[schemaID]; ok {
			schema = val
		} else {
			schema = generateRandomSchema(rz, rz.Intn(6))
			schemaID2Schema[schemaID] = schema
		}
		jsonLock.Unlock()
	}

	// Generate random data based on the schema
	data := generateRandomData(rz, schema)

	// Convert the JSON object to a JSON string
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Println("Error:", err)

		return ""
	}

	// fmt.Printf("json schema #%d: %s\n", schemaID, string(jsonData))
	return string(jsonData)
}

// generateRandomSchema generates a random schema with the given depth level.
func generateRandomSchema(rz *Randomizer, depth int) Schema {
	schema := make(Schema)

	for i := 0; i < depth; i++ {
		key := fmt.Sprintf("field%d", i)

		// 50% chance of creating a nested object at each level
		if rz.Intn(2) == 0 && i < depth-1 {
			schema[key] = generateRandomSchema(rz, depth-1)
		} else {
			// 50% chance of having a string field and 50% chance of an integer field
			if rz.Intn(2) == 0 {
				schema[key] = "string"
			} else {
				schema[key] = "integer"
			}
		}
	}

	return schema
}

// generateRandomData generates random data based on the given schema.
func generateRandomData(rz *Randomizer, schema Schema) interface{} {
	data := make(map[string]interface{})

	for key, value := range schema {
		switch valueType := value.(type) {
		case Schema:
			// For nested objects, recursively generate random data
			data[key] = generateRandomData(rz, valueType)
		case string:
			// Generate random data based on the field type
			if valueType == "string" {
				data[key] = randomString(rz, []string{"foo", "bar", "baz", "needle"})
			} else if valueType == "integer" {
				data[key] = rz.Intn(100)
			}
		}
	}

	return data
}

// randomString returns a random element from the given string slice.
func randomString(rz *Randomizer, choices []string) string {
	return choices[rz.Intn(len(choices))]
}
