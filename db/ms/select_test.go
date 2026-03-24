package ms

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/acronis/perfkit/db"
)

func TestQuoteFilterValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"numeric integer", "42", "42"},
		{"numeric negative", "-5", "-5"},
		{"numeric float", "3.14", "3.14"},
		{"string value", "hello", "'hello'"},
		{"string with spaces", "hello world", "'hello world'"},
		{"uuid", "00000000-0000-0000-0000-000000000001", "'00000000-0000-0000-0000-000000000001'"},
		{"string with single quote", "it's", "'it\\'s'"},
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteFilterValue(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestBuildMeilisearchFilter(t *testing.T) {
	tests := []struct {
		name        string
		where       map[string][]string
		expected    string
		expectError bool
		errContains string
	}{
		{
			name:     "nil where",
			where:    nil,
			expected: "",
		},
		{
			name:     "empty where",
			where:    map[string][]string{},
			expected: "",
		},
		{
			name:     "single equality",
			where:    map[string][]string{"status": {"active"}},
			expected: "status = 'active'",
		},
		{
			name:     "numeric equality",
			where:    map[string][]string{"id": {"42"}},
			expected: "id = 42",
		},
		{
			name:     "less than",
			where:    map[string][]string{"age": {"lt(30)"}},
			expected: "age < 30",
		},
		{
			name:     "greater than or equal",
			where:    map[string][]string{"score": {"ge(100)"}},
			expected: "score >= 100",
		},
		{
			name:     "less than or equal",
			where:    map[string][]string{"score": {"le(50)"}},
			expected: "score <= 50",
		},
		{
			name:     "not equal",
			where:    map[string][]string{"type": {"ne(deleted)"}},
			expected: "type != 'deleted'",
		},
		{
			name:     "is null",
			where:    map[string][]string{"email": {db.SpecialConditionIsNull}},
			expected: "email IS NULL",
		},
		{
			name:     "is not null",
			where:    map[string][]string{"email": {db.SpecialConditionIsNotNull}},
			expected: "email IS NOT NULL",
		},
		{
			name:     "multiple conditions on same field",
			where:    map[string][]string{"age": {"gt(18)", "lt(65)"}},
			expected: "age > 18 AND age < 65",
		},
		{
			name:        "like unsupported",
			where:       map[string][]string{"name": {"like(john)"}},
			expectError: true,
			errContains: "like functions are not supported",
		},
		{
			name:        "hlike unsupported",
			where:       map[string][]string{"name": {"hlike(john)"}},
			expectError: true,
			errContains: "like functions are not supported",
		},
		{
			name:        "empty field name",
			where:       map[string][]string{"": {"value"}},
			expectError: true,
			errContains: "empty condition field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildMeilisearchFilter(tt.where)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestBuildSortParams(t *testing.T) {
	tests := []struct {
		name         string
		order        []string
		expectedSort []string
		expectVector bool
		vectorField  string
		vectorValues []float32
		expectError  bool
		errContains  string
	}{
		{
			name:         "empty order",
			order:        nil,
			expectedSort: nil,
		},
		{
			name:         "asc single",
			order:        []string{"asc(created_at)"},
			expectedSort: []string{"created_at:asc"},
		},
		{
			name:         "desc single",
			order:        []string{"desc(score)"},
			expectedSort: []string{"score:desc"},
		},
		{
			name:         "multiple sort fields",
			order:        []string{"asc(name)", "desc(id)"},
			expectedSort: []string{"name:asc", "id:desc"},
		},
		{
			name:         "nearest vector search",
			order:        []string{"nearest(embedding;L2;[1,2,3])"},
			expectVector: true,
			vectorField:  "embedding",
			vectorValues: []float32{1, 2, 3},
		},
		{
			name:        "nearest with wrong arg count",
			order:       []string{"nearest(embedding;L2)"},
			expectError: true,
			errContains: "nearest expects 3 arguments",
		},
		{
			name:        "sort and nearest mutually exclusive",
			order:       []string{"asc(id)", "nearest(embedding;L2;[1,2,3])"},
			expectError: true,
			errContains: "mutually exclusive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, vs, err := buildSortParams(tt.order)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				if tt.expectVector {
					require.NotNil(t, vs)
					assert.Equal(t, tt.vectorField, vs.field)
					assert.Equal(t, tt.vectorValues, vs.vector)
				} else {
					assert.Nil(t, vs)
					assert.Equal(t, tt.expectedSort, got)
				}
			}
		})
	}
}
