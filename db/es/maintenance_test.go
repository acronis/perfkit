package es

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/acronis/perfkit/db"
)

func (suite *TestingSuite) TestElasticSearchSchemaInit() {
	dbo, err := db.Open(db.Config{
		ConnString:      suite.ConnString,
		MaxOpenConns:    16,
		MaxConnLifetime: 1000 * time.Millisecond,
	})

	if err != nil {
		suite.T().Error("db create", err)
		return
	}

	var exists bool
	if exists, err = dbo.TableExists("perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else if exists {
		suite.T().Error("table exists")
		return
	}

	var tableSpec = testTableDefinition()

	if err = dbo.CreateTable("perf_table", tableSpec, ""); err != nil {
		suite.T().Error("create table", err)
		return
	}

	if exists, err = dbo.TableExists("perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else if !exists {
		suite.T().Error("table not exists")
		return
	}

	if err = dbo.DropTable("perf_table"); err != nil {
		suite.T().Error("drop table", err)
		return
	}

	if exists, err = dbo.TableExists("perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else if exists {
		suite.T().Error("table exists")
		return
	}
}

func Test_fieldSpecItem_MarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		fields  map[string]fieldSpec
		want    string
		wantErr bool
	}{
		{
			name: "Valid fieldSpec item",
			fields: map[string]fieldSpec{
				"field1": fieldSpecItem{
					Type:    "keyword",
					Dims:    3,
					Indexed: false,
				},
			},
			want:    `{"field1":{"type":"keyword","dims":3,"index":false}}`,
			wantErr: false,
		},
		{
			name: "Valid fieldSpec item with nested field",
			fields: map[string]fieldSpec{
				"field1": fieldSpecItem{
					Type:    fieldTypeKeyword,
					Dims:    3,
					Indexed: false,
				},
				"field2": fieldSpecObject{
					fieldSpecs: map[string]fieldSpec{
						"field3": fieldSpecItem{
							Type:    fieldTypeKeyword,
							Dims:    4,
							Indexed: true,
						},
					},
				},
			},
			want:    `{"field1":{"type":"keyword","dims":3,"index":false},"field2":{"properties":{"field3":{"type":"keyword","dims":4}}}}`,
			wantErr: false,
		},
		{
			name: "Multi-level nested fieldSpec item with object field",
			fields: map[string]fieldSpec{
				"field1": fieldSpecItem{
					Type:    fieldTypeKeyword,
					Dims:    3,
					Indexed: false,
				},
				"field2": fieldSpecObject{
					fieldSpecs: map[string]fieldSpec{
						"field3": fieldSpecItem{
							Type:    fieldTypeKeyword,
							Dims:    4,
							Indexed: true,
						},
						"field4": fieldSpecObject{
							fieldSpecs: map[string]fieldSpec{
								"field5": fieldSpecItem{
									Type:    fieldTypeKeyword,
									Dims:    3,
									Indexed: false,
								},
							},
						},
					},
				},
			},
			want:    `{"field1":{"type":"keyword","dims":3,"index":false},"field2":{"properties":{"field3":{"type":"keyword","dims":4},"field4":{"properties":{"field5":{"type":"keyword","dims":3,"index":false}}}}}}`,
			wantErr: false,
		},
		{
			name: "Valid fieldSpec history item with nested field",
			fields: map[string]fieldSpec{
				"field1": fieldSpecItem{
					Type:    fieldTypeKeyword,
					Dims:    3,
					Indexed: false,
				},
				"history": fieldSpecObject{
					Type: fieldTypeObject,
					fieldSpecs: map[string]fieldSpec{
						"history_type": fieldSpecItem{
							Type:    fieldTypeKeyword,
							Indexed: true,
						},
						"timestamp": fieldSpecItem{
							Type:    fieldTypeDateNano,
							Indexed: true,
						},
						"event": fieldSpecItem{
							Type:    fieldTypeKeyword,
							Indexed: false,
						},
					},
				},
			},
			want:    `{"field1":{"type":"keyword","dims":3,"index":false},"history":{"properties":{"event":{"type":"keyword","index":false},"history_type":{"type":"keyword"},"timestamp":{"type":"date_nanos"}}}}`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got bytes.Buffer
			err := json.NewEncoder(&got).Encode(tt.fields)

			if (err != nil) != tt.wantErr {
				t.Errorf("fieldSpec json marshal error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			resultStr := strings.TrimSpace(got.String())

			if resultStr != tt.want {
				t.Errorf("fieldSpec json marshal error, result does not match = %v, want %v", resultStr, tt.want)
			}
		})
	}
}

func Test_convertToEsType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tr   db.TableRow
		want fieldSpec
	}{
		{
			name: "Simple row item",
			tr:   db.TableRowItem{Name: "@timestamp", Type: db.DataTypeDateTime, Indexed: true},
			want: fieldSpecItem{Type: fieldTypeDateNano, Indexed: true},
		},
		{
			name: "Simple nested row item",
			tr: db.TableRowSubtable{
				Name: "history",
				Type: db.DataTypeNested,
				Subtable: []db.TableRow{
					db.TableRowItem{Name: "history_type", Type: db.DataTypeVarChar, Indexed: true},
					db.TableRowItem{Name: "timestamp", Type: db.DataTypeDateTime, Indexed: true},
					db.TableRowItem{Name: "event", Type: db.DataTypeVarChar, Indexed: true},
				},
			},
			want: fieldSpecObject{
				Type:     fieldTypeObject,
				IsNested: true,
				fieldSpecs: map[string]fieldSpec{
					"history_type": fieldSpecItem{Type: fieldTypeKeyword, Indexed: true},
					"timestamp":    fieldSpecItem{Type: fieldTypeDateNano, Indexed: true},
					"event":        fieldSpecItem{Type: fieldTypeKeyword, Indexed: true},
				},
			},
		},
		{
			name: "Multi-level nested row item",
			tr: db.TableRowSubtable{
				Name: "history",
				Type: db.DataTypeNested,
				Subtable: []db.TableRow{
					db.TableRowItem{Name: "history_type", Type: db.DataTypeVarChar, Indexed: true},
					db.TableRowItem{Name: "timestamp", Type: db.DataTypeDateTime, Indexed: true},
					db.TableRowItem{Name: "event", Type: db.DataTypeVarChar, Indexed: true},
					db.TableRowSubtable{
						Name: "history",
						Type: db.DataTypeNested,
						Subtable: []db.TableRow{
							db.TableRowItem{Name: "history_type", Type: db.DataTypeVarChar, Indexed: true},
							db.TableRowItem{Name: "timestamp", Type: db.DataTypeDateTime, Indexed: true},
							db.TableRowItem{Name: "event", Type: db.DataTypeVarChar, Indexed: false},
						},
					},
				},
			},
			want: fieldSpecObject{
				Type:     fieldTypeObject,
				IsNested: true,
				fieldSpecs: map[string]fieldSpec{
					"history_type": fieldSpecItem{Type: fieldTypeKeyword, Indexed: true},
					"timestamp":    fieldSpecItem{Type: fieldTypeDateNano, Indexed: true},
					"event":        fieldSpecItem{Type: fieldTypeKeyword, Indexed: true},
					"history": fieldSpecObject{
						Type:     fieldTypeObject,
						IsNested: true,
						fieldSpecs: map[string]fieldSpec{
							"history_type": fieldSpecItem{Type: fieldTypeKeyword, Indexed: true},
							"timestamp":    fieldSpecItem{Type: fieldTypeDateNano, Indexed: true},
							"event":        fieldSpecItem{Type: fieldTypeKeyword, Indexed: false},
						},
					},
				},
			},
		},
		{
			name: "Multi-level with mixed of nested and object row item",
			tr: db.TableRowSubtable{
				Name: "history",
				Type: db.DataTypeNested,
				Subtable: []db.TableRow{
					db.TableRowItem{Name: "history_type", Type: db.DataTypeVarChar, Indexed: true},
					db.TableRowItem{Name: "timestamp", Type: db.DataTypeDateTime, Indexed: true},
					db.TableRowItem{Name: "event", Type: db.DataTypeVarChar, Indexed: true},
					db.TableRowSubtable{
						Name: "history",
						Type: db.DataTypeObject,
						Subtable: []db.TableRow{
							db.TableRowItem{Name: "history_type", Type: db.DataTypeVarChar, Indexed: true},
							db.TableRowItem{Name: "timestamp", Type: db.DataTypeDateTime, Indexed: true},
							db.TableRowItem{Name: "event", Type: db.DataTypeVarChar, Indexed: false},
							db.TableRowSubtable{
								Name: "history",
								Type: db.DataTypeNested,
								Subtable: []db.TableRow{
									db.TableRowItem{Name: "history_type", Type: db.DataTypeVarChar, Indexed: true},
									db.TableRowItem{Name: "timestamp", Type: db.DataTypeDateTime, Indexed: true},
									db.TableRowItem{Name: "event", Type: db.DataTypeVarChar, Indexed: false},
								},
							},
						},
					},
				},
			},
			want: fieldSpecObject{
				Type:     fieldTypeObject,
				IsNested: true,
				fieldSpecs: map[string]fieldSpec{
					"history_type": fieldSpecItem{Type: fieldTypeKeyword, Indexed: true},
					"timestamp":    fieldSpecItem{Type: fieldTypeDateNano, Indexed: true},
					"event":        fieldSpecItem{Type: fieldTypeKeyword, Indexed: true},
					"history": fieldSpecObject{
						Type:     fieldTypeObject,
						IsNested: false,
						fieldSpecs: map[string]fieldSpec{
							"history_type": fieldSpecItem{Type: fieldTypeKeyword, Indexed: true},
							"timestamp":    fieldSpecItem{Type: fieldTypeDateNano, Indexed: true},
							"event":        fieldSpecItem{Type: fieldTypeKeyword, Indexed: false},
							"history": fieldSpecObject{
								Type:     fieldTypeObject,
								IsNested: true,
								fieldSpecs: map[string]fieldSpec{
									"history_type": fieldSpecItem{Type: fieldTypeKeyword, Indexed: true},
									"timestamp":    fieldSpecItem{Type: fieldTypeDateNano, Indexed: true},
									"event":        fieldSpecItem{Type: fieldTypeKeyword, Indexed: false},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := convertToEsType(nil, tt.tr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertToEsType() = %v, want %v", got, tt.want)
			}
		})
	}
}
