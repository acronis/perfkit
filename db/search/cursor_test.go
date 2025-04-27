package search

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/acronis/perfkit/db"
)

type execState string
type retCode string

// Execution states and return codes
const (
	stateEnqueued  execState = "enqueued"
	stateCompleted execState = "completed"

	doneError     retCode = "error"
	doneAbandoned retCode = "abandoned"
)

type testStruct struct {
	ID          int64      `json:"id"`
	UUID        string     `json:"uuid" svd:"uuid,tm_uuid"`
	Queue       string     `json:"queue" svd:"queue,tm_str64"`
	State       execState  `json:"state"`
	EnqueuedAt  time.Time  `json:"enqueuedAt"`
	AssignedAt  *time.Time `json:"assignedAt,omitempty"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	ResultCode  retCode    `json:"resultCode,omitempty"`
}

func (entity testStruct) Unique(field string) bool {
	switch field {
	case "id", "uuid", "enqueuedAt", "assignedAt", "startedAt", "updatedAt", "completedAt":
		return true
	default:
		return false
	}
}

func (entity testStruct) Nullable(field string) bool {
	switch field {
	case "assignedAt", "startedAt", "completedAt":
		return true
	default:
		return false
	}
}

func (entity testStruct) Cursor(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("empty order field")
	}

	// mapping for all fields that can be used for ordering
	var cursor string
	switch field {
	case "id":
		cursor = id2str(entity.ID)
	case "uuid":
		cursor = entity.UUID
	case "state":
		cursor = string(entity.State)
	case "enqueuedAt":
		cursor = entity.EnqueuedAt.UTC().Format(time.RFC3339Nano)
	case "assignedAt":
		if entity.AssignedAt != nil {
			cursor = entity.AssignedAt.UTC().Format(time.RFC3339Nano)
		}
	case "startedAt":
		if entity.StartedAt != nil {
			cursor = entity.StartedAt.UTC().Format(time.RFC3339Nano)
		}
	case "updatedAt":
		cursor = entity.UpdatedAt.UTC().Format(time.RFC3339Nano)
	case "completedAt":
		if entity.CompletedAt != nil {
			cursor = entity.CompletedAt.UTC().Format(time.RFC3339Nano)
		}
	case "resultCode":
		cursor = string(entity.ResultCode)
	default:
		return "", fmt.Errorf("unknown field %s", field)
	}

	if cursor == "" {
		cursor = db.SpecialConditionIsNull
	}

	return cursor, nil
}

func TestUniqueSort(t *testing.T) {
	type testUniqueSort struct {
		initialSort []string

		encoded []string
		sorts   []sorting
		err     error
	}

	var tests = []testUniqueSort{
		{
			initialSort: []string{"asc(id)"},

			encoded: []string{"asc(id)"},
			sorts: []sorting{
				{
					Field: "id",
					Func:  "asc",
				},
			},
		},
		{
			initialSort: []string{"asc(state)", "asc(resultCode)"},

			encoded: []string{"asc(state)", "asc(resultCode)", "asc(id)"},
			sorts: []sorting{
				{
					Field: "state",
					Func:  "asc",
				},
				{
					Field: "resultCode",
					Func:  "asc",
				},
				{
					Field: "id",
					Func:  "asc",
				},
			},
		},
		{
			initialSort: []string{"desc(state)", "desc(resultCode)"},

			encoded: []string{"desc(state)", "desc(resultCode)", "desc(id)"},
			sorts: []sorting{
				{
					Field: "state",
					Func:  "desc",
				},
				{
					Field: "resultCode",
					Func:  "desc",
				},
				{
					Field: "id",
					Func:  "desc",
				},
			},
		},
		{
			initialSort: []string{"desc(enqueuedAt)"},

			encoded: []string{"desc(enqueuedAt)"},
			sorts: []sorting{
				{
					Field: "enqueuedAt",
					Func:  "desc",
				},
			},
		},
		{
			initialSort: []string{"asc(completedAt)", "asc(id)"},

			encoded: []string{"asc(completedAt)", "asc(id)"},
			sorts: []sorting{
				{
					Field: "completedAt",
					Func:  "asc",
				},
				{
					Field: "id",
					Func:  "asc",
				},
			},
		},
		{
			initialSort: []string{"asc(completedAt)", "desc(id)"},

			encoded: []string{"asc(completedAt)", "desc(id)"},
			sorts: []sorting{
				{
					Field: "completedAt",
					Func:  "asc",
				},
				{
					Field: "id",
					Func:  "desc",
				},
			},
		},
		{
			initialSort: []string{"asc(completedAt)"},

			encoded: []string{"asc(completedAt)", "asc(id)"},
			sorts: []sorting{
				{
					Field: "completedAt",
					Func:  "asc",
				},
				{
					Field: "id",
					Func:  "asc",
				},
			},
		},
		{
			initialSort: []string{"asc(id)", "asc(completedAt)"},

			encoded: []string{"asc(id)"},
			sorts: []sorting{
				{
					Field: "id",
					Func:  "asc",
				},
			},
		},
	}

	for _, test := range tests {
		var encodedSorts, sorts, err = uniqueSort[testStruct](test.initialSort, nil, testStruct{})
		if err != nil {
			if test.err != nil {
				assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for sorts %v", test.initialSort)
			} else {
				t.Errorf("failure in test for sorts %v, err: %v", test.initialSort, err)
			}

			continue
		}

		assert.Equalf(t, test.encoded, encodedSorts, "failure in test for sorts %v, err: %v", test.initialSort, err)
		assert.Equalf(t, test.sorts, sorts, "failure in test for sorts %v, err: %v", test.initialSort, err)
	}
}

func TestSplitQueryOnLightWeightQueries(t *testing.T) {
	type splitTest struct {
		pt  PageToken
		uf  map[string]bool
		pts []PageToken
		err error
	}

	var tests = []splitTest{
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(state)", "asc(resultCode)"},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Order:  []string{"asc(state)", "asc(resultCode)", "asc(id)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"desc(enqueuedAt)"},
				Cursor: map[string]string{
					"enqueuedAt": "10",
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"enqueuedAt": {"lt(10)"},
					},
					Order: []string{"desc(enqueuedAt)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(state)", "asc(resultCode)"},
				Cursor: map[string]string{
					"state":      string(stateCompleted),
					"resultCode": string(doneAbandoned),
					"id":         id2str(10),
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state":      {string(stateCompleted)},
						"resultCode": {string(doneAbandoned)},
						"id":         {"gt(10)"},
					},
					Order: []string{"asc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state":      {string(stateCompleted)},
						"resultCode": {fmt.Sprintf("gt(%s)", doneAbandoned)},
					},
					Order: []string{"asc(resultCode)", "asc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state": {fmt.Sprintf("gt(%s)", stateCompleted)},
					},
					Order: []string{"asc(state)", "asc(resultCode)", "asc(id)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(state)", "asc(resultCode)"},
				Cursor: map[string]string{
					"state":      string(stateEnqueued),
					"resultCode": db.SpecialConditionIsNull,
					"id":         id2str(10),
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state":      {string(stateEnqueued)},
						"resultCode": {db.SpecialConditionIsNull},
						"id":         {"gt(10)"},
					},
					Order: []string{"asc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state":      {string(stateEnqueued)},
						"resultCode": {db.SpecialConditionIsNotNull},
					},
					Order: []string{"asc(resultCode)", "asc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state": {fmt.Sprintf("gt(%s)", stateEnqueued)},
					},
					Order: []string{"asc(state)", "asc(resultCode)", "asc(id)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(state)", "asc(resultCode)"},
				Cursor: map[string]string{
					"state":      string(stateCompleted),
					"resultCode": string(doneError),
					"id":         id2str(10),
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state":      {string(stateCompleted)},
						"resultCode": {string(doneError)},
						"id":         {"gt(10)"},
					},
					Order: []string{"asc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state":      {string(stateCompleted)},
						"resultCode": {fmt.Sprintf("gt(%s)", string(doneError))},
					},
					Order: []string{"asc(resultCode)", "asc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state": {fmt.Sprintf("gt(%s)", stateCompleted)},
					},
					Order: []string{"asc(state)", "asc(resultCode)", "asc(id)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"desc(state)", "desc(resultCode)"},
				Cursor: map[string]string{
					"state":      string(stateEnqueued),
					"resultCode": db.SpecialConditionIsNull,
					"id":         id2str(10),
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state":      {string(stateEnqueued)},
						"resultCode": {db.SpecialConditionIsNull},
						"id":         {"lt(10)"},
					},
					Order: []string{"desc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"state": {fmt.Sprintf("lt(%s)", stateEnqueued)},
					},
					Order: []string{"desc(state)", "desc(resultCode)", "desc(id)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{}, // Count
				Order:  []string{"asc(some_id)", "asc(time)", "asc(id)"},
				Filter: map[string][]string{
					"some_id": {"2000"},
					"time":    {"567"},
					"id":      {"10"},
				},
			},
			pts: []PageToken{
				{
					Fields: []string{}, // Count
					Order:  []string{"asc(some_id)", "asc(time)", "asc(id)"},
					Filter: map[string][]string{
						"some_id": {"2000"},
						"time":    {"567"},
						"id":      {"10"},
					},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(completedAt)", "asc(id)"},
				Cursor: map[string]string{
					"completedAt": db.SpecialConditionIsNull,
					"id":          "10",
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"completedAt": {db.SpecialConditionIsNull},
						"id":          {"gt(10)"},
					},
					Order: []string{"asc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"completedAt": {db.SpecialConditionIsNotNull},
					},
					Order: []string{"asc(completedAt)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(completedAt)", "desc(id)"},
				Cursor: map[string]string{
					"completedAt": db.SpecialConditionIsNull,
					"id":          "10",
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"completedAt": {db.SpecialConditionIsNull},
						"id":          {"lt(10)"},
					},
					Order: []string{"desc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"completedAt": {db.SpecialConditionIsNotNull},
					},
					Order: []string{"asc(completedAt)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Cursor: map[string]string{
					"completedAt": db.SpecialConditionIsNull,
					"id":          "10",
				},
				Order: []string{"asc(completedAt)"},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"completedAt": {db.SpecialConditionIsNull},
						"id":          {"gt(10)"},
					},
					Order: []string{"asc(id)"},
				},
				{
					Fields: []string{"id"},
					Filter: map[string][]string{
						"completedAt": {db.SpecialConditionIsNotNull},
					},
					Order: []string{"asc(completedAt)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(completedAt)"},
				Cursor: map[string]string{
					"completedAt": "15",
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Order:  []string{"asc(completedAt)"},
					Filter: map[string][]string{
						"completedAt": {"gt(15)"},
					},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"desc(completedAt)"},
				Cursor: map[string]string{
					"completedAt": "15",
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Order:  []string{"desc(completedAt)", "desc(id)"},
					Filter: map[string][]string{
						"completedAt": {"lt(15)"},
					},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"desc(completedAt)", "desc(id)"},
				Cursor: nil,
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Order:  []string{"desc(completedAt)", "desc(id)"},
					Filter: nil,
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"desc(completedAt)"},
				Cursor: nil,
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Order:  []string{"desc(completedAt)", "desc(id)"},
					Filter: nil,
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"desc(completedAt)"},
				Cursor: map[string]string{
					"completedAt": db.SpecialConditionIsNull,
					"id":          "10",
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Order:  []string{"desc(id)"},
					Filter: map[string][]string{
						"completedAt": {db.SpecialConditionIsNull},
						"id":          {"lt(10)"},
					},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(state)", "asc(completedAt)"},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Order:  []string{"asc(state)", "asc(completedAt)", "asc(id)"},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(state)", "asc(completedAt)"},
				Cursor: map[string]string{
					"state":       string(stateEnqueued),
					"completedAt": db.SpecialConditionIsNull,
					"id":          "10",
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Order:  []string{"asc(id)"},
					Filter: map[string][]string{
						"state":       {string(stateEnqueued)},
						"completedAt": {db.SpecialConditionIsNull},
						"id":          {"gt(10)"},
					},
				},
				{
					Fields: []string{"id"},
					Order:  []string{"asc(completedAt)"},
					Filter: map[string][]string{
						"state":       {string(stateEnqueued)},
						"completedAt": {db.SpecialConditionIsNotNull},
					},
				},
				{
					Fields: []string{"id"},
					Order:  []string{"asc(state)", "asc(completedAt)", "asc(id)"},
					Filter: map[string][]string{
						"state": {fmt.Sprintf("gt(%s)", stateEnqueued)},
					},
				},
			},
		},
		{
			pt: PageToken{
				Fields: []string{"id"},
				Order:  []string{"asc(state)", "asc(startedAt)", "asc(completedAt)"},
				Cursor: map[string]string{
					"state":       string(stateEnqueued),
					"startedAt":   db.SpecialConditionIsNull,
					"completedAt": db.SpecialConditionIsNull,
					"id":          "10",
				},
			},
			pts: []PageToken{
				{
					Fields: []string{"id"},
					Order:  []string{"asc(id)"},
					Filter: map[string][]string{
						"state":       {string(stateEnqueued)},
						"startedAt":   {db.SpecialConditionIsNull},
						"completedAt": {db.SpecialConditionIsNull},
						"id":          {"gt(10)"},
					},
				},
				{
					Fields: []string{"id"},
					Order:  []string{"asc(completedAt)"},
					Filter: map[string][]string{
						"state":       {string(stateEnqueued)},
						"startedAt":   {db.SpecialConditionIsNull},
						"completedAt": {db.SpecialConditionIsNotNull},
					},
				},
				{
					Fields: []string{"id"},
					Order:  []string{"asc(startedAt)"},
					Filter: map[string][]string{
						"state":     {string(stateEnqueued)},
						"startedAt": {db.SpecialConditionIsNotNull},
					},
				},
				{
					Fields: []string{"id"},
					Order:  []string{"asc(state)", "asc(startedAt)", "asc(completedAt)", "asc(id)"},
					Filter: map[string][]string{
						"state": {fmt.Sprintf("gt(%s)", string(stateEnqueued))},
					},
				},
			},
		},
	}

	for _, test := range tests {
		var res, err = splitQueryOnLightWeightQueries[testStruct](test.pt, testStruct{})
		if err != nil {
			if test.err != nil {
				assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for pt %v with unique values %v", test.pt, test.uf)
			} else {
				t.Errorf("failure in test for pt %v with unique values %v, err: %v", test.pt, test.uf, err)
			}

			continue
		}

		require.Equal(t, len(test.pts), len(res))
		for i := range test.pts {
			require.Equal(t, test.pts[i], res[i])
		}
	}
}

func TestCreateNextCursorBasedPageTokenForTasks(t *testing.T) {
	type taskCursorTest struct {
		items []testStruct
		pt    *PageToken
		err   error
	}

	var tests = []taskCursorTest{
		{
			items: nil,
			pt:    nil,
			err:   nil,
		},
	}

	for _, test := range tests {
		var res, err = createNextCursorBasedPageToken[testStruct](PageToken{}, test.items, 10, testStruct{})
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for tasks %v", test.items)
			continue
		}

		assert.Equalf(t, test.pt, res, "wrong empty value in test for tasks %v", test.items)
	}
}
