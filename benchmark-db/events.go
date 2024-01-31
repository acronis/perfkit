package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	guuid "github.com/google/uuid"

	"github.com/acronis/perfkit/benchmark"
)

// EventTopic is a helper structure to simulate event topics
type EventTopic struct {
	InternalID int    `db:"internal_id"`
	TopicID    string `db:"topic_id"`
}

// EventType is a helper structure to simulate event types
type EventType struct {
	InternalID      int    `db:"internal_id"`
	TopicInternalID int    `db:"topic_internal_id"`
	EventType       string `db:"event_type"`
}

// EventData is a helper structure to simulate event data
type EventData struct {
	InternalID          int64     `db:"internal_id"`
	TopicInternalID     int64     `db:"topic_internal_id"`
	EventTypeInternalID int64     `db:"event_type_internal_id"`
	EventID             string    `db:"event_id"`
	Source              string    `db:"source"`
	Sequence            int64     `db:"sequence"`
	TenantID            string    `db:"tenant_id"`
	ClientID            string    `db:"client_id"`
	TraceParent         string    `db:"trace_parent"`
	SubjectID           string    `db:"subject_id"`
	DataRef             string    `db:"data_ref"`
	Data                string    `db:"data"`
	DataBase64          string    `db:"data_base64"`
	CreatedAt           time.Time `db:"created_at"`
	ConsolidationKey    string    `db:"consolidation_key"`
}

// MaxTopics is the maximum number of topics supported by the event bus
var MaxTopics = 8

// EventBus is a helper structure to simulate event bus
type EventBus struct {
	workerConn      *benchmark.DBConnector
	workerStarted   bool
	stopCh          chan bool
	wg              sync.WaitGroup
	batchSize       int
	sleepMsec       int
	workerIteration uint64
}

// NewEventBus creates a new event bus worker instance
func NewEventBus(dbOpts *benchmark.DatabaseOpts, logger *benchmark.Logger) *EventBus {
	return &EventBus{
		workerConn:    benchmark.NewDBConnector(dbOpts, -1, logger, 1),
		workerStarted: false,
		stopCh:        make(chan bool),
		batchSize:     500,
		sleepMsec:     10,
	}
}

// Log is a helper function to log event bus messages
func (e *EventBus) Log(LogLevel int, format string, args ...interface{}) {
	msg := "eventbus: " + fmt.Sprintf(format, args...)
	e.workerConn.Log(LogLevel, msg)
}

// MainLoop is the main worker loop for the event bus
func (e *EventBus) MainLoop() {
	defer e.wg.Done()

	for {
		if e.QueueIsEmpty() {
			select {
			case <-e.stopCh:
				e.Log(benchmark.LogTrace, "stopping main worker loop")

				return
			default:
			}
		}

		time.Sleep(time.Duration(e.sleepMsec) * time.Millisecond)
		e.Work()
	}
}

// QueueIsEmpty returns true if the event bus queue is empty
func (e *EventBus) QueueIsEmpty() bool {
	c := e.workerConn

	return c.GetRowsCount("acronis_db_bench_eventbus_events", "") == 0
}

// Start starts the event bus worker
func (e *EventBus) Start() {
	c := e.workerConn
	if c.DbOpts.Driver == benchmark.CLICKHOUSE {
		c.Exit("event bus is not supported for ClickHouse DB")
	}
	if c.DbOpts.Driver == benchmark.CASSANDRA {
		c.Exit("event bus is not supported for Cassandra DB")
	}
	if e.workerStarted {
		return
	}
	e.workerStarted = true

	e.Log(benchmark.LogTrace, "worker start")

	e.wg.Add(1)
	go e.MainLoop()
}

// Stop stops the event bus worker
func (e *EventBus) Stop() {
	if !e.workerStarted {
		return
	}
	e.stopCh <- true
	e.wg.Wait()
	e.Log(benchmark.LogTrace, "worker stop")
}

// CreateTables creates all the tables required for the event bus
func (e *EventBus) CreateTables() {
	c := e.workerConn

	if c.TableExists("acronis_db_bench_eventbus_events") {
		return
	}

	if c.DbOpts.Driver == benchmark.CLICKHOUSE || c.DbOpts.Driver == benchmark.CASSANDRA {
		return
	}

	c.Begin()

	c.ApplyMigrations("", EventBusDDL)

	for i := 1; i < MaxTopics+1; i++ {
		eventTopic := EventTopic{
			InternalID: i,
			TopicID:    fmt.Sprintf("cti.a.p.em.topic.v1.0~a.p.my_topic.%d.v1.0", i),
		}
		c.InsertInto("acronis_db_bench_eventbus_topics", eventTopic, []string{"internal_id", "topic_id"})

		eventType := EventType{
			InternalID:      i,
			TopicInternalID: i,
			EventType:       fmt.Sprintf("cti.a.p.em.event.v1.0~a.p.my_event.%d.v1.0", i),
		}
		c.InsertInto("acronis_db_bench_eventbus_event_types", eventType, []string{"internal_id", "topic_internal_id", "event_type"})
	}

	c.ExecOrExit("INSERT INTO acronis_db_bench_eventbus_sequences (int_id, sequence) VALUES (1, 0)")

	c.Commit()

	c.Log(benchmark.LogDebug, "created EventBus tables and indexes")
}

// DropTables drops all the tables created by CreateTables()
func (e *EventBus) DropTables() {
	c := e.workerConn
	c.DropTable("acronis_db_bench_eventbus_consolidated")
	c.DropTable("acronis_db_bench_eventbus_archive")
	c.DropTable("acronis_db_bench_eventbus_distrlocks")
	c.DropTable("acronis_db_bench_eventbus_migrations")
	c.DropTable("acronis_db_bench_eventbus_sequences")
	c.DropTable("acronis_db_bench_eventbus_stream")
	c.DropTable("acronis_db_bench_eventbus_events")
	c.DropTable("acronis_db_bench_eventbus_initial_seeding_cursors")
	c.DropTable("acronis_db_bench_eventbus_event_types")
	c.DropTable("acronis_db_bench_eventbus_topics")
	c.DropTable("acronis_db_bench_eventbus_data")
}

// InsertEvent inserts a single event into the event bus
func (e *EventBus) InsertEvent(rw *benchmark.RandomizerWorker, conn *benchmark.DBConnector, subjectUUID string) {
	topicID := 1 + rw.Intn(MaxTopics)
	typeID := topicID
	eventUUID := guuid.New().String()
	tenantUUID := guuid.New().String()
	clientUUID := guuid.New().String()

	d := EventData{
		TopicInternalID:     int64(topicID),
		EventTypeInternalID: int64(typeID),
		EventID:             eventUUID,
		Source:              "",
		TenantID:            tenantUUID,
		ConsolidationKey:    "",
		Data: fmt.Sprintf("\"id\":\"%s\",\"t\":\"%s\",\"src\":\"com.acronis.eu2-cloud/account-server\",\"sub\":\"%s\",\"tid\":\"%s\",\"cid\":\"%s\",\"d\":\"%s\"",
			eventUUID, time.Now().Format(time.RFC3339Nano), subjectUUID, tenantUUID, clientUUID,
			"{\"id\":\"entity id\",\"name\":\"entity name\"}"),
	}

	e.Start()

	conn.InsertInto("acronis_db_bench_eventbus_events", d,
		[]string{"topic_internal_id", "event_type_internal_id", "event_id", "source", "tenant_id", "consolidation_key", "data"})
}

/*
 * Main worker
 */

type stepType func() //nolint:unused

// Work performs a single iteration of the worker
func (e *EventBus) Work() {
	e.workerIteration++
	e.Log(benchmark.LogTrace, fmt.Sprintf("worker iteration #%d start", e.workerIteration))
	if e.Step("phase #1 (aligner)", e.DoAlign) { // perf model: per event
		e.Step("phase #2 (max seq shifter)", e.DoMaxSeqShifter) // perf model: per batch
		e.Step("phase #3 (fetcher)", e.DoFetch)                 // perf model: per event, but in a batch
		// e.Step("phase # (window shift)", e.DoWindowShift)     // perf model: per larger batch, depends on ingest & delivery response
		// e.Step("phase # (consolidation)", e.DoConsolidate)    // rarely used
		// e.Step("phase # (fetch consolidated)", e.DoFetchConsolidated) // rarely used
		e.Step("phase #4 (archive)", e.DoArchive) // perf model: per event, but in a batch
		// e.Step("phase #5 (delete)", e.DoDelete)               // perf model: per event, but in a larger batch
	}
	e.Log(benchmark.LogTrace, fmt.Sprintf("worker iteration #%d end", e.workerIteration))
}

// Step is a helper function to log step start/end
func (e *EventBus) Step(msg string, dofunc func() bool) bool {
	e.Log(benchmark.LogTrace, msg+" start")
	ret := dofunc()
	e.Log(benchmark.LogTrace, msg+" end")

	return ret
}

// DoAlign simulates events alignment
func (e *EventBus) DoAlign() bool {
	c := e.workerConn

	var data []*EventData
	var ids []string

	var unused interface{}

	c.Begin()

	/*
	 * step #1 - get fresh events
	 */

	rows := c.Select("acronis_db_bench_eventbus_events",
		"internal_id, topic_internal_id, event_type_internal_id, event_id, source, sequence, tenant_id, client_id, trace_parent, "+
			"subject_id, data_ref, data, data_base64, created_at",
		"",
		"internal_id",
		e.batchSize,
		false)

	for rows.Next() {
		ed := &EventData{}
		err := rows.Scan(&ed.InternalID,
			&ed.TopicInternalID,
			&ed.EventTypeInternalID,
			&ed.EventID,
			&ed.Source,
			&unused, // &ed.Sequence
			&ed.TenantID,
			&unused, // &ed.ClientID
			&unused, // &ed.TraceParent
			&unused, // &ed.SubjectID
			&unused, // &ed.DataRef
			&ed.Data,
			&unused, // &ed.DataBase64
			&unused) // &ed.CreatedAt
		if err != nil {
			c.Exit(err.Error())
		}
		ids = append(ids, strconv.FormatInt(ed.InternalID, 10))
		data = append(data, ed)
	}

	if len(ids) == 0 {
		c.Commit()
		e.Log(benchmark.LogTrace, "no new events found, exiting")

		return false
	}

	e.Log(benchmark.LogTrace, fmt.Sprintf("%d new events found", len(ids)))

	/*
	 * step #2 - delete events from original table `acronis_db_bench_eventbus_events`
	 */

	c.ExecOrExit(fmt.Sprintf("DELETE FROM acronis_db_bench_eventbus_events WHERE internal_id IN (%s);", strings.Join(ids, ",")))

	/*
	 * step #3 - allocate events sequence
	 */

	var seq64 int64
	var err error

	switch c.DbOpts.Driver {
	case benchmark.MSSQL:
		sequence := c.QueryAndReturnString("SELECT sequence + 1 FROM acronis_db_bench_eventbus_sequences WITH (UPDLOCK) WHERE int_id = $1;", 1)
		seq64, err = strconv.ParseInt(sequence, 10, 64)
		if err != nil {
			c.Exit(err.Error())
		}
		c.ExecOrExit("UPDATE acronis_db_bench_eventbus_sequences SET sequence = $1 - 1 WHERE int_id = $2;", seq64+int64(len(ids)), 1)

	default:
		sequence := c.QueryAndReturnString("SELECT sequence + 1 FROM acronis_db_bench_eventbus_sequences WHERE int_id = $1 FOR UPDATE;", 1)
		seq64, err = strconv.ParseInt(sequence, 10, 64)
		if err != nil {
			c.Exit(err.Error())
		}
		c.ExecOrExit("UPDATE acronis_db_bench_eventbus_sequences SET sequence = $1 - 1 WHERE int_id = $2;", seq64+int64(len(ids)), 1)
	}

	/*
	 * step #4 - copy data to `acronis_db_bench_eventbus_data`
	 */

	fields := 4
	placeholders := make([]string, len(data))
	values := make([]interface{}, fields*len(data))

	for n := range data {
		placeholders[n] = fmt.Sprintf("(%s)", benchmark.GenDBParameterPlaceholders(n*fields, fields))
		values[n*fields+0] = seq64 + int64(n)            // intId: global sequence
		values[n*fields+1] = data[n].TopicInternalID     // topic_id
		values[n*fields+2] = data[n].EventTypeInternalID // event type_id
		values[n*fields+3] = data[n].Data                // event data
	}
	c.ExecOrExit(fmt.Sprintf("INSERT INTO acronis_db_bench_eventbus_data (int_id, topic_id, type_id, data) VALUES%s;", strings.Join(placeholders, ",")), values...)

	/*
	 * step #4 - create meta data in `acronis_db_bench_eventbus_data`
	 */

	fields = 3
	values = make([]interface{}, fields*len(data))

	for n := range data {
		if c.DbOpts.Driver == benchmark.MSSQL {
			placeholders[n] = fmt.Sprintf("(%s, GETDATE())", benchmark.GenDBParameterPlaceholders(n*fields, fields))
		} else {
			placeholders[n] = fmt.Sprintf("(%s, NOW())", benchmark.GenDBParameterPlaceholders(n*fields, fields))
		}
		values[n*fields+0] = seq64 + int64(n)        // int_id: global sequence
		values[n*fields+1] = data[n].TopicInternalID // topic_id
		values[n*fields+2] = seq64 + int64(n)        // seq: per-topic sequence, currently equals to int_id
	}
	c.ExecOrExit(fmt.Sprintf("INSERT INTO acronis_db_bench_eventbus_stream (int_id, topic_id, seq, seq_time) VALUES%s;", strings.Join(placeholders, ",")), values...)

	c.Commit()

	return true
}

// DoMaxSeqShifter simulates events max sequence shift
func (e *EventBus) DoMaxSeqShifter() bool {
	c := e.workerConn

	for t := 1; t < MaxTopics+1; t++ {
		c.Begin()

		var sequence string

		switch c.DbOpts.Driver {
		case benchmark.MSSQL:
			sequence = c.QueryAndReturnString("SELECT TOP(1) seq FROM acronis_db_bench_eventbus_stream WHERE topic_id = $1 AND seq IS NOT NULL ORDER BY seq DESC;", t)
		default:
			sequence = c.QueryAndReturnString("SELECT seq FROM acronis_db_bench_eventbus_stream WHERE topic_id = $1 AND seq IS NOT NULL ORDER BY seq DESC LIMIT 1;", t)
		}
		seq64, _ := strconv.ParseInt(sequence, 10, 64)
		c.ExecOrExit("UPDATE acronis_db_bench_eventbus_topics SET max_seq = $1, acked_cursor = $2 WHERE internal_id = $3 AND max_seq < $4", seq64, seq64, t, seq64)

		c.Commit()
	}

	return true
}

// DoFetch simulates events sending
func (e *EventBus) DoFetch() bool {
	c := e.workerConn

	for t := 1; t < MaxTopics+1; t++ {
		cursor := c.QueryAndReturnString("SELECT sent_cursor FROM acronis_db_bench_eventbus_topics WHERE internal_id = $1", t)
		cur64, _ := strconv.ParseInt(cursor, 10, 64)

		rows := c.Select("acronis_db_bench_eventbus_stream s INNER JOIN acronis_db_bench_eventbus_data d ON s.int_id = d.int_id",
			"s.int_id, s.topic_id, d.type_id, s.seq, s.seq_time, d.data",
			"s.topic_id = $1 AND s.seq IS NOT NULL AND s.seq > $2",
			"s.seq",
			e.batchSize,
			false,
			t, cur64)

		var sentCursor int64
		var unused interface{}

		for rows.Next() {
			err := rows.Scan(&unused,
				&unused,
				&unused,
				&sentCursor,
				&unused,
				&unused)
			if err != nil {
				c.Exit(err.Error())
			}
		}

		c.ExecOrExit("UPDATE acronis_db_bench_eventbus_topics SET sent_cursor = $1 WHERE internal_id = $2", sentCursor, t)
	}

	return true
}

// DoWindowShift simulates events window shift
func (e *EventBus) DoWindowShift() bool {
	return true
}

// DoConsolidate simulates events consolidation
func (e *EventBus) DoConsolidate() bool {
	return true
}

// DoFetchConsolidated simulates events sending
func (e *EventBus) DoFetchConsolidated() bool {
	return true
}

// DoArchive simulates events archiving
func (e *EventBus) DoArchive() bool {
	c := e.workerConn

	for t := 1; t < MaxTopics+1; t++ {
		c.Begin()

		cursor := c.QueryAndReturnString("SELECT acked_cursor FROM acronis_db_bench_eventbus_topics WHERE internal_id = $1", t)
		cur64, _ := strconv.ParseInt(cursor, 10, 64)

		switch c.DbOpts.Driver {
		case benchmark.MSSQL:
			c.ExecOrExit(fmt.Sprintf("INSERT INTO acronis_db_bench_eventbus_archive (int_id, topic_id, seq, seq_time) SELECT TOP %d int_id, topic_id, seq, seq_time "+
				"FROM acronis_db_bench_eventbus_stream WHERE topic_id = %d AND seq IS NOT NULL AND seq <= %d ORDER BY seq ;",
				e.batchSize, t, cur64))
		default:
			c.ExecOrExit("INSERT INTO acronis_db_bench_eventbus_archive (int_id, topic_id, seq, seq_time) SELECT int_id, topic_id, seq, seq_time "+
				"FROM acronis_db_bench_eventbus_stream WHERE topic_id = $1 AND seq IS NOT NULL AND seq <= $2 ORDER BY seq LIMIT $3;",
				t, cur64, e.batchSize)
		}

		switch c.DbOpts.Driver {
		case benchmark.MYSQL:
			c.ExecOrExit("DELETE FROM acronis_db_bench_eventbus_stream WHERE topic_id = $1 AND seq <= $2 ORDER BY seq ASC LIMIT $3;", t, cur64, e.batchSize)
		case benchmark.MSSQL:
			c.ExecOrExit(fmt.Sprintf(`DELETE x
						FROM acronis_db_bench_eventbus_stream x
						INNER JOIN (
						    SELECT TOP %d int_id
						    FROM acronis_db_bench_eventbus_stream
						    WHERE topic_id = %d AND seq <= %d
						    ORDER BY seq ASC
						) y ON x.int_id = y.int_id;`, e.batchSize, t, cur64))
		default:
			c.ExecOrExit("DELETE FROM acronis_db_bench_eventbus_stream WHERE (topic_id, seq) IN ("+
				"SELECT topic_id, seq "+
				"FROM acronis_db_bench_eventbus_stream "+
				"WHERE topic_id = $1 AND seq <= $2 "+
				"ORDER BY seq ASC "+
				"LIMIT $3"+
				");",
				t, cur64, e.batchSize)
		}

		c.Commit()
	}

	return true
}

// DoDelete simulates events deletion
func (e *EventBus) DoDelete() bool {
	return true
}
