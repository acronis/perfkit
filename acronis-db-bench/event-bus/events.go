package event_bus

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	guuid "github.com/google/uuid"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"
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
	workerConn      db.Database
	workerStarted   bool
	stopCh          chan bool
	wg              sync.WaitGroup
	batchSize       int
	sleepMsec       int
	workerIteration uint64
	logger          logger.Logger
}

// NewEventBus creates a new event bus worker instance
func NewEventBus(conn db.Database, logger logger.Logger) *EventBus {
	return &EventBus{
		workerConn:    conn,
		workerStarted: false,
		stopCh:        make(chan bool, 1),
		batchSize:     500,
		sleepMsec:     10,
		logger:        logger,
	}
}

// Log is a helper function to log event bus messages
func (e *EventBus) Log(LogLevel logger.LogLevel, format string, args ...interface{}) {
	msg := "eventbus: " + fmt.Sprintf(format, args...)
	e.logger.Log(LogLevel, msg)
}

// MainLoop is the main worker loop for the event bus
func (e *EventBus) MainLoop() {
	defer e.wg.Done()

	for {
		select {
		case <-e.stopCh:
			e.logger.Info("stopping main worker loop")
			return
		default:
			if empty, err := e.QueueIsEmpty(); err != nil {
				e.logger.Error("cannot check if queue is empty: %v", err)
				return
			} else if empty {
				time.Sleep(time.Duration(e.sleepMsec) * time.Millisecond)
				continue
			}

			time.Sleep(time.Duration(e.sleepMsec) * time.Millisecond)
			e.Work()
		}
	}
}

// QueueIsEmpty returns true if the event bus queue is empty
func (e *EventBus) QueueIsEmpty() (bool, error) {
	c := e.workerConn
	session := c.Session(c.Context(context.Background(), false))

	var rowNum uint64
	if err := session.QueryRow("SELECT COUNT(*) FROM acronis_db_bench_eventbus_events;").Scan(&rowNum); err != nil {
		return false, fmt.Errorf("eventbus: cannot get rows count in table '%s': %v", "acronis_db_bench_eventbus_events", err)
	}

	return rowNum == 0, nil
}

// Start starts the event bus worker
func (e *EventBus) Start() error {
	var dialectName = e.workerConn.DialectName()

	if dialectName == db.CLICKHOUSE {
		return fmt.Errorf("event bus is not supported for ClickHouse DB")
	}
	if dialectName == db.CASSANDRA {
		return fmt.Errorf("event bus is not supported for Cassandra DB")
	}
	if e.workerStarted {
		return nil
	}
	e.workerStarted = true

	e.logger.Debug("worker start")

	e.wg.Add(1)
	go e.MainLoop()

	return nil
}

// Stop stops the event bus worker
func (e *EventBus) Stop() {
	if !e.workerStarted {
		return
	}
	e.stopCh <- true
	e.workerStarted = false
	e.wg.Wait()
	e.logger.Debug("worker stop")
}

// CreateTables creates all the tables required for the event bus
func (e *EventBus) CreateTables() error {
	c := e.workerConn

	if exists, err := c.TableExists("acronis_db_bench_eventbus_events"); err != nil {
		return fmt.Errorf("eventbus: cannot check if table '%s' exists: %v", "acronis_db_bench_eventbus_events", err)
	} else if exists {
		return nil
	}

	if c.DialectName() == db.CLICKHOUSE || c.DialectName() == db.CASSANDRA {
		return nil
	}

	if err := c.ApplyMigrations("", EventBusDDL); err != nil {
		return err
	}

	var session = c.Session(c.Context(context.Background(), false))
	if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
		for i := 1; i < MaxTopics+1; i++ {
			var eventTopic = EventTopic{
				InternalID: i,
				TopicID:    fmt.Sprintf("cti.a.p.em.topic.v1.0~a.p.my_topic.%d.v1.0", i),
			}

			if err := tx.BulkInsert("acronis_db_bench_eventbus_topics", [][]interface{}{
				{eventTopic.InternalID, eventTopic.TopicID},
			}, []string{"internal_id", "topic_id"}); err != nil {
				return err
			}

			var eventType = EventType{
				InternalID:      i,
				TopicInternalID: i,
				EventType:       fmt.Sprintf("cti.a.p.em.event.v1.0~a.p.my_event.%d.v1.0", i),
			}

			if err := tx.BulkInsert("acronis_db_bench_eventbus_event_types", [][]interface{}{
				{eventType.InternalID, eventType.TopicInternalID, eventType.EventType},
			}, []string{"internal_id", "topic_internal_id", "event_type"}); err != nil {
				return err
			}
		}

		if _, err := tx.Exec("INSERT INTO acronis_db_bench_eventbus_sequences (int_id, sequence) VALUES (1, 0)"); err != nil {
			return err
		}

		return nil
	}); txErr != nil {
		return fmt.Errorf("eventbus: cannot create tables: %v", txErr)
	}

	e.logger.Debug("created EventBus tables and indexes")

	return nil
}

// DropTables drops all the tables created by CreateTables()
func (e *EventBus) DropTables() error {
	c := e.workerConn
	var constraints []db.Constraint

	if c.UseTruncate() {
		var err error
		if constraints, err = c.ReadConstraints(); err != nil {
			return fmt.Errorf("db: cannot read constraints: %v", err)
		}

		if err = c.DropConstraints(constraints); err != nil {
			return fmt.Errorf("eventbus: cannot drop constraints: %v", err)
		}
	}

	for _, table := range []string{
		"acronis_db_bench_eventbus_consolidated",
		"acronis_db_bench_eventbus_archive",
		"acronis_db_bench_eventbus_distrlocks",
		"acronis_db_bench_eventbus_migrations",
		"acronis_db_bench_eventbus_sequences",
		"acronis_db_bench_eventbus_stream",
		"acronis_db_bench_eventbus_events",
		"acronis_db_bench_eventbus_initial_seeding_cursors",
		"acronis_db_bench_eventbus_data",
		"acronis_db_bench_eventbus_event_types",
		"acronis_db_bench_eventbus_topics",
	} {
		if err := c.DropTable(table); err != nil {
			return fmt.Errorf("eventbus: cannot drop table '%s': %v", table, err)
		}
	}

	if c.UseTruncate() {
		if err := c.AddConstraints(constraints); err != nil {
			return fmt.Errorf("eventbus: cannot add constraints: %v", err)
		}
	}

	return nil
}

// InsertEvent inserts a single event into the event bus
func (e *EventBus) InsertEvent(rz *benchmark.Randomizer, databaseAccessor db.DatabaseAccessor, subjectUUID string) error {
	topicID := 1 + rz.Intn(MaxTopics)
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

	return databaseAccessor.BulkInsert("acronis_db_bench_eventbus_events", [][]interface{}{
		{d.TopicInternalID, d.EventTypeInternalID, d.EventID, d.Source, d.TenantID, d.ConsolidationKey, d.Data},
	},
		[]string{"topic_internal_id", "event_type_internal_id", "event_id", "source", "tenant_id", "consolidation_key", "data"})
}

/*
 * Main worker
 */

type stepType func() //nolint:unused

// Work performs a single iteration of the worker
func (e *EventBus) Work() {
	e.workerIteration++
	e.logger.Trace(fmt.Sprintf("worker iteration #%d start", e.workerIteration))

	// Phase 1: Align events
	if aligned := e.Step("phase #1 (aligner)", e.DoAlign); aligned {
		e.logger.Debug("Events aligned, proceeding with processing")

		// Phase 2: Update max sequence
		if shifted := e.Step("phase #2 (max seq shifter)", e.DoMaxSeqShifter); shifted {
			e.logger.Debug("Max sequence shifted")
		} else {
			e.logger.Debug("Max sequence shift failed or no events to process")
		}

		// Phase 3: Fetch and process events
		if fetched := e.Step("phase #3 (fetcher)", e.DoFetch); fetched {
			e.logger.Debug("Events fetched and processed")
		} else {
			e.logger.Debug("No events fetched or processing failed")
		}

		// Phase 4: Archive processed events
		if archived := e.Step("phase #4 (archive)", e.DoArchive); archived {
			e.logger.Debug("Events archived")
		} else {
			e.logger.Debug("Archiving failed or no events to archive")
		}
	} else {
		e.logger.Debug("No events to align or alignment failed")
	}

	e.logger.Trace(fmt.Sprintf("worker iteration #%d end", e.workerIteration))
}

// Step is a helper function to log step start/end
func (e *EventBus) Step(msg string, dofunc func() (bool, error)) bool {
	e.logger.Trace(msg + " start")
	ret, err := dofunc()
	if err != nil {
		e.logger.Error(fmt.Sprintf("%s: %v", msg, err))
	}
	e.logger.Trace(msg + " end")

	return ret
}

// DoAlign simulates events alignment
func (e *EventBus) DoAlign() (bool, error) {
	c := e.workerConn

	var data []*EventData
	var ids []string

	var unused interface{}
	var newEventsFound bool

	var session = c.Session(c.Context(context.Background(), false))
	if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
		e.logger.Debug("Starting DoAlign transaction")

		/*
		 * step #1 - get fresh events
		 */

		var topOrEmpty, limitOrEmpty string
		if c.DialectName() == db.MSSQL {
			topOrEmpty = fmt.Sprintf("TOP %d", e.batchSize)
			limitOrEmpty = ""
		} else {
			topOrEmpty = ""
			limitOrEmpty = fmt.Sprintf("LIMIT %d", e.batchSize)
		}

		var rows, err = tx.Query(fmt.Sprintf(`
			SELECT %s internal_id,
				   topic_internal_id,
				   event_type_internal_id,
				   event_id,
				   source,
				   sequence,
				   tenant_id,
				   client_id,
				   trace_parent,
				   subject_id,
				   data_ref,
				   data,
				   data_base64,
				   created_at
			FROM acronis_db_bench_eventbus_events
			ORDER BY internal_id
			%s;`,
			topOrEmpty,
			limitOrEmpty))
		if err != nil {
			e.logger.Error("Failed to query events: %v", err)
			return err
		}

		for rows.Next() {
			var ed = &EventData{}
			err = rows.Scan(&ed.InternalID,
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
				e.logger.Error("Failed to scan event: %v", err)
				return fmt.Errorf("scan failed: %v", err)
			}

			ids = append(ids, strconv.FormatInt(ed.InternalID, 10))
			data = append(data, ed)
		}

		if len(ids) == 0 {
			e.logger.Trace("no new events found, exiting")
			newEventsFound = false

			return nil
		}

		e.logger.Debug("Found %d new events", len(ids))
		newEventsFound = true

		/*
		 * step #2 - delete events from original table `acronis_db_bench_eventbus_events`
		 */

		if _, err = tx.Exec(fmt.Sprintf("DELETE FROM acronis_db_bench_eventbus_events WHERE internal_id IN (%s);", strings.Join(ids, ","))); err != nil {
			e.logger.Error("Failed to delete events: %v", err)
			return err
		}

		/*
		 * step #3 - allocate events sequence
		 */

		var seq64 int64

		switch c.DialectName() {
		case db.MSSQL:
			if err = tx.QueryRow("SELECT sequence + 1 FROM acronis_db_bench_eventbus_sequences WITH (UPDLOCK) WHERE int_id = @p1;", 1).Scan(&seq64); err != nil {
				e.logger.Error("Failed to get sequence (MSSQL): %v", err)
				return err
			}

			if _, err = tx.Exec("UPDATE acronis_db_bench_eventbus_sequences SET sequence = @p1 - 1 WHERE int_id = @p2;", seq64+int64(len(ids)), 1); err != nil {
				e.logger.Error("Failed to update sequence (MSSQL): %v", err)
				return err
			}

		case db.MYSQL:
			if err = tx.QueryRow("SELECT sequence + 1 FROM acronis_db_bench_eventbus_sequences WHERE int_id = ? FOR UPDATE;", 1).Scan(&seq64); err != nil {
				e.logger.Error("Failed to get sequence (MySQL): %v", err)
				return err
			}

			if _, err = tx.Exec("UPDATE acronis_db_bench_eventbus_sequences SET sequence = ? - 1 WHERE int_id = ?;", seq64+int64(len(ids)), 1); err != nil {
				e.logger.Error("Failed to update sequence (MySQL): %v", err)
				return err
			}

		case db.SQLITE:
			if err = tx.QueryRow("SELECT sequence + 1 FROM acronis_db_bench_eventbus_sequences WHERE int_id = $1;", 1).Scan(&seq64); err != nil {
				e.logger.Error("Failed to get sequence (SQLite): %v", err)
				return err
			}

			if _, err = tx.Exec("UPDATE acronis_db_bench_eventbus_sequences SET sequence = $1 - 1 WHERE int_id = $2;", seq64+int64(len(ids)), 1); err != nil {
				e.logger.Error("Failed to update sequence (SQLite): %v", err)
				return err
			}

		default:
			if err = tx.QueryRow("SELECT sequence + 1 FROM acronis_db_bench_eventbus_sequences WHERE int_id = $1 FOR UPDATE;", 1).Scan(&seq64); err != nil {
				e.logger.Error("Failed to get sequence (default): %v", err)
				return err
			}

			if _, err = tx.Exec("UPDATE acronis_db_bench_eventbus_sequences SET sequence = $1 - 1 WHERE int_id = $2;", seq64+int64(len(ids)), 1); err != nil {
				e.logger.Error("Failed to update sequence (default): %v", err)
				return err
			}
		}

		/*
		 * step #4 - copy data to `acronis_db_bench_eventbus_data`
		 */

		fields := 4
		placeholders := make([]string, len(data))
		values := make([]interface{}, fields*len(data))

		for n := range data {
			var rawPlaceholders string
			start := n * fields
			switch c.DialectName() {
			case db.MSSQL:
				rawPlaceholders = fmt.Sprintf("(@p%d,@p%d,@p%d,@p%d)", start+1, start+2, start+3, start+4)
			case db.MYSQL:
				rawPlaceholders = fmt.Sprintf("(?,?,?,?)")
			default:
				rawPlaceholders = fmt.Sprintf("($%d,$%d,$%d,$%d)", start+1, start+2, start+3, start+4)
			}

			placeholders[n] = rawPlaceholders
			values[n*fields+0] = seq64 + int64(n)            // intId: global sequence
			values[n*fields+1] = data[n].TopicInternalID     // topic_id
			values[n*fields+2] = data[n].EventTypeInternalID // event type_id
			values[n*fields+3] = data[n].Data                // event data
		}

		if _, err = tx.Exec(fmt.Sprintf("INSERT INTO acronis_db_bench_eventbus_data (int_id, topic_id, type_id, data) VALUES %s;", strings.Join(placeholders, ",")), values...); err != nil {
			e.logger.Error("Failed to insert into eventbus_data: %v", err)
			return err
		}

		/*
		 * step #5 - create meta data in `acronis_db_bench_stream`
		 */

		fields = 3
		values = make([]interface{}, fields*len(data))

		for n := range data {
			var rawPlaceholders string
			start := n * fields

			switch c.DialectName() {
			case db.MSSQL:
				rawPlaceholders = fmt.Sprintf("(@p%d,@p%d,@p%d, GETDATE())", start+1, start+2, start+3)
			case db.POSTGRES:
				rawPlaceholders = fmt.Sprintf("($%d,$%d,$%d, NOW())", start+1, start+2, start+3)
			case db.SQLITE:
				rawPlaceholders = fmt.Sprintf("($%d,$%d,$%d, datetime('now'))", start+1, start+2, start+3)
			case db.MYSQL:
				rawPlaceholders = fmt.Sprintf("(?,?,?,NOW())")
			}

			placeholders[n] = rawPlaceholders
			values[n*fields+0] = seq64 + int64(n)        // int_id: global sequence
			values[n*fields+1] = data[n].TopicInternalID // topic_id
			values[n*fields+2] = seq64 + int64(n)        // seq: per-topic sequence, currently equals to int_id
		}

		if _, err = tx.Exec(fmt.Sprintf("INSERT INTO acronis_db_bench_eventbus_stream (int_id, topic_id, seq, seq_time) VALUES %s;", strings.Join(placeholders, ",")), values...); err != nil {
			e.logger.Error("Failed to insert into eventbus_stream: %v", err)
			return err
		}

		e.logger.Debug("Successfully completed DoAlign transaction")
		return nil
	}); txErr != nil {
		return false, fmt.Errorf("align: %v", txErr)
	}

	return newEventsFound, nil
}

// DoMaxSeqShifter simulates events max sequence shift
func (e *EventBus) DoMaxSeqShifter() (bool, error) {
	var c = e.workerConn
	var sess = c.Session(c.Context(context.Background(), false))

	for t := 1; t < MaxTopics+1; t++ {
		if txErr := sess.Transact(func(tx db.DatabaseAccessor) error {
			var seq64 int64
			var err error

			switch c.DialectName() {
			case db.MSSQL:
				err = tx.QueryRow("SELECT TOP(1) seq FROM acronis_db_bench_eventbus_stream WHERE topic_id = $1 AND seq IS NOT NULL ORDER BY seq DESC;", t).Scan(&seq64)
			case db.MYSQL:
				err = tx.QueryRow("SELECT seq FROM acronis_db_bench_eventbus_stream WHERE topic_id = ? AND seq IS NOT NULL ORDER BY seq DESC LIMIT 1;", t).Scan(&seq64)
			default:
				err = tx.QueryRow("SELECT seq FROM acronis_db_bench_eventbus_stream WHERE topic_id = $1 AND seq IS NOT NULL ORDER BY seq DESC LIMIT 1;", t).Scan(&seq64)
			}

			if err != nil && err.Error() != sql.ErrNoRows.Error() {
				return err
			}

			switch c.DialectName() {
			case db.MYSQL:
				_, err = tx.Exec("UPDATE acronis_db_bench_eventbus_topics SET max_seq = ?, acked_cursor = ? WHERE internal_id = ? AND max_seq < ?;", seq64, seq64, t, seq64)
			default:
				_, err = tx.Exec("UPDATE acronis_db_bench_eventbus_topics SET max_seq = $1, acked_cursor = $2 WHERE internal_id = $3 AND max_seq < $4;", seq64, seq64, t, seq64)
			}

			if err != nil {
				return err
			}

			return nil
		}); txErr != nil {
			return false, fmt.Errorf("max seq shifter: %v", txErr)
		}
	}

	return true, nil
}

// DoFetch simulates events sending
func (e *EventBus) DoFetch() (bool, error) {
	var c = e.workerConn
	var sess = c.Session(c.Context(context.Background(), false))

	for t := 1; t < MaxTopics+1; t++ {
		var cur64 int64
		switch c.DialectName() {
		case db.MYSQL:
			if err := sess.QueryRow("SELECT sent_cursor FROM acronis_db_bench_eventbus_topics WHERE internal_id = ?;", t).Scan(&cur64); err != nil {
				return false, err
			}
		default:
			if err := sess.QueryRow("SELECT sent_cursor FROM acronis_db_bench_eventbus_topics WHERE internal_id = $1;", t).Scan(&cur64); err != nil {
				return false, err
			}
		}

		var topOrEmpty, limitOrEmpty string
		if c.DialectName() == db.MSSQL {
			topOrEmpty = fmt.Sprintf("TOP %d", e.batchSize)
			limitOrEmpty = ""
		} else {
			topOrEmpty = ""
			limitOrEmpty = fmt.Sprintf("LIMIT %d", e.batchSize)
		}

		var rows, err = sess.Query(fmt.Sprintf(`
			SELECT %s s.int_id, s.topic_id, d.type_id, s.seq, s.seq_time, d.data
			FROM acronis_db_bench_eventbus_stream s
					 INNER JOIN acronis_db_bench_eventbus_data d ON s.int_id = d.int_id
			WHERE s.topic_id = %d
			  AND s.seq IS NOT NULL
			  AND s.seq > %d
			ORDER BY s.seq
			%s`,
			topOrEmpty,
			t, cur64,
			limitOrEmpty))
		if err != nil {
			return false, err
		}

		var sentCursor int64
		var unused interface{}

		for rows.Next() {
			err = rows.Scan(&unused,
				&unused,
				&unused,
				&sentCursor,
				&unused,
				&unused)
			if err != nil {
				return false, err
			}
		}

		switch c.DialectName() {
		case db.MYSQL:
			_, err = sess.Exec("UPDATE acronis_db_bench_eventbus_topics SET sent_cursor = ? WHERE internal_id = ?;", sentCursor, t)
		default:
			_, err = sess.Exec("UPDATE acronis_db_bench_eventbus_topics SET sent_cursor = $1 WHERE internal_id = $2;", sentCursor, t)
		}

		return false, err
	}

	return true, nil
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
func (e *EventBus) DoArchive() (bool, error) {
	var c = e.workerConn
	var sess = c.Session(c.Context(context.Background(), false))

	for t := 1; t < MaxTopics+1; t++ {
		if txErr := sess.Transact(func(tx db.DatabaseAccessor) error {
			var cur64 int64
			switch c.DialectName() {
			case db.MYSQL:
				if err := tx.QueryRow("SELECT acked_cursor FROM acronis_db_bench_eventbus_topics WHERE internal_id = ?;", t).Scan(&cur64); err != nil {
					return err
				}
			default:
				if err := tx.QueryRow("SELECT acked_cursor FROM acronis_db_bench_eventbus_topics WHERE internal_id = $1;", t).Scan(&cur64); err != nil {
					return err
				}
			}

			var err error
			switch c.DialectName() {
			case db.MSSQL:
				_, err = tx.Exec(fmt.Sprintf("INSERT INTO acronis_db_bench_eventbus_archive (int_id, topic_id, seq, seq_time) SELECT TOP %d int_id, topic_id, seq, seq_time "+
					"FROM acronis_db_bench_eventbus_stream WHERE topic_id = %d AND seq IS NOT NULL AND seq <= %d ORDER BY seq ;",
					e.batchSize, t, cur64))
			case db.MYSQL:
				_, err = tx.Exec("INSERT INTO acronis_db_bench_eventbus_archive (int_id, topic_id, seq, seq_time) SELECT int_id, topic_id, seq, seq_time "+
					"FROM acronis_db_bench_eventbus_stream WHERE topic_id = ? AND seq IS NOT NULL AND seq <= ? ORDER BY seq LIMIT ?;",
					t, cur64, e.batchSize)
			default:
				_, err = tx.Exec("INSERT INTO acronis_db_bench_eventbus_archive (int_id, topic_id, seq, seq_time) SELECT int_id, topic_id, seq, seq_time "+
					"FROM acronis_db_bench_eventbus_stream WHERE topic_id = $1 AND seq IS NOT NULL AND seq <= $2 ORDER BY seq LIMIT $3;",
					t, cur64, e.batchSize)
			}

			switch c.DialectName() {
			case db.MYSQL:
				_, err = tx.Exec("DELETE FROM acronis_db_bench_eventbus_stream WHERE topic_id = ? AND seq <= ? ORDER BY seq ASC LIMIT ?;", t, cur64, e.batchSize)
			case db.MSSQL:
				_, err = tx.Exec(fmt.Sprintf(`DELETE x
						FROM acronis_db_bench_eventbus_stream x
						INNER JOIN (
						    SELECT TOP %d int_id
						    FROM acronis_db_bench_eventbus_stream
						    WHERE topic_id = %d AND seq <= %d
						    ORDER BY seq ASC
						) y ON x.int_id = y.int_id;`, e.batchSize, t, cur64))
			default:
				_, err = tx.Exec("DELETE FROM acronis_db_bench_eventbus_stream WHERE (topic_id, seq) IN ("+
					"SELECT topic_id, seq "+
					"FROM acronis_db_bench_eventbus_stream "+
					"WHERE topic_id = $1 AND seq <= $2 "+
					"ORDER BY seq ASC "+
					"LIMIT $3"+
					");",
					t, cur64, e.batchSize)
			}

			return err
		}); txErr != nil {
			return false, fmt.Errorf("archive: %v", txErr)
		}
	}

	return true, nil
}

// DoDelete simulates events deletion
func (e *EventBus) DoDelete() bool {
	return true
}
