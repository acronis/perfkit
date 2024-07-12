package sql

import (
	"database/sql"

	"github.com/acronis/perfkit/db"
)

// DBPollEng is a virtual interface providing a mechanism to track and deliver database object
/* changes to external services which can be used in production services as simple MQ replacement
 *
 * Methods:
 * - InitCursor - creates all the needed meta-tables, must be called in the beginning of test
 * - CleanupCursor - removes all the polling-engine specific data
 * - InsertObjCursor - must be created when a pollable object is created in 'obj_table_name' table
 * - UpdateObjCursorByUUID - must be called when a pollable object is updated and object UUID is known in the main transaction
 * - UpdateObjCursorByID - must be called when a pollable object is updated and object ID is known in the main transaction
 */
type DBPollEng interface {
	InitCursor(opts db.Config, objTableName string)
	CleanupCursor(opts db.Config, objTableName string)

	InsertObjCursor(worker int, tx *sql.Tx, objTableName, objUUID, tranID string, objectTypeID int)
	UpdateObjCursorByUUID(worker int, tx *sql.Tx, objTableName, objUUID, tranID string, objectTypeID int)
	UpdateObjCursorByID(worker int, tx *sql.Tx, objTableName, tranID string, objectTypeID, id int)
}
