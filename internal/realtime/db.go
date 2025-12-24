// db.go
package realtime

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// collectionNameSanitizer ensures collection names are safe for use in dynamic SQL queries.
// It prevents SQL injection by allowing only a restricted, known-safe character set.
var collectionNameSanitizer = regexp.MustCompile("^[a-z0-9_]+$")

// identifierSanitizer is used for user-provided index and field names.
var identifierSanitizer = regexp.MustCompile("^[a-zA-Z0-9_]+$")

// InitDB initializes the SQLite database connection, creates the core schema, and sets
// essential PRAGMA settings for performance and concurrency.
// It returns a ready-to-use database connection pool.
func InitDB(filepath string) *sql.DB {
	// Construction of the DSN (Data Source Name) with performance optimizations.
	// 1. _journal_mode=WAL: Enables Write-Ahead Logging. Readers don't block writers.
	// 2. _busy_timeout=5000: Waits 5s for a lock before failing. Essential for concurrency.
	// 3. _synchronous=NORMAL: In WAL mode, this is safe and significantly faster than FULL.
	// 4. _cache_size=-64000: Uses ~64MB of RAM for page cache (negative = kilobytes).
	// 5. _foreign_keys=on: Enforces integrity.
	dsn := fmt.Sprintf(
		"%s?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL&_cache_size=-64000&_foreign_keys=on",
		filepath,
	)

	// Open the database using the configured DSN.
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// === Connection Pool Tuning ===
	// In WAL mode, SQLite can handle multiple simultaneous readers.
	// We set a limit to prevent OS resource exhaustion, but high enough to handle
	// the Studio dashboard and multiple WebSocket subscribers reading data.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connection immediately
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Try to set mmap_size for zero-copy access (Performance boost for reads)
	// This is done via Exec because it's not always available as a URI parameter.
	if _, err := dbExec(db, "PRAGMA mmap_size=268435456;"); err != nil { // 256MB
		log.Printf("Warning: Failed to set mmap_size: %v", err)
	}

	// === Schema Initialization ===

	// This table is a simple key-value store for persisting the application's state,
	// making the event processor durable and crash-safe.
	stateSchema := `
    CREATE TABLE IF NOT EXISTS system_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    -- Ensure the processor's starting point exists.
    INSERT OR IGNORE INTO system_state (key, value) VALUES ('last_processed_changelog_id', '0');
    `

	// The changelog table is the central, append-only log of all mutations.
	// It's the immutable source of truth for the real-time eventing system.
	changelogSchema := `
    CREATE TABLE IF NOT EXISTS changelog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
        operation TEXT NOT NULL,
        collection_name TEXT NOT NULL,
        document_id TEXT NOT NULL,
        new_data JSON,
    	old_data JSON
    );
    -- Optional: Create an index on ID if the table grows massive,
    -- though INTEGER PRIMARY KEY is already indexed by rowid.
    `

	// Execute Schema creation in a transaction for safety
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin schema transaction: %v", err)
	}

	if _, err := tx.Exec(stateSchema); err != nil {
		tx.Rollback()
		log.Fatalf("Failed to create system_state schema: %v", err)
	}
	if _, err := tx.Exec(changelogSchema); err != nil {
		tx.Rollback()
		log.Fatalf("Failed to create changelog schema: %v", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit schema transaction: %v", err)
	}

	log.Println("Database initialized with High-Performance settings.")
	return db
}

// CreateCollection dynamically creates a new table for a collection and its corresponding triggers.
// The triggers automatically populate the changelog table on any CUD (Create, Update, Delete) operation.
func CreateCollection(db *sql.DB, name string) error {
	if !collectionNameSanitizer.MatchString(name) {
		return fmt.Errorf("invalid collection name: %s", name)
	}

	// Using fmt.Sprintf is safe here ONLY because we have sanitized the 'name' with a regex.
	tableSchema := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY, data JSON NOT NULL);`, name)
	triggers := fmt.Sprintf(`
    -- For INSERT, old_data is NULL.
    CREATE TRIGGER IF NOT EXISTS %[1]s_insert_trigger AFTER INSERT ON %[1]s
    BEGIN INSERT INTO changelog (operation, collection_name, document_id, new_data, old_data) VALUES ('INSERT', '%[1]s', NEW.id, NEW.data, NULL); END;

    -- For UPDATE, we now capture both NEW.data and OLD.data. This is the key change.
    CREATE TRIGGER IF NOT EXISTS %[1]s_update_trigger AFTER UPDATE ON %[1]s
    BEGIN INSERT INTO changelog (operation, collection_name, document_id, new_data, old_data) VALUES ('UPDATE', '%[1]s', NEW.id, NEW.data, OLD.data); END;

    -- For DELETE, new_data is NULL, old_data has the final state.
    CREATE TRIGGER IF NOT EXISTS %[1]s_delete_trigger AFTER DELETE ON %[1]s
    BEGIN INSERT INTO changelog (operation, collection_name, document_id, new_data, old_data) VALUES ('DELETE', '%[1]s', OLD.id, NULL, OLD.data); END;
    `, name)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	txID := fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err := execTxQuery(tx, txID, tableSchema); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create table for collection %s: %w", name, err)
	}
	if _, err := execTxQuery(tx, txID, triggers); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create triggers for collection %s: %w", name, err)
	}
	log.Printf("Successfully created collection '%s'", name)
	return tx.Commit()
}

// DeleteCollection drops a collection's table. SQLite automatically drops associated triggers.
func DeleteCollection(db *sql.DB, name string) error {
	if !collectionNameSanitizer.MatchString(name) {
		return fmt.Errorf("invalid collection name: %s", name)
	}
	_, err := dbExec(db, fmt.Sprintf("DROP TABLE IF EXISTS %s;", name))
	if err != nil {
		return fmt.Errorf("failed to delete collection %s: %w", name, err)
	}
	log.Printf("Successfully deleted collection '%s'", name)
	return nil
}

// CreateIndex dynamically creates an index on a collection's JSON data column.
// It supports composite indexes (multiple fields) and uniqueness constraints.
func CreateIndex(db *sql.DB, collectionName string, indexName string, fields []string, unique bool) error {
	if !collectionNameSanitizer.MatchString(collectionName) {
		return fmt.Errorf("invalid collection name: %s", collectionName)
	}
	if !identifierSanitizer.MatchString(indexName) {
		return fmt.Errorf("invalid index name: %s", indexName)
	}
	if len(fields) == 0 {
		return fmt.Errorf("at least one field is required to create an index")
	}

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("CREATE ")
	if unique {
		sqlBuilder.WriteString("UNIQUE ")
	}
	sqlBuilder.WriteString(fmt.Sprintf("INDEX IF NOT EXISTS %s ON %s (", indexName, collectionName))

	expressions := make([]string, len(fields))
	for i, field := range fields {
		if !identifierSanitizer.MatchString(field) {
			return fmt.Errorf("invalid field name for index: %s", field)
		}
		// This creates an index on the value extracted from the JSON blob.
		expressions[i] = fmt.Sprintf("json_extract(data, '$.%s')", field)
	}
	sqlBuilder.WriteString(strings.Join(expressions, ", "))
	sqlBuilder.WriteString(");")

	finalSQL := sqlBuilder.String()
	log.Printf("Executing Create Index statement: %s", finalSQL)

	_, err := dbExec(db, finalSQL)
	if err != nil {
		return fmt.Errorf("failed to execute create index statement: %w", err)
	}

	log.Printf("Successfully created index '%s' on collection '%s'", indexName, collectionName)
	return nil
}

// RunChangelogJanitor starts a background goroutine to periodically clean
// the changelog table to prevent it from growing indefinitely.
func RunChangelogJanitor(db *sql.DB, retentionPeriod time.Duration, cleanupInterval time.Duration) {
	log.Printf("Changelog janitor started. Retention: %v, Interval: %v", retentionPeriod, cleanupInterval)
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			cutoffTime := time.Now().UTC().Add(-retentionPeriod)
			cutoffString := cutoffTime.Format("2006-01-02 15:04:05")
			result, err := dbExec(db, `DELETE FROM changelog WHERE timestamp < ?`, cutoffString)
			if err == nil {
				rowsAffected, _ := result.RowsAffected()
				if rowsAffected > 0 {
					log.Printf("Janitor: Cleaned up %d old changelog entries.", rowsAffected)
				}
			}
		}
	}()
}
