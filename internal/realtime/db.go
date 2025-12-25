// internal/realtime/db.go
package realtime

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/mattn/go-sqlite3" // Specific import required for registering driver hooks
)

// collectionNameSanitizer ensures collection names are safe for use in dynamic SQL.
var collectionNameSanitizer = regexp.MustCompile("^[a-z0-9_]+$")

// identifierSanitizer is used for index/field names.
var identifierSanitizer = regexp.MustCompile("^[a-zA-Z0-9_]+$")

// InitDB initializes the database connection pool using the "Attached Database" pattern.
// It connects to the Main DB and automatically attaches the Log DB to every connection.
func InitDB(mainPath, logPath string) *sql.DB {
	// 1. Register a custom driver wrapper.
	// This 'ConnectHook' executes every time the pool opens a physical connection.
	// We use it to ATTACH the audit database, ensuring consistency across the pool.
	sql.Register("sqlite3_with_logs", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			// Attach the log database under the namespace 'audit'
			_, err := conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS audit;", logPath), nil)
			return err
		},
	})

	// 2. Configure DSN for Maximum Performance (WAL Mode + tuning)
	// _synchronous=NORMAL: Significant write speedup, safe against app crashes.
	// _busy_timeout=5000: Wait 5s for locks instead of failing immediately.
	// _cache_size=-64000: ~64MB Page Cache.
	dsn := fmt.Sprintf(
		"%s?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL&_cache_size=-64000&_foreign_keys=on",
		mainPath,
	)

	// Open using our custom driver
	db, err := sql.Open("sqlite3_with_logs", dsn)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// 3. Connection Pool Tuning
	// Since we are in WAL mode, we can allow multiple readers.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connection and attachment
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database (check if log file path is valid): %v", err)
	}

	// Optimize Memory Map (Zero-copy reads if possible)
	if _, err := dbExec(db, "PRAGMA main.mmap_size=268435456;"); err != nil {
		log.Printf("Warning: Failed to set mmap_size: %v", err)
	}

	// 4. Initialize Schemas

    // A. Audit Schema (Changelog + State)
    // We group these because the State relies on the Changelog IDs.
    auditSchema := `
    CREATE TABLE IF NOT EXISTS audit.changelog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
        operation TEXT NOT NULL,
        collection_name TEXT NOT NULL,
        document_id TEXT NOT NULL,
        new_data JSON,
        old_data JSON
    );

    CREATE TABLE IF NOT EXISTS audit.system_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    -- Initialize cursor if missing
    INSERT OR IGNORE INTO audit.system_state (key, value) VALUES ('last_processed_changelog_id', '0');
    `

    // B. System Metadata (Main DB)
    // Schema definitions belong with the data in Main
    metaSchema := `
    CREATE TABLE IF NOT EXISTS main.system_schema (
        name TEXT PRIMARY KEY,
        schema JSON NOT NULL,
        type TEXT DEFAULT 'base',
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
    );`

	// Run migrations inside a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := tx.Exec(auditSchema); err != nil {
		tx.Rollback()
		log.Fatalf("Failed to init audit schema: %v", err)
	}
	if _, err := tx.Exec(metaSchema); err != nil {
		tx.Rollback()
		log.Fatalf("Failed to init meta schema: %v", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit init transaction: %v", err)
	}

	// Initialize the In-Memory Schema Cache
	if err := LoadSchemaCache(db); err != nil {
		log.Printf("Warning: Initial schema cache load failed: %v", err)
	}

	log.Println("Database initialized: Main + Attached Audit Log.")
	return db
}

// RunChangelogJanitor starts a background goroutine to clean the 'audit' database.
func RunChangelogJanitor(db *sql.DB, retentionPeriod time.Duration, cleanupInterval time.Duration) {
	log.Printf("Changelog janitor started. Retention: %v", retentionPeriod)
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			cutoff := time.Now().UTC().Add(-retentionPeriod).Format("2006-01-02 15:04:05")

			// Target the 'audit' namespace
			result, err := dbExec(db, `DELETE FROM audit.changelog WHERE timestamp < ?`, cutoff)
			if err == nil {
				rowsAffected, _ := result.RowsAffected()
				if rowsAffected > 0 {
					log.Printf("Janitor: Pruned %d old audit entries.", rowsAffected)
				}
			} else {
				log.Printf("Janitor Error: %v", err)
			}
		}
	}()
}
