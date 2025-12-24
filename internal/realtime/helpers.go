// helpers.go
package realtime

import (
	"database/sql"
	"log"
	"strings"

	json "github.com/goccy/go-json"
)

// formatArgsForLogging inspects each argument and converts byte slices to strings for readability.
func formatArgsForLogging(args ...any) []any {
	// Create a new slice to hold the formatted arguments.
	formattedArgs := make([]any, len(args))

	for i, arg := range args {
		// Use a type switch to check the type of each argument.
		switch v := arg.(type) {
		case []byte:
			// If it's a byte slice, convert it to a string.
			formattedArgs[i] = string(v)
		case json.RawMessage:
			// Also handle json.RawMessage explicitly.
			formattedArgs[i] = string(v)
		default:
			// For all other types, keep the original value.
			formattedArgs[i] = v
		}
	}
	return formattedArgs
}

func logQuery(query string, args ...any) {
	if !IsSqlLoggingEnabled {
		return
	}
	// Sanitize the query for better multi-line logging and remove extra whitespace.
	sanitizedQuery := strings.Join(strings.Fields(query), " ")
	log.Printf("[SQL]: \"%s\" | Args: %v", sanitizedQuery, formatArgsForLogging(args...))
}

// dbExec wraps db.Exec and adds logging.
func dbExec(db *sql.DB, query string, args ...any) (sql.Result, error) {
	logQuery(query, args...)
	return db.Exec(query, args...)
}

// dbQuery wraps db.Query and adds logging.
func dbQuery(db *sql.DB, query string, args ...any) (*sql.Rows, error) {
	logQuery(query, args...)
	return db.Query(query, args...)
}

// dbQueryRow wraps db.QueryRow and adds logging.
func dbQueryRow(db *sql.DB, query string, args ...any) *sql.Row {
	logQuery(query, args...)
	return db.QueryRow(query, args...)
}

func logTxQuery(txID string, query string, args ...any) {
	if !IsSqlLoggingEnabled {
		return
	}
	sanitizedQuery := strings.Join(strings.Fields(query), " ")
	log.Printf("[SQL-TX %s]: \"%s\" | Args: %v", txID, sanitizedQuery, formatArgsForLogging(args...))
}

// execTxQuery wraps tx.Exec and adds logging.
// It takes a transaction ID for clearer log messages.
func execTxQuery(tx *sql.Tx, txID string, query string, args ...any) (sql.Result, error) {
	logTxQuery(txID, query, args...)
	return tx.Exec(query, args...)
}
