package realtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// StorageTimeFormat defines the strict ISO8601 format used for storing datetimes.
// We use Milliseconds (.000) and UTC (Z) to ensure that lexicographical sorting (string compare)
// matches chronological sorting.
// Example: "2025-12-25T08:30:00.123Z"
const StorageTimeFormat = "2006-01-02T15:04:05.000Z07:00"

// CreateCollectionRequest defines the payload for creating a new collection.
// It allows defining specific typed columns ("Schema") that are promoted from the JSON blob
// for high-performance indexing and querying.
type CreateCollectionRequest struct {
	Name   string     `json:"name"`   // The name of the collection (table)
	Schema []FieldDef `json:"schema"` // List of typed columns to create
}

// IndexOptions defines the payload for creating an index.
type IndexOptions struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Unique bool     `json:"unique"`
}

// validateAndNormalizeDateTime parses various date input formats and standardizes them
// to a strict UTC ISO8601 format with milliseconds. This ensures data consistency.
func validateAndNormalizeDateTime(input string) (string, error) {
	var t time.Time
	var err error

	// Supported Input Formats
	formats := []string{
		time.RFC3339,              // "2006-01-02T15:04:05Z07:00"
		time.RFC3339Nano,          // "2006-01-02T15:04:05.999999999Z07:00"
		"2006-01-02 15:04:05",     // SQL Standard
		"2006-01-02 15:04:05.999", // SQL with Millis
		"2006-01-02",              // Date only
	}

	for _, f := range formats {
		t, err = time.Parse(f, input)
		if err == nil {
			break
		}
	}

	if err != nil {
		return "", fmt.Errorf("invalid datetime format")
	}

	// Always convert to UTC and standardize format
	return t.UTC().Format(StorageTimeFormat), nil
}

// collectionHandler dispatches requests to specific collection management functions.
// Route: /db/collections
func collectionHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetAllCollections(db, w)
		case http.MethodPost:
			handleCreateCollection(db, w, r)
		case http.MethodDelete:
			handleDeleteCollection(db, w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// handleCreateCollection creates a new physical SQLite table and records its schema metadata.
// It supports "Hybrid Schema" where specific fields are columns, and the rest live in a JSON blob.
func handleCreateCollection(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	var req CreateCollectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", 400)
		return
	}

	// Sanitize table name to prevent SQL Injection
	if !collectionNameSanitizer.MatchString(req.Name) {
		http.Error(w, "Invalid collection name", 400)
		return
	}

	// Validate field types before creating anything
	for _, field := range req.Schema {
		if !field.Type.IsValid() {
			http.Error(w, fmt.Sprintf("Invalid type '%s' for field '%s'. Allowed: text, float, int, bool, datetime", field.Type, field.Name), 400)
			return
		}
	}

	// Serialize schema to JSON for storage in system_schema
	schemaJSON, _ := json.Marshal(req.Schema)

	tx, _ := db.Begin()
	defer tx.Rollback()

	// 1. Build Physical CREATE TABLE SQL
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE main.%s (id TEXT PRIMARY KEY, ", req.Name))

	for _, field := range req.Schema {
		sqlType := "TEXT"
		// Map our logical types to SQLite storage classes
		switch field.Type {
		case TypeFloat:
			sqlType = "REAL"
		case TypeInt, TypeBool:
			sqlType = "INTEGER"
		case TypeText, TypeDateTime:
			sqlType = "TEXT"
		}

		constraints := ""
		if field.Required {
			constraints += " NOT NULL"
		}
		if field.Unique {
			constraints += " UNIQUE"
		}

		sb.WriteString(fmt.Sprintf("%s %s%s, ", field.Name, sqlType, constraints))
	}

	// Add the catch-all 'data' column for unstructured JSON
	sb.WriteString("data TEXT) STRICT;")

	// Execute Create Table
	if _, err := tx.Exec(sb.String()); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// 2. Insert Metadata into system_schema
	// This serves as the Source of Truth for the API/Studio
	_, err := tx.Exec("INSERT INTO main.system_schema (name, schema) VALUES (?, ?)", req.Name, schemaJSON)
	if err != nil {
		http.Error(w, "Failed to save metadata", 500)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "Commit failed", 500)
		return
	}

	// OPTIMIZATION: Update memory directly instead of reloading DB
	SetCollectionSchema(req.Name, req.Schema)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "created", "collection": req.Name})
}

// setDocumentHandler handles PUT requests to fully replace or create a document (Upsert).
// It implements "Application-Side CDC": it writes to the Data Table and the Audit Log
// in a single atomic transaction, avoiding slow SQLite Triggers.
func setDocumentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		docID := r.PathValue("docId")

		// Read body once as bytes (needed for storage and parsing)
		bodyBytes, _ := io.ReadAll(r.Body)
		if !json.Valid(bodyBytes) {
			http.Error(w, "Invalid JSON", 400)
			return
		}

		// Retrieve cached schema to determine which fields map to columns
		fields := GetCollectionFields(collection)

		// --- Build Dynamic SQL ---
		// We always insert 'id' and 'data'.
		cols := []string{"id", "data"}
		placeholders := []string{"?", "json(?)"} // Use json() for faster storage
		args := []any{docID, bodyBytes}
		// On conflict (Update), we replace the data blob
		updateSets := []string{"data=excluded.data"}

		// Iterate over promoted columns defined in the schema
		for _, field := range fields {
			cols = append(cols, field.Name)
			placeholders = append(placeholders, "?")
			updateSets = append(updateSets, fmt.Sprintf("%s=excluded.%s", field.Name, field.Name))

			// Extract value efficiently using gjson (zero allocation)
			res := gjson.GetBytes(bodyBytes, field.Name)
			var val any

			// Map JSON values to Go/SQL types
			switch field.Type {
			case TypeInt:
				val = res.Int()
			case TypeFloat:
				val = res.Float()
			case TypeBool:
				if res.Bool() {
					val = 1
				} else {
					val = 0
				}
			case TypeDateTime:
				// Validate and Normalize to UTC String with Millis
				rawStr := res.String()
				validStr, err := validateAndNormalizeDateTime(rawStr)
				if err != nil {
					http.Error(w, fmt.Sprintf("Field '%s' invalid datetime: %s", field.Name, rawStr), 400)
					return
				}
				val = validStr
			default: // TypeText
				val = res.String()
			}
			args = append(args, val)
		}

		// Construct the final UPSERT query
		query := fmt.Sprintf(
			"INSERT INTO main.%s (%s) VALUES (%s) ON CONFLICT(id) DO UPDATE SET %s",
			collection,
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(updateSets, ", "),
		)

		// --- Transaction Start ---
		tx, _ := db.Begin()
		defer tx.Rollback()

		// 1. Fetch Old Data
		// Needed by the Hub to detect if a document 'Left' a query filter (REMOVE event)
		var oldData sql.NullString
		tx.QueryRow(fmt.Sprintf("SELECT json(data) FROM main.%s WHERE id = ?", collection), docID).Scan(&oldData)

		// 2. Write Data to Main DB
		if _, err := tx.Exec(query, args...); err != nil {
			http.Error(w, "Write Failed: "+err.Error(), 500)
			return
		}

		// 3. Write Log to Attached Audit DB
		// This replaces the Trigger logic. We explicitly insert the event.
		// Note: We assume "UPDATE" for simplicity in upsert, but the Hub handles logic based on oldData being null.
		_, err := tx.Exec(
			`INSERT INTO audit.changelog
            (operation, collection_name, document_id, new_data, old_data)
            VALUES (?, ?, ?, json(?), json(?))`,
			"UPDATE", collection, docID, bodyBytes, oldData,
		)

		if err != nil {
			http.Error(w, "Audit Log Failed", 500)
			return
		}

		tx.Commit()
		// --- Transaction End ---

		// Signal the background processor to wake up
		notifyUpdate()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "replaced", "id": docID})
	}
}

// updateDocumentHandler handles PATCH requests for partial updates (Merge Patch).
// It selectively updates columns that are present in the patch.
func updateDocumentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		docID := r.PathValue("docId")

		patchBytes, _ := io.ReadAll(r.Body)
		if !json.Valid(patchBytes) {
			http.Error(w, "Invalid JSON", 400)
			return
		}

		fields := GetCollectionFields(collection)
		// Parse patch to find which fields are being updated
		patchResult := gjson.ParseBytes(patchBytes)

		// Always update the 'data' blob using SQLite's json_patch function
		updateParts := []string{"data = json_patch(data, json(?))"}
		args := []any{patchBytes}

		// Check if any Promoted Columns are in the patch
		for _, field := range fields {
			valRes := patchResult.Get(field.Name)
			if valRes.Exists() {
				// Field is present in patch, so update the column
				updateParts = append(updateParts, fmt.Sprintf("%s = ?", field.Name))
				var val any
				switch field.Type {
				case TypeInt:
					val = valRes.Int()
				case TypeFloat:
					val = valRes.Float()
				case TypeBool:
					if valRes.Bool() {
						val = 1
					} else {
						val = 0
					}
				case TypeDateTime:
					rawStr := valRes.String()
					validStr, err := validateAndNormalizeDateTime(rawStr)
					if err != nil {
						http.Error(w, fmt.Sprintf("Field '%s' invalid datetime: %s", field.Name, rawStr), 400)
						return
					}
					val = validStr
				default:
					val = valRes.String()
				}
				args = append(args, val)
			}
		}

		args = append(args, docID) // Add ID for WHERE clause

		tx, _ := db.Begin()
		defer tx.Rollback()

		// 1. Get Old Data (Required for Changelog)
		var oldData sql.NullString
		tx.QueryRow(fmt.Sprintf("SELECT json(data) FROM main.%s WHERE id = ?", collection), docID).Scan(&oldData)

		if !oldData.Valid {
			http.Error(w, "Document not found", 404)
			return
		}

		// 2. Execute Update
		sqlStatement := fmt.Sprintf("UPDATE main.%s SET %s WHERE id = ?", collection, strings.Join(updateParts, ", "))
		if _, err := tx.Exec(sqlStatement, args...); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		// 3. Fetch New Data (Required for Changelog)
		// Since we did a patch inside SQL, we don't have the full new state in memory.
		// We must read it back to store a complete snapshot in the log.
		var newData sql.NullString
		tx.QueryRow(fmt.Sprintf("SELECT json(data) FROM main.%s WHERE id = ?", collection), docID).Scan(&newData)

		// 4. Write Log
		_, err := tx.Exec(
			`INSERT INTO audit.changelog
            (operation, collection_name, document_id, new_data, old_data)
            VALUES (?, ?, ?, json(?), json(?))`,
			"UPDATE", collection, docID, newData, oldData,
		)

		if err != nil {
			http.Error(w, "Audit Log Failed: "+err.Error(), 500)
			return
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, "Commit failed", 500)
			return
		}

		notifyUpdate()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "patched", "id": docID})
	}
}

// deleteDocumentHandler removes a document and logs the deletion.
func deleteDocumentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		docID := r.PathValue("docId")

		tx, _ := db.Begin()
		defer tx.Rollback()

		// 1. Fetch Old Data (to log what was deleted)
		var oldData sql.NullString
		tx.QueryRow(fmt.Sprintf("SELECT json(data) FROM main.%s WHERE id = ?", collection), docID).Scan(&oldData)

		if !oldData.Valid {
			http.Error(w, "Document not found", 404)
			return
		}

		// 2. Perform Delete
		_, err := tx.Exec(fmt.Sprintf("DELETE FROM main.%s WHERE id = ?", collection), docID)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		// 3. Write Log (new_data is NULL for delete)
		_, err = tx.Exec(
			`INSERT INTO audit.changelog
            (operation, collection_name, document_id, new_data, old_data)
            VALUES (?, ?, ?, NULL, json(?))`,
			"DELETE", collection, docID, oldData,
		)

		tx.Commit()
		notifyUpdate()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted", "id": docID})
	}
}

// getDocumentHandler retrieves a single document by ID.
func getDocumentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		docID := r.PathValue("docId")

		// Note: We use json(data) to ensure the client receives a JSON string,
		// because we store it as JSON (jsonb).
		query := fmt.Sprintf("SELECT json(data) FROM main.%s WHERE id = ?", collection)

		var data json.RawMessage
		if err := db.QueryRow(query, docID).Scan(&data); err != nil {
			http.Error(w, "Not found", 404)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}

// handleGetAllCollections lists all user-created collections, excluding SQLite internal tables
// and our own system tables.
func handleGetAllCollections(db *sql.DB, w http.ResponseWriter) {
	rows, _ := db.Query(`
		SELECT name FROM sqlite_master
		WHERE type='table'
		AND name NOT LIKE 'sqlite_%'
		AND name NOT LIKE 'system_%'`)
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var s string
		rows.Scan(&s)
		cols = append(cols, s)
	}
	if cols == nil {
		cols = []string{}
	}
	json.NewEncoder(w).Encode(cols)
}

// handleDeleteCollection drops a collection table and removes its metadata.
// WARNING: This is destructive and deletes all data in the collection.
func handleDeleteCollection(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	tx, _ := db.Begin()
	// Drop the physical table
	tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS main.%s", req.Name))
	// Clean up metadata
	tx.Exec("DELETE FROM system_schema WHERE name = ?", req.Name)
	tx.Commit()

	// OPTIMIZATION: Update memory directly
	RemoveCollectionSchema(req.Name)

	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

// =============================================================================
// DOCUMENT DISPATCHER
// =============================================================================

// documentHandler acts as a router for /db/data/{collection}/{docId}
// It delegates to the specific CRUD handler based on the HTTP method.
func documentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Sanitize Inputs
		collection := r.PathValue("collection")
		if !collectionNameSanitizer.MatchString(collection) {
			http.Error(w, "Invalid collection name", 400)
			return
		}

		docID := r.PathValue("docId")
		if docID == "" {
			http.Error(w, "Document ID required", 400)
			return
		}

		// 2. Dispatch
		// Note: We use 'nil' for the Hub in GET requests as they don't trigger updates.
		// For Write requests, we assume the Hub is accessible via global/closure or passed in.
		// If you structured Server() to pass Hub, use that.
		// Here we rely on the implementation where notifyUpdate() is global/package-level.
		switch r.Method {
		case http.MethodGet:
			getDocumentHandler(db).ServeHTTP(w, r)
		case http.MethodPut:
			setDocumentHandler(db).ServeHTTP(w, r)
		case http.MethodPatch:
			updateDocumentHandler(db).ServeHTTP(w, r)
		case http.MethodDelete:
			deleteDocumentHandler(db).ServeHTTP(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// =============================================================================
// QUERY ENGINE
// =============================================================================

// queryHandler executes a read-only QueryDSL request against a collection.
// Route: POST /db/query/{collection}
func queryHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		if !collectionNameSanitizer.MatchString(collection) {
			http.Error(w, "Invalid collection name", 400)
			return
		}

		// 1. Parse Query DSL
		var dsl QueryDSL
		// Use ParseAndValidateQuery for strict validation (prevents SQL injection via operators)
		// We read body to bytes first to pass to the validator
		bodyBytes, _ := io.ReadAll(r.Body)
		parsedDSL, err := ParseAndValidateQuery(bodyBytes)
		if err != nil {
			http.Error(w, "Invalid Query: "+err.Error(), 400)
			return
		}
		dsl = *parsedDSL

		// 2. Build SQL
		// Note: buildQuery needs to generate "SELECT id, json(data) ..."
		// to handle the JSON storage correctly.
		sqlQuery, args, err := buildQuery("main."+collection, &dsl)
		if err != nil {
			http.Error(w, "Failed to build query: "+err.Error(), 400)
			return
		}

		// 3. Execute
		rows, err := db.Query(sqlQuery, args...)
		if err != nil {
			http.Error(w, "Query Execution Failed: "+err.Error(), 500)
			return
		}
		defer rows.Close()

		// 4. Serialize Results
		// Pre-allocate a slice. Assume Limit is around 20-50 usually.
		results := make([]Document, 0, 50)

		for rows.Next() {
			var doc Document
			var rawData []byte // Read as bytes

			if err := rows.Scan(&doc.ID, &rawData); err != nil {
				continue
			}

			// Zero-copy cast bytes to RawMessage
			// This avoids parsing the JSON on the server side.
			doc.Data = json.RawMessage(rawData)
			results = append(results, doc)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	}
}

// =============================================================================
// INDEX MANAGEMENT
// =============================================================================

// indexHandler creates a database index to optimize specific queries.
// Route: POST /db/indexes/{collection}
func indexHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		if !collectionNameSanitizer.MatchString(collection) {
			http.Error(w, "Invalid collection name", 400)
			return
		}

		var req IndexOptions
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", 400)
			return
		}

		if !identifierSanitizer.MatchString(req.Name) {
			http.Error(w, "Invalid index name", 400)
			return
		}

		// 1. Build Index SQL
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString("CREATE ")
		if req.Unique {
			sqlBuilder.WriteString("UNIQUE ")
		}
		// Note: Target 'main' database explicitly
		sqlBuilder.WriteString(fmt.Sprintf("INDEX IF NOT EXISTS main.%s ON %s (", req.Name, collection))

		cols := make([]string, len(req.Fields))
		for i, field := range req.Fields {
			if !identifierSanitizer.MatchString(field) {
				http.Error(w, "Invalid field name: "+field, 400)
				return
			}

			// Optimization Check:
			// If the field is a Native Column (from schema), index it directly.
			// If it's inside JSON, use an Expression Index.
			fields := GetCollectionFields(collection)
			isNative := false
			for _, f := range fields {
				if f.Name == field {
					isNative = true
					break
				}
			}

			if isNative {
				cols[i] = field // Native Column Index (Fastest)
			} else {
				// Expression Index (json_extract)
				cols[i] = fmt.Sprintf("json_extract(data, '$.%s')", field)
			}
		}
		sqlBuilder.WriteString(strings.Join(cols, ", "))
		sqlBuilder.WriteString(");")

		// 2. Execute
		_, err := dbExec(db, sqlBuilder.String())
		if err != nil {
			http.Error(w, "Failed to create index: "+err.Error(), 500)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{
			"status":     "created",
			"collection": collection,
			"index":      req,
		})
	}
}

// =============================================================================
// SYSTEM HEALTH
// =============================================================================

// healthHandler checks the database connection status.
// Route: GET /health
func healthHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer cancel()

		err := db.PingContext(ctx)
		w.Header().Set("Content-Type", "application/json")

		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"status":   "unhealthy",
				"database": "disconnected",
				"error":    err.Error(),
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":   "ok",
			"database": "connected",
		})
	}
}
