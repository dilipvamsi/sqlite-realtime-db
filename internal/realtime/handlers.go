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
	Schema []FieldDef `json:"schema"` // List of typed columns to promote from JSON
}

// CollectionResponse defines the payload for creating a new collection.
// It allows defining specific typed columns ("Schema") that are promoted from the JSON blob
// for high-performance indexing and querying.
type CollectionResponse struct {
	Name   string     `json:"name"`   // Name of the table
	Schema []FieldDef `json:"schema"` // List of typed columns to promote from JSON
}

// UpdateSchemaRequest defines the payload for migrating fields.
type UpdateSchemaRequest struct {
	Promote []FieldDef `json:"promote"` // Extract field from JSON Blob -> Native Column (Data Preserved)
	Demote  []string   `json:"demote"`  // Move field from Native Column -> JSON Blob (Data Preserved)
	Add     []FieldDef `json:"add"`     // Create new Native Column (Starts NULL/Empty)
	Delete  []string   `json:"delete"`  // Drop Native Column (Data DESTROYED)
}

// IndexOptions defines the payload for creating an index.
type IndexOptions struct {
	Name   string   `json:"name"`   // Index name (e.g., idx_status)
	Fields []string `json:"fields"` // Fields to index
	Unique bool     `json:"unique"` // Enforce uniqueness constraint
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

// Queryer is an interface to allow helper functions to accept either *sql.DB or *sql.Tx.
type Queryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

// fetchAndReconstruct reads a document from the database and merges its
// native columns back into a single JSON object.
//
// Background: To save space and improve performance, we "Split" the data on write.
// Typed columns (e.g., 'status') are stored in SQL columns, and removed from the 'data' BLOB.
// When reading, we must reverse this process to present a unified JSON document to the client/log.
func fetchAndReconstruct(q Queryer, collection, docID string, fields []FieldDef) ([]byte, error) {
	// 1. Build the SELECT query.
	// We fetch the JSON blob plus every native column defined in the schema.
	cols := []string{"json(data)"} // 'json(data)' converts the internal JSONB/BLOB to text
	for _, f := range fields {
		cols = append(cols, f.Name)
	}

	query := fmt.Sprintf("SELECT %s FROM main.%s WHERE id = ?", strings.Join(cols, ", "), collection)

	// 2. Prepare destination pointers for Scan.
	scanDest := make([]any, len(cols))
	var dataBlob []byte
	scanDest[0] = &dataBlob

	// We use interface{} pointers for columns so the driver can scan into the appropriate type
	// (int64, float64, string, etc.) without us guessing beforehand.
	fieldValues := make([]any, len(fields))
	for i := range fields {
		scanDest[i+1] = &fieldValues[i]
	}

	// 3. Execute the query.
	if err := q.QueryRow(query, docID).Scan(scanDest...); err != nil {
		return nil, err
	}

	// 4. Reconstruct using the shared zero-allocation helper
	return constructDocumentJSON(dataBlob, fields, fieldValues)
}

// collectionHandler routes API requests to the appropriate collection management function.
// Route: /db/collections
func collectionHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			collection := r.PathValue("collection")
			if collection == "" {
				handleGetAllCollections(db, w)
			} else {
				handleGetCollection(db, w, collection)
			}
		case http.MethodPost:
			handleCreateCollection(db, w, r)
		case http.MethodPatch:
			handleUpdateSchema(db, w, r)
		case http.MethodDelete:
			handleDeleteCollection(db, w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// handleCreateCollection creates a new physical table with Typed Columns and a Catch-All JSON Blob.
// It also persists the schema definition to the `system_schema` table for future reference.
func handleCreateCollection(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	var req CreateCollectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", 400)
		return
	}
	collection := r.PathValue("collection")

	// Sanity check to prevent SQL injection in table names
	if !collectionNameSanitizer.MatchString(collection) {
		http.Error(w, "Invalid collection name", 400)
		return
	}
	// Validate supported data types
	for _, field := range req.Schema {
		if !field.Type.IsValid() {
			http.Error(w, fmt.Sprintf("Invalid type '%s' for field '%s'", field.Type, field.Name), 400)
			return
		}
	}

	// Serialize schema to store in metadata table
	schemaJSON, _ := json.Marshal(req.Schema)

	tx, _ := db.Begin()
	defer tx.Rollback()

	// 1. Construct the CREATE TABLE SQL statement
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, ", collection))
	for _, field := range req.Schema {
		sqlType := "TEXT"
		// Map logical types to SQLite storage classes
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
	// Add the catch-all BLOB column. We use BLOB because we store binary JSONB.
	// STRICT mode enforces types, improving data integrity.
	sb.WriteString("data BLOB) STRICT;")

	// Execute creation
	if _, err := tx.Exec(sb.String()); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// 2. Insert Metadata into system_schema
	if _, err := tx.Exec("INSERT INTO system_schema (name, schema) VALUES (?, ?)", collection, schemaJSON); err != nil {
		http.Error(w, "Failed to save metadata", 500)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "Commit failed", 500)
		return
	}

	// Update the in-memory schema cache immediately
	SetCollectionSchema(collection, req.Schema)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "created", "collection": collection})
}

// documentHandler is the main router for CRUD operations on documents.
// Route: /db/data/{collection}/{docId}
func documentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		// Delegate to specific method handlers
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

// setDocumentHandler (PUT) performs an "Upsert" (Insert or Replace).
// It implements "Application-Side CDC" (Change Data Capture) by writing to
// both the Data table and the Audit Log in a single transaction.
func setDocumentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		docID := r.PathValue("docId")

		// Read the raw JSON body
		bodyBytes, _ := io.ReadAll(r.Body)
		if !json.Valid(bodyBytes) {
			http.Error(w, "Invalid JSON", 400)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "DB Connection Error", 500)
			return
		}
		defer tx.Rollback()

		opType, err := ApplySetDocument(tx, collection, docID, bodyBytes)

		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, "Commit Failed", 500)
			return
		}
		// Signal Event Processor
		notifyUpdate()

		w.Header().Set("Content-Type", "application/json")
		if opType == "INSERT" {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "replaced", "id": docID, "operation": string(opType)})
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

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "DB Error", 500)
			return
		}
		defer tx.Rollback()

		if err := ApplyUpdateDocument(tx, collection, docID, patchBytes); err != nil {
			if strings.Contains(err.Error(), "not found") {
				http.Error(w, err.Error(), 404)
			} else {
				http.Error(w, err.Error(), 500)
			}
			return
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, "Commit Failed", 500)
			return
		}

		notifyUpdate()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "patched", "id": docID})
	}
}

// deleteDocumentHandler removes a document and logs the event.
func deleteDocumentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		docID := r.PathValue("docId")

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "DB Error", 500)
			return
		}
		defer tx.Rollback()

		if err := ApplyDeleteDocument(tx, collection, docID); err != nil {
			if strings.Contains(err.Error(), "not found") {
				http.Error(w, err.Error(), 404)
			} else {
				http.Error(w, err.Error(), 500)
			}
			return
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, "Commit Failed", 500)
			return
		}

		notifyUpdate()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted", "id": docID})
	}
}

// getDocumentHandler retrieves a document by ID.
// It reconstructs the full JSON object from the split storage (Blob + Columns).
func getDocumentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		docID := r.PathValue("docId")
		fields := GetCollectionFields(collection)

		// Use the helper to fetch and merge data.
		// fetchAndReconstruct accepts *sql.DB via the Queryer interface.
		dataBlob, err := fetchAndReconstruct(db, collection, docID, fields)
		if err != nil {
			http.Error(w, "Not found", 404)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(dataBlob)
	}
}

// handleGetAllCollections lists all managed collections and their schema definitions.
// Route: GET /db/collections
func handleGetAllCollections(db *sql.DB, w http.ResponseWriter) {
	// We query the system_schema table because it contains the 'schema' JSON blob
	// which sqlite_master does not have.
	rows, err := db.Query(`SELECT name, schema FROM main.system_schema ORDER BY name ASC`)
	if err != nil {
		// Should not happen if InitDB ran correctly, but safe fallback to empty list
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
		return
	}
	defer rows.Close()

	var collections []CollectionResponse

	for rows.Next() {
		var col CollectionResponse
		var schemaRaw []byte

		if err := rows.Scan(&col.Name, &schemaRaw); err != nil {
			continue
		}

		// Parse the stored JSON schema back into the Go struct
		if len(schemaRaw) > 0 {
			if err := json.Unmarshal(schemaRaw, &col.Schema); err != nil {
				col.Schema = []FieldDef{}
			}
		} else {
			col.Schema = []FieldDef{}
		}

		collections = append(collections, col)
	}

	// Ensure we return "[]" instead of "null" if empty
	if collections == nil {
		collections = []CollectionResponse{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(collections)
}

// handleGetCollection lists all managed collections and their schema definitions.
// Route: GET /db/collections/{collection}
func handleGetCollection(db *sql.DB, w http.ResponseWriter, collection string) {
	// We query the system_schema table because it contains the 'schema' JSON blob
	// which sqlite_master does not have.
	rows, err := db.Query(`SELECT schema FROM main.system_schema where name = ?`, collection)
	if err != nil {
		// Should not happen if InitDB ran correctly, but safe fallback to empty list
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
		return
	}
	defer rows.Close()

	hasCollection := false

	// We can reuse CreateCollectionRequest struct as it fits the shape {schema}
	var collectionResponse CreateCollectionRequest

	for rows.Next() {
		var schemaRaw []byte

		if err := rows.Scan(&schemaRaw); err != nil {
			continue
		}

		// Parse the stored JSON schema back into the Go struct
		if len(schemaRaw) > 0 {
			if err := json.Unmarshal(schemaRaw, &collectionResponse.Schema); err != nil {
				collectionResponse.Schema = []FieldDef{}
			} else {
				hasCollection = true
			}
		} else {
			collectionResponse.Schema = []FieldDef{}
		}
	}

	if !hasCollection {
		http.Error(w, fmt.Sprintf("Collection: %s not found", collection), 404)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(collectionResponse)
}

// handleDeleteCollection drops a table entirely and removes its metadata.
// WARNING: This is a destructive operation.
func handleDeleteCollection(db *sql.DB, w http.ResponseWriter, r *http.Request) {

	collection := r.PathValue("collection")
	tx, _ := db.Begin()
	// Drop Physical Table
	tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS main.%s", collection))
	// Remove Metadata
	tx.Exec("DELETE FROM system_schema WHERE name = ?", collection)
	tx.Commit()
	// Update Cache
	RemoveCollectionSchema(collection)
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

// handleUpdateSchema manages promoting, demoting, adding, and deleting columns.
// OPTIMIZED: Consolidates data migration into a SINGLE SQL UPDATE and validates inputs upfront.
// It uses "BEGIN IMMEDIATE" to prevent deadlocks during the schema migration.
func handleUpdateSchema(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	// 1. Parse Request
	var req UpdateSchemaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", 400)
		return
	}

	collection := r.PathValue("collection")

	// 2. Validate Collection Name
	if collection == "" {
		http.Error(w, "Collection name is required", 400)
		return
	}
	if !collectionNameSanitizer.MatchString(collection) {
		http.Error(w, "Invalid collection name", 400)
		return
	}

	// 3. Fetch Current Schema (Pre-computation)
	fields := GetCollectionFields(collection)

	// Create a map for O(1) checks of existing columns
	currentFieldMap := make(map[string]FieldDef)
	for _, f := range fields {
		currentFieldMap[f.Name] = f
	}

	// --- 4. PRE-FLIGHT VALIDATION (Fail Fast) ---

	// A. Validate Promote (Must be new column, valid type)
	for _, field := range req.Promote {
		if !identifierSanitizer.MatchString(field.Name) {
			http.Error(w, "Invalid promote field name: "+field.Name, 400)
			return
		}
		if !field.Type.IsValid() {
			http.Error(w, fmt.Sprintf("Invalid type for promoted field '%s'", field.Name), 400)
			return
		}
		if _, exists := currentFieldMap[field.Name]; exists {
			http.Error(w, fmt.Sprintf("Cannot promote field '%s': column already exists.", field.Name), 400)
			return
		}
	}

	// B. Validate Add (Must be new column, valid type)
	for _, field := range req.Add {
		if !identifierSanitizer.MatchString(field.Name) {
			http.Error(w, "Invalid add field name: "+field.Name, 400)
			return
		}
		if !field.Type.IsValid() {
			http.Error(w, fmt.Sprintf("Invalid type for added field '%s'", field.Name), 400)
			return
		}
		if _, exists := currentFieldMap[field.Name]; exists {
			http.Error(w, fmt.Sprintf("Cannot add field '%s': column already exists.", field.Name), 400)
			return
		}
		// Check against Promote list for duplicates in same request
		for _, p := range req.Promote {
			if p.Name == field.Name {
				http.Error(w, fmt.Sprintf("Field '%s' cannot be in both Add and Promote lists.", field.Name), 400)
				return
			}
		}
	}

	// C. Validate Demote (Must exist)
	for _, name := range req.Demote {
		if !identifierSanitizer.MatchString(name) {
			http.Error(w, "Invalid demote field name: "+name, 400)
			return
		}
		if _, exists := currentFieldMap[name]; !exists {
			http.Error(w, fmt.Sprintf("Cannot demote field '%s': column does not exist.", name), 400)
			return
		}
	}

	// D. Validate Delete (Must exist)
	for _, name := range req.Delete {
		if !identifierSanitizer.MatchString(name) {
			http.Error(w, "Invalid delete field name: "+name, 400)
			return
		}
		if _, exists := currentFieldMap[name]; !exists {
			http.Error(w, fmt.Sprintf("Cannot delete field '%s': column does not exist.", name), 400)
			return
		}
		// Check against Demote list
		for _, d := range req.Demote {
			if d == name {
				http.Error(w, fmt.Sprintf("Field '%s' cannot be in both Demote and Delete lists.", name), 400)
				return
			}
		}
	}

	// --- 5. START TRANSACTION (BEGIN IMMEDIATE) ---

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		http.Error(w, "Database pool error: "+err.Error(), 500)
		return
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		http.Error(w, "Database Locked/Busy: "+err.Error(), 503)
		return
	}

	var committed bool
	defer func() {
		if !committed {
			conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	// Double check table existence inside tx
	var exists int
	err = conn.QueryRowContext(ctx, "SELECT 1 FROM system_schema WHERE name = ?", collection).Scan(&exists)
	if err != nil || exists == 0 {
		http.Error(w, "Collection not found", 404)
		return
	}

	// --- PHASE 1: CREATE COLUMNS (Promote & Add) ---

	// Helper to create columns
	createColumns := func(list []FieldDef) error {
		for _, newField := range list {
			sqlType := "TEXT"
			switch newField.Type {
			case TypeFloat:
				sqlType = "REAL"
			case TypeInt, TypeBool:
				sqlType = "INTEGER"
			}
			alterSQL := fmt.Sprintf("ALTER TABLE main.%s ADD COLUMN %s %s", collection, newField.Name, sqlType)
			if _, err := conn.ExecContext(ctx, alterSQL); err != nil {
				return err
			}
			// Update in-memory map for metadata logic
			currentFieldMap[newField.Name] = newField
		}
		return nil
	}

	if err := createColumns(req.Promote); err != nil {
		http.Error(w, "Promote Column failed: "+err.Error(), 500)
		return
	}
	if err := createColumns(req.Add); err != nil {
		http.Error(w, "Add Column failed: "+err.Error(), 500)
		return
	}

	// --- PHASE 2: DATA MIGRATION (DML) ---
	// Construct ONE massive UPDATE statement.

	var updateSetClauses []string
	var jsonSetArgs []string    // For Demote (Column -> Blob)
	var jsonRemoveArgs []string // For Promote (Blob -> Column)

	// A. Promote Logic (Blob -> New Column)
	for _, field := range req.Promote {
		// SET col = jsonb_extract(data, '$.col')
		updateSetClauses = append(updateSetClauses,
			fmt.Sprintf("%s = jsonb_extract(data, '$.%s')", field.Name, field.Name))
		// Remove from blob
		jsonRemoveArgs = append(jsonRemoveArgs, fmt.Sprintf("'$.%s'", field.Name))
	}

	// B. Demote Logic (Old Column -> Blob)
	for _, fieldName := range req.Demote {
		// Insert into blob
		jsonSetArgs = append(jsonSetArgs, fmt.Sprintf("'$.%s'", fieldName), fieldName)
	}

	// Note: 'Add' fields start NULL/Empty, so no data migration needed.
	// Note: 'Delete' fields are just dropped, so no data migration needed.

	// C. Build the 'data = ...' clause
	if len(jsonSetArgs) > 0 || len(jsonRemoveArgs) > 0 {
		dataExpr := "data"
		if len(jsonSetArgs) > 0 {
			dataExpr = fmt.Sprintf("jsonb_set(%s, %s)", dataExpr, strings.Join(jsonSetArgs, ", "))
		}
		if len(jsonRemoveArgs) > 0 {
			dataExpr = fmt.Sprintf("jsonb_remove(%s, %s)", dataExpr, strings.Join(jsonRemoveArgs, ", "))
		}
		updateSetClauses = append(updateSetClauses, "data = "+dataExpr)
	}

	// Execute Update
	if len(updateSetClauses) > 0 {
		bigQuery := fmt.Sprintf("UPDATE main.%s SET %s", collection, strings.Join(updateSetClauses, ", "))
		if _, err := conn.ExecContext(ctx, bigQuery); err != nil {
			http.Error(w, "Data Migration failed: "+err.Error(), 500)
			return
		}
	}

	// --- PHASE 3: DROP COLUMNS (Demote & Delete) ---

	dropColumns := func(list []string) error {
		for _, fieldName := range list {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE main.%s DROP COLUMN %s", collection, fieldName)); err != nil {
				return err
			}
			delete(currentFieldMap, fieldName)
		}
		return nil
	}

	if err := dropColumns(req.Demote); err != nil {
		http.Error(w, "Demote (Drop) failed: "+err.Error(), 500)
		return
	}

	if err := dropColumns(req.Delete); err != nil {
		http.Error(w, "Delete (Drop) failed: "+err.Error(), 500)
		return
	}

	// --- PHASE 4: UPDATE METADATA ---
	var finalSchema []FieldDef
	for _, f := range currentFieldMap {
		finalSchema = append(finalSchema, f)
	}

	schemaJSON, _ := json.Marshal(finalSchema)
	if _, err := conn.ExecContext(ctx, "UPDATE system_schema SET schema = ? WHERE name = ?", schemaJSON, collection); err != nil {
		http.Error(w, "Failed to update metadata", 500)
		return
	}

	// Final Commit
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		http.Error(w, "Commit failed", 500)
		return
	}
	committed = true

	// 6. Update Cache
	SetCollectionSchema(collection, finalSchema)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "updated",
		"collection": collection,
		"schema":     finalSchema,
	})
}

// indexHandler creates a B-Tree index on specific fields.
// It automatically detects if a field is a Native Column or a JSON path
// and generates the appropriate CREATE INDEX SQL.
func indexHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		if !collectionNameSanitizer.MatchString(collection) {
			http.Error(w, "Invalid collection", 400)
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

		var sqlBuilder strings.Builder
		sqlBuilder.WriteString("CREATE ")
		if req.Unique {
			sqlBuilder.WriteString("UNIQUE ")
		}
		fmt.Fprintf(&sqlBuilder, "INDEX IF NOT EXISTS main.%s ON %s (", req.Name, collection)

		cols := make([]string, len(req.Fields))
		collFields := GetCollectionFields(collection)

		for i, field := range req.Fields {
			if !identifierSanitizer.MatchString(field) {
				http.Error(w, "Invalid field", 400)
				return
			}
			// Check if field is Native or JSON
			isNative := false
			for _, f := range collFields {
				if f.Name == field {
					isNative = true
					break
				}
			}
			if isNative {
				// Index Native Column (Fastest)
				cols[i] = field
			} else {
				// Index JSON Expression (e.g., jsonb_extract(data, '$.field'))
				cols[i] = fmt.Sprintf("jsonb_extract(data, '$.%s')", field)
			}
		}
		sqlBuilder.WriteString(strings.Join(cols, ", "))
		sqlBuilder.WriteString(");")

		if _, err := dbExec(db, sqlBuilder.String()); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"status": "created", "index": req})
	}
}

// healthHandler checks the connection pool status via Ping.
func healthHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "database": "connected"})
	}
}

// queryHandler executes a read-only QueryDSL request against a collection.
// Route: POST /db/query/{collection}
func queryHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := r.PathValue("collection")
		// 1. Sanitize input
		if !collectionNameSanitizer.MatchString(collection) {
			http.Error(w, "Invalid collection name", 400)
			return
		}

		// 2. Parse and Validate the Query DSL
		bodyBytes, _ := io.ReadAll(r.Body)
		parsedDSL, err := ParseAndValidateQuery(bodyBytes)
		if err != nil {
			http.Error(w, "Invalid Query: "+err.Error(), 400)
			return
		}

		// 3. Prepare for Split Storage Reconstruction
		// We need to fetch the JSON blob AND all the native columns.
		fields := GetCollectionFields(collection)

		// Build the column list: "id, json(data), col1, col2..."
		// We cast data to json(data) to get text back from BLOB.
		selectCols := []string{"id", "json(data)"}
		for _, f := range fields {
			selectCols = append(selectCols, f.Name)
		}
		selectClause := "SELECT " + strings.Join(selectCols, ", ")

		// 4. Generate SQL
		// We use the shared buildQuery function for WHERE/ORDER BY logic.
		// Note: We access the 'main' database namespace.
		sqlQuery, args, err := buildQuery("main."+collection, parsedDSL)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		// HACK/OPTIMIZATION: Replace the default SELECT clause from buildQuery
		// with our comprehensive list of columns needed for reconstruction.
		// Assumes buildQuery returns "SELECT id, json(data) FROM ..."
		// This avoids changing the signature of buildQuery while supporting Split Storage.
		sqlQuery = strings.Replace(sqlQuery, "SELECT id, json(data)", selectClause, 1)

		// 5. Execute Query
		rows, err := db.Query(sqlQuery, args...)
		if err != nil {
			http.Error(w, "Execution Error: "+err.Error(), 500)
			return
		}
		defer rows.Close()

		// 6. Process Results
		results := make([]Document, 0, 50) // Pre-allocate assuming limit is small

		for rows.Next() {
			// Prepare destination pointers
			// [0]=id, [1]=dataBlob, [2...N]=columns
			scanDest := make([]any, len(selectCols))
			var id string
			var dataBlob []byte
			scanDest[0] = &id
			scanDest[1] = &dataBlob

			// Interface pointers for dynamic columns
			colValues := make([]any, len(fields))
			for i := range fields {
				scanDest[i+2] = &colValues[i]
			}

			if err := rows.Scan(scanDest...); err != nil {
				continue
			}

			// 7. Reconstruct the Full Document (Optimized Zero-Parse Splicing)
			finalJSON, err := constructDocumentJSON(dataBlob, fields, colValues)
			if err != nil {
				continue
			}

			// Append to result list
			results = append(results, Document{
				ID:   id,
				Data: json.RawMessage(finalJSON),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	}
}

// constructDocumentJSON merges the document ID, native columns, and the remaining JSON blob
// into a single JSON byte slice using zero-parse splicing.
func constructDocumentJSON(rawBlob []byte, fields []FieldDef, colValues []any) ([]byte, error) {
	// 1. Create Header (Native Columns)
	headerMap := make(map[string]any, len(fields))

	for i, f := range fields {
		val := colValues[i]
		if val != nil {
			headerMap[f.Name] = val
		}
	}

	// 2. Marshal Header
	headerBytes, err := json.Marshal(headerMap)
	if err != nil {
		return nil, err
	}

	// 3. Splice Header + Blob
	// If rawBlob is empty/nil or just "{}" (len 2), return header only
	if len(rawBlob) <= 2 {
		return headerBytes, nil
	}

	// Alloc exact size: header + comma + blob - 1 overlap (})
	// len(header) - 1 (}) + 1 (,) + len(blob) - 1 ({) = len(header) + len(blob) - 1
	finalJSON := make([]byte, 0, len(headerBytes)+len(rawBlob))

	// Append Header (minus closing '}')
	finalJSON = append(finalJSON, headerBytes[:len(headerBytes)-1]...)
	// Append Comma
	finalJSON = append(finalJSON, ',')
	// Append Blob (minus opening '{')
	// Assumes valid JSON starting with '{'
	finalJSON = append(finalJSON, rawBlob[1:]...)

	return finalJSON, nil
}

type BatchOperation struct {
	Method     string          `json:"method"` // "PUT", "PATCH", "DELETE"
	Collection string          `json:"collection"`
	DocID      string          `json:"docId"`
	Data       json.RawMessage `json:"data,omitempty"`
}

type BatchRequest struct {
	Operations []BatchOperation `json:"operations"`
}

func batchHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", 400)
			return
		}

		// 1. Start ONE Transaction for all 1000 items
		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "DB Error", 500)
			return
		}
		defer tx.Rollback()

		// 2. Loop and Router
		for _, op := range req.Operations {
			// Basic Validation
			if op.Collection == "" || op.DocID == "" {
				http.Error(w, "Missing collection or docId", 400)
				return
			}

			var opErr error
			switch strings.ToUpper(op.Method) {
			case "PUT":
				_, opErr = ApplySetDocument(tx, op.Collection, op.DocID, []byte(op.Data))
			case "PATCH":
				opErr = ApplyUpdateDocument(tx, op.Collection, op.DocID, []byte(op.Data))
			case "DELETE":
				opErr = ApplyDeleteDocument(tx, op.Collection, op.DocID)
			default:
				http.Error(w, "Unknown method: "+op.Method, 400)
				return
			}

			if opErr != nil {
				// Atomicity: If ONE fails, the WHOLE batch fails.
				http.Error(w, fmt.Sprintf("Error on %s %s: %v", op.Collection, op.DocID, opErr), 500)
				return
			}
		}

		// 3. Commit ONCE (The massive performance win)
		if err := tx.Commit(); err != nil {
			http.Error(w, "Commit Failed", 500)
			return
		}

		// 4. Notify ONCE
		notifyUpdate()

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"count":  len(req.Operations),
		})
	}
}
