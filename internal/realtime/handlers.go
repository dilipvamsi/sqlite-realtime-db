// handlers.go
package realtime

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	json "github.com/goccy/go-json"
)

// collectionHandler manages GET, POST, and DELETE for /db/collections.
func collectionHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetAllCollections(db, w)
		case http.MethodPost:
			var req struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
			if err := CreateCollection(db, req.Name); err != nil {
				http.Error(w, fmt.Sprintf("Failed to create collection: %v", err), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"status": "created", "collection": req.Name})

		case http.MethodDelete:
			var req struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
			if err := DeleteCollection(db, req.Name); err != nil {
				http.Error(w, fmt.Sprintf("Failed to delete collection: %v", err), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "deleted", "collection": req.Name})

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// handleGetAllCollections fetches a list of all user-created collections.
func handleGetAllCollections(db *sql.DB, w http.ResponseWriter) {
	// We query SQLite's master table to find all tables,
	// excluding our own system tables and SQLite's internal tables.
	querySql := `
		SELECT name FROM sqlite_master
		WHERE type='table'
		AND name NOT LIKE 'sqlite_%'
		AND name NOT IN ('changelog', 'system_state');
	`
	rows, err := dbQuery(db, querySql)
	if err != nil {
		log.Printf("Failed to query for collections: %v", err)
		http.Error(w, "Failed to fetch collections", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var collections []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Printf("Failed to scan collection name: %v", err)
			http.Error(w, "Failed to process collections list", http.StatusInternalServerError)
			return
		}
		collections = append(collections, name)
	}

	// This ensures that we return an empty JSON array `[]` instead of `null` if no collections are found.
	if collections == nil {
		collections = []string{}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(collections)
}

// IndexRequest defines the structure for a new index creation request from the client.
type IndexRequest struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Unique bool     `json:"unique"`
}

// indexHandler manages POST /db/indexes/{collection}.
// It uses the standard library's r.PathValue() to extract the collection name.
func indexHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Parse the collection name directly from the URL path.
		collectionName := r.PathValue("collection")
		if collectionName == "" {
			http.Error(w, "Collection name is missing in the URL path", http.StatusBadRequest)
			return
		}

		// Note: We don't need to check `r.Method == http.MethodPost` here because
		// the router in `main.go` is already configured to only send POST
		// requests to this handler. This makes the handler cleaner.

		// 2. Decode the JSON request body into our struct.
		var req IndexRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// 3. Delegate the core logic to the database function.
		if err := CreateIndex(db, collectionName, req.Name, req.Fields, req.Unique); err != nil {
			// Provide a more specific error if possible. Bad Request is often appropriate here.
			http.Error(w, fmt.Sprintf("Failed to create index: %v", err), http.StatusBadRequest)
			return
		}

		// 4. Send a success response.
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{
			"status":     "created",
			"collection": collectionName,
			"index":      req,
		})
	}
}

// documentHandler is a dispatcher for all requests to /db/data/{collection}/{docId}.
// It uses the standard library's r.PathValue() to extract URL parameters.
func documentHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Parse URL parameters using the built-in Go 1.22+ method.
		collection := r.PathValue("collection")
		docID := r.PathValue("docId")

		// It's crucial to validate the collection name before using it in a query string.
		if !collectionNameSanitizer.MatchString(collection) {
			http.Error(w, "Invalid collection name", http.StatusBadRequest)
			return
		}

		if docID == "" {
			http.Error(w, fmt.Sprintf("Document ID is required for %s", r.Method), http.StatusBadRequest)
			return
		}

		// 2. Route the request to the appropriate helper based on the HTTP method.
		switch r.Method {
		case http.MethodGet:
			handleGetDocument(db, w, collection, docID)
			return
		case http.MethodPut:
			handlePutDocument(db, w, collection, docID, r)
		case http.MethodPatch:
			handlePatchDocument(db, w, collection, docID, r)
		case http.MethodDelete:
			handleDeleteDocument(db, w, collection, docID)
		default:
			// This is a fallback, though the router in main.go already prevents
			// other methods from reaching this handler.
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		notifyUpdate()
	}
}

// --- Helper Functions (The Actual Logic) ---

// handleGetDocument fetches a single document.
func handleGetDocument(db *sql.DB, w http.ResponseWriter, collection, docID string) {
	// Sprintf is safe here because 'collection' has been sanitized.
	query := fmt.Sprintf("SELECT data FROM %s WHERE id = ?", collection)
	var data json.RawMessage
	if err := db.QueryRow(query, docID).Scan(&data); err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Document not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to fetch document", http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// handlePutDocument replaces a document entirely or creates it if it doesn't exist (UPSERT).
func handlePutDocument(db *sql.DB, w http.ResponseWriter, collection, docID string, r *http.Request) {
	var data json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid JSON data in request body", http.StatusBadRequest)
		return
	}
	if !json.Valid(data) || string(data[0]) != "{" {
		http.Error(w, "Request body must be a valid JSON object", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("INSERT INTO %s (id, data) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET data=excluded.data;", collection)
	if _, err := dbExec(db, query, docID, data); err != nil {
		http.Error(w, "Failed to upsert document", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "replaced", "id": docID})
}

// handlePatchDocument applies a partial update using a JSON Patch document (RFC 6902).
func handlePatchDocument(db *sql.DB, w http.ResponseWriter, collection, docID string, r *http.Request) {
	var patch json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, "Invalid JSON Patch data in request body", http.StatusBadRequest)
		return
	}
	if !json.Valid(patch) {
		http.Error(w, "Request body must be a valid JSON)", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("UPDATE %s SET data = json_patch(data, ?) WHERE id = ?;", collection)
	result, err := dbExec(db, query, patch, docID)
	if err != nil {
		http.Error(w, "Failed to patch document", http.StatusInternalServerError)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		http.Error(w, "Document not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "patched", "id": docID})
}

// handleDeleteDocument removes a document.
func handleDeleteDocument(db *sql.DB, w http.ResponseWriter, collection, docID string) {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", collection)
	result, err := dbExec(db, query, docID)
	if err != nil {
		http.Error(w, "Failed to delete document", http.StatusInternalServerError)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		http.Error(w, "Document not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted", "id": docID})
}

// queryHandler executes a read-only query against a collection.
func queryHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Get the collection name from the URL.
		collectionName := r.PathValue("collection")
		if collectionName == "" {
			http.Error(w, "Collection name is missing in the URL path", http.StatusBadRequest)
			return
		}

		// 2. Decode the QueryDSL object from the request body.
		var dsl QueryDSL
		if err := json.NewDecoder(r.Body).Decode(&dsl); err != nil {
			http.Error(w, "Invalid query object in request body", http.StatusBadRequest)
			return
		}

		// 3. Reuse the existing buildQuery function to safely create the SQL.
		sqlQuery, args, err := buildQuery(collectionName, &dsl)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to build query: %v", err), http.StatusBadRequest)
			return
		}

		// 4. Execute the query using our logging wrapper.
		rows, err := dbQuery(db, sqlQuery, args...)
		if err != nil {
			http.Error(w, "Failed to execute query", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// 5. Scan the results into a slice of Document structs.
		results := make([]Document, 0)
		for rows.Next() {
			var docID string
			var data sql.NullString // Use NullString for safety

			if err := rows.Scan(&docID, &data); err != nil {
				log.Printf("Error scanning query row: %v", err)
				// Don't kill the whole request for one bad row, just log it.
				continue
			}

			doc := Document{ID: docID}
			if data.Valid {
				doc.Data = unsafeStringToRawJson(data.String)
			}
			results = append(results, doc)
		}

		// 6. Send the JSON response.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(results)
	}
}

// healthHandler checks the health of the service, including the database connection.
func healthHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Create a context with a short timeout to prevent the health check from hanging.
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer cancel()

		// db.PingContext is the idiomatic way to check database connectivity.
		err := db.PingContext(ctx)

		w.Header().Set("Content-Type", "application/json")

		if err != nil {
			// If PingContext fails, the service is unhealthy.
			log.Printf("Health check failed: database ping error: %v", err)

			// Respond with a 503 Service Unavailable status.
			w.WriteHeader(http.StatusServiceUnavailable)

			// Provide a clear JSON response.
			response := map[string]string{
				"status":   "unhealthy",
				"database": "disconnected",
			}
			json.NewEncoder(w).Encode(response)
			return
		}

		// If PingContext succeeds, the service is healthy.
		w.WriteHeader(http.StatusOK)
		response := map[string]string{
			"status":   "ok",
			"database": "connected",
		}
		json.NewEncoder(w).Encode(response)
	}
}
