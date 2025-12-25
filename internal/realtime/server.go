// server.go
package realtime

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"time"
)

//go:embed public
var publicFs embed.FS

var IsSqlLoggingEnabled = false

func Server(db *sql.DB, host string, port uint16) {

	// 1. Register the hook for changelog
	// RegisterChangelogHook(db)

	if os.Getenv("IS_SQL_LOGGING_ENABLED") == "1" {
		IsSqlLoggingEnabled = true
	}

	// 2. Initialize and run the WebSocket Hub in its own goroutine.
	hub := newHub()
	go hub.run()

	// 3. Run the event processor in its own goroutine.
	go runEventProcessor(db, hub)

	// 4. Run the cleanup janitor in its own goroutine.
	RunChangelogJanitor(db, 24*time.Hour, 1*time.Hour)

	// 5. Register API and WebSocket routes
	mux := http.NewServeMux()

	// == System Route ==
	mux.HandleFunc("GET /health", healthHandler(db))

	// == Collection Routes ==
	mux.HandleFunc("GET /db/collections", collectionHandler(db))
	mux.HandleFunc("GET /db/collections/{collection}", collectionHandler(db))
	mux.HandleFunc("POST /db/collections/{collection}", collectionHandler(db))
	mux.HandleFunc("PATCH /db/collections/{collection}", collectionHandler(db))
	mux.HandleFunc("DELETE /db/collections/{collection}", collectionHandler(db))

	// == Index Route ==
	mux.HandleFunc("POST /db/indexes/{collection}", indexHandler(db))

	// == Document Routes ==
	// Each method for the document path is now explicitly handled.
	// They all point to the same documentHandler, which will then switch on r.Method.
	mux.HandleFunc("GET /db/data/{collection}/{docId}", documentHandler(db))
	mux.HandleFunc("PUT /db/data/{collection}/{docId}", documentHandler(db))
	mux.HandleFunc("PATCH /db/data/{collection}/{docId}", documentHandler(db))
	mux.HandleFunc("DELETE /db/data/{collection}/{docId}", documentHandler(db))

	// == Query Route ==
	// We use POST to allow for a complex JSON query object in the request body.
	mux.HandleFunc("POST /db/query/{collection}", queryHandler(db))

	// == Batch Route ==
    mux.HandleFunc("POST /db/batch", batchHandler(db))

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) { serveWs(hub, w, r, db) })
	publicFS, err := fs.Sub(publicFs, "public")
	if err != nil {
		log.Fatal(err)
	}

	mux.HandleFunc("GET /studio", func(w http.ResponseWriter, r *http.Request) {
		// Read the content of the embedded studio.html file
		content, err := fs.ReadFile(publicFS, "studio.html")
		if err != nil {
			http.Error(w, "Studio file not found", http.StatusInternalServerError)
			return
		}
		// Serve the file with the correct content type
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(content)
	})

	// Serve Simulator UI
	mux.HandleFunc("GET /simulator", func(w http.ResponseWriter, r *http.Request) {
		content, err := fs.ReadFile(publicFS, "simulator.html")
		if err != nil {
			// Fallback for development if file isn't embedded yet
			http.ServeFile(w, r, "internal/realtime/public/simulator.html")
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(content)
	})

	// Create a file server that serves files from this sub-filesystem.
	// Now, a request to "/" will correctly map to "index.html" inside the embedded "public" folder.
	fsHandler := http.FileServer(http.FS(publicFS))

	// Register the handler.
	mux.Handle("/", fsHandler)

	if host == "" {
		host = "localhost"
	}

	// 6. Start the server
	log.Printf("Server starting on http://%s:%d...\n", host, port)
	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), mux); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
