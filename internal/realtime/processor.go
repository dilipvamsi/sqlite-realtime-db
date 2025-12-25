// processor.go
package realtime

import (
	"database/sql"
	"log"
	"strconv"
)

// A simple, non-blocking function to signal an update.
func notifyUpdate() {
	select {
	case dbUpdateChan <- true:
	default: // Do nothing if the channel is already full.
	}
}

const batchSize = 1000

// runEventProcessor waits for notifications from the database hook and processes
// new changelog entries. It's designed to be crash-safe and durable.
func runEventProcessor(db *sql.DB, hub *Hub) {
	// Initialize lastProcessedId from the database to make the processor crash-safe.
	// This ensures that on restart, it picks up exactly where it left off.
	var lastProcessedId int64
	var valueStr string
	err := db.QueryRow(`SELECT value FROM audit.system_state WHERE key = 'last_processed_changelog_id'`).Scan(&valueStr)
	if err != nil {
		log.Fatalf("FATAL: Could not read last_processed_changelog_id from database: %v", err)
	}
	lastProcessedId, _ = strconv.ParseInt(valueStr, 10, 64)
	log.Printf("Event processor started. Resuming from changelog ID: %d", lastProcessedId)

	// This removes the SQL parsing overhead from the tight loop.
	stmt, err := db.Prepare(`
        SELECT id, operation, collection_name, document_id, new_data, old_data
        FROM audit.changelog
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?`) // Parameterize Limit
	if err != nil {
		log.Fatal("Failed to prepare processor statement:", err)
	}
	defer stmt.Close()

	// The "Sentry" loop: waits for a signal that work is available.
	for range dbUpdateChan {
		// The "Worker" loop: processes the entire backlog in batches until caught up.
		for {
			rows, err := stmt.Query(lastProcessedId, batchSize)
			if err != nil {
				log.Printf("Event processor: query error: %v", err)
				break // Exit the inner loop on error and wait for the next signal.
			}

			var eventsProcessedInBatch int
			var newLastIdInBatch int64 = lastProcessedId

			for rows.Next() {
				// OPTIMIZATION: Get event from Pool
				event := EventPool.Get().(*RealTimeEvent)

				var eventDBId int64
				var rawNewData, rawOldData sql.NullString
				if err := rows.Scan(&eventDBId, &event.Operation, &event.Collection, &event.DocumentID, &rawNewData, &rawOldData); err != nil {
					log.Printf("Event processor: scan error: %v", err)
					EventPool.Put(event) // Return on error
					continue
				}
				// log.Printf("Op: %s, id: %s, coll: %s\n", event.Operation, event.DocumentID, event.Collection)
				if rawNewData.Valid {
					event.Data = unsafeStringToRawJson(rawNewData.String)
				} else {
					event.Data = nil
				}

				if rawOldData.Valid {
					event.OldData = unsafeStringToRawJson(rawOldData.String)
				} else {
					event.OldData = nil
				}

				hub.broadcast <- event
				newLastIdInBatch = eventDBId
				eventsProcessedInBatch++
			}
			rows.Close() // It's crucial to close rows before the next DB operation.

			// Persist the new lastProcessedId *after* a batch is successfully processed.
			if newLastIdInBatch > lastProcessedId {
				_, err := dbExec(db, `UPDATE audit.system_state SET value = ? WHERE key = 'last_processed_changelog_id'`, newLastIdInBatch)
				if err != nil {
					// If this fails, we don't update our in-memory ID. This ensures we will retry
					// processing this batch on the next wakeup, providing at-least-once delivery.
					log.Printf("CRITICAL: Failed to persist lastProcessedId %d: %v", newLastIdInBatch, err)
					break
				}
				// Only update the in-memory ID after successful persistence.
				lastProcessedId = newLastIdInBatch
			}

			// If we processed fewer events than our batch limit, we know we're caught up.
			if eventsProcessedInBatch < batchSize {
				break
			}
		}
	}
}
