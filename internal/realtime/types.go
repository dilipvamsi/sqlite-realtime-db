// types.go
package realtime

import (
	"sync"

	json "github.com/goccy/go-json"
)

// Subscription holds the client's subscription details.
type Subscription struct {
	ID                string          `json:"subscriptionId"`
	Collection        string          `json:"collection"`
	DocID             string          `json:"docId,omitempty"`
	Query             *QueryDSL       `json:"query,omitempty"`
	RawWhereCondition json.RawMessage `json:"-"` // Used internally for hashing
}

const SQLITE_INSERT = 18
const SQLITE_DELETE = 9
const SQLITE_UPDATE = 23

// OperationType defines the type for our database operation types.
type OperationType string

// Constants for all database operation types.
const (
	OperationInsert OperationType = "INSERT"
	OperationUpdate OperationType = "UPDATE"
	OperationDelete OperationType = "DELETE"
	OperationRemove OperationType = "REMOVE"
)

// EventType defines the type for our WebSocket message types.
type EventType string

// Constants for all server-to-client WebSocket event types.
const (
	//EventTypeRealTimeUpdate is the real-time event from the DB
	EventTypeRealTimeUpdate EventType = "realtime_update"

	// EventTypeInitialDataBatch is sent for each batch of initial documents.
	EventTypeInitialDataBatch EventType = "initial_data_batch"

	// EventTypeInitialDataComplete signals the end of the initial data stream.
	EventTypeInitialDataComplete EventType = "initial_data_complete"

	// EventTypeSubscriptionAck signals the Acknowledgment of subscription
	EventTypeSubscriptionAck EventType = "subscribe_ack"

	// EventTypeSubscriptionError is sent when a subscription request fails.
	EventTypeSubscriptionError EventType = "subscription_error"
)

// RealTimeEvent is the pure data object representing a single database change.
type RealTimeEvent struct {
	Operation  OperationType   `json:"operation"`
	Collection string          `json:"collection"`
	DocumentID string          `json:"docId"`
	Data       json.RawMessage `json:"data,omitempty"`
	OldData    json.RawMessage `json:"oldData,omitempty"`
}

// EventPool reuses RealTimeEvent structs to reduce GC overhead on high-frequency updates.
var EventPool = sync.Pool{
	New: func() any {
		return new(RealTimeEvent)
	},
}

// Server -> Client: Acknowledgment containing the assigned Topic ID
type SubscribeAckPayload struct {
	Type           EventType `json:"type"` // "subscribe_ack"
	SubscriptionID string    `json:"subscriptionId"`
	Topic          string    `json:"topic"`
}

// CachedRealTimeUpdatePayload is the structured message for broadcasting by caching event.
type CachedRealTimeUpdatePayload struct {
	Topic string          `json:"topic"`
	Type  EventType       `json:"type"`
	Event json.RawMessage `json:"payload"` // Use RawMessage!
}

// Document represents a single document with its ID and data.
type Document struct {
	ID   string          `json:"id"`
	Data json.RawMessage `json:"data"`
}

// InitialDataBatchPayload is the structured message for sending a batch of documents.
// Using a struct is much more performant than a map[string]any.
type InitialDataBatchPayload struct {
	SubscriptionID string     `json:"subscriptionId"`
	Type           EventType  `json:"type"`
	Collection     string     `json:"collection"`
	Documents      []Document `json:"documents"`
}

// SubscriptionCompletionPayload is the structured message for signaling the end of the initial data stream.
type SubscriptionCompletionPayload struct {
	SubscriptionID string    `json:"subscriptionId"`
	Collection     string    `json:"collection"`
	Type           EventType `json:"type"`
}

// SubscriptionErrorPayload is the structured message for sending subscription errors.
type SubscriptionErrorPayload struct {
	SubscriptionID string    `json:"subscriptionId"`
	Type           EventType `json:"type"`
	Error          string    `json:"error"`
}
