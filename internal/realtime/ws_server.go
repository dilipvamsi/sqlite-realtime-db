// ws_server.go
package realtime

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"
	"unsafe"

	json "github.com/goccy/go-json"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

// ASCII Record Separator (RS)
var asciiRecordSeparator = []byte{0x1e}

// dbUpdateChan is the channel that the SQLite hook uses to notify the event processor.
// It is buffered with size 1 to coalesce rapid-fire notifications into a single signal.
var dbUpdateChan = make(chan bool, 1)

// upgrader configures the parameters for upgrading an HTTP connection to a WebSocket connection.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true }, // Allow all origins
	// ENABLE COMPRESSION
	EnableCompression: true,
}

// serveWs handles incoming WebSocket connections, creating a Client object for each.
func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request, db *sql.DB) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Failed to upgrade connection:", err)
		return
	}

	client := &Client{
		hub:           hub,
		conn:          conn,
		send:          make(chan []byte, 256),                     // Buffered channel to prevent blocking the hub
		sendPrepared:  make(chan *websocket.PreparedMessage, 256), // Buffered channel for prepared messages
		topics:        make(map[string]bool),
		subscriptions: make(SubscriptionTopics),
	}
	client.hub.register <- client

	// Start the goroutines for reading and writing to this client's connection.
	go client.writePump()
	go client.readPump(db)
}

type clientRequest struct {
	Type         string `json:"type"`
	Subscription struct {
		ID         string `json:"subscriptionId"`
		Collection string `json:"collection"`
		DocID      string `json:"docId,omitempty"`
		// This is the key: we capture the query field as raw bytes.
		// The json library will parse the outer structure but leave this field untouched.
		Query json.RawMessage `json:"query,omitempty"`
	} `json:"subscription"`
}

// Helper function to canonicalizeWhereCondition.
func canonicalizeWhereCondition(whereCondition *Where) (json.RawMessage, error) {
	canonicalBytes, err := json.Marshal(whereCondition)
	if err != nil {
		return nil, fmt.Errorf("failed to create canonical JSON: %w", err)
	}

	return json.RawMessage(canonicalBytes), nil
}

// readPump is now a clean dispatcher. Its only job is to parse incoming
// messages and delegate the work to the appropriate handler.
func (c *Client) readPump(db *sql.DB) {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("readPump error: %v", err)
			}
			break
		}

		var req clientRequest
		if err := json.Unmarshal(message, &req); err != nil {
			log.Printf("Error parsing client message: %v", err)
			continue
		}

		// Construct Subscription object
		sub := Subscription{
			ID:         req.Subscription.ID,
			Collection: req.Subscription.Collection,
			DocID:      req.Subscription.DocID,
		}

		if len(req.Subscription.Query) > 0 && string(req.Subscription.Query) != "null" {
			dsl, err := ParseAndValidateQuery(req.Subscription.Query)
			if err != nil {
				// Send error back to client
				errorPayload, _ := json.Marshal(SubscriptionErrorPayload{
					SubscriptionID: sub.ID,
					Type:           EventTypeSubscriptionError,
					Error:          fmt.Sprintf("Invalid query: %v", err),
				})
				c.send <- errorPayload
				continue
			}
			sub.Query = dsl
			if sub.Query.Where != nil {
				sub.RawWhereCondition, err = canonicalizeWhereCondition(sub.Query.Where)
				if err != nil {
					log.Printf("Error canonicalizing query JSON: %v", err)
					continue
				}
			}
		}

		switch req.Type {
		case "subscribe":
			c.handleSubscribe(db, sub)
		case "unsubscribe":
			c.handleUnsubscribe(sub)
		}
	}
}

// handleSubscribe contains all logic for adding a client to a subscription.
func (c *Client) handleSubscribe(db *sql.DB, sub Subscription) {

	// 1. Calculate the canonical Topic string
	topic, hash, err := c.getTopicAndHash(sub)
	log.Printf("topic: %s, hash: %d\n", topic, hash)
	if err != nil {
		log.Printf("Error getting topic for subscribe: %v", err)
		return
	}

	// 2. Send ACK immediately so client knows mapping: SubID -> Topic
	ack, _ := json.Marshal(SubscribeAckPayload{
		Type:           EventTypeSubscriptionAck, // Define this constant in types.go
		SubscriptionID: sub.ID,
		Topic:          topic,
	})
	c.send <- ack

	c.hub.mu.Lock()
	defer c.hub.mu.Unlock()

	// This is our new error payload for duplicate subscriptions
	sendDuplicateError := func(existingSubID string) {
		errorPayload, _ := json.Marshal(SubscriptionErrorPayload{
			SubscriptionID: sub.ID,
			Type:           EventTypeSubscriptionError,
			Error:          fmt.Sprintf("Client is already subscribed to this feed with subscriptionId '%s'", existingSubID),
		})
		c.send <- errorPayload
	}

	if sub.Query != nil && sub.Query.Where != nil {
		if c.hub.querySubscriptions[sub.Collection] == nil {
			c.hub.querySubscriptions[sub.Collection] = make(QueryClientSubMap)
		}
		if c.hub.querySubscriptions[sub.Collection][hash] == nil {
			c.hub.querySubscriptions[sub.Collection][hash] = make(ClientSubMap)
		}
		subs := c.hub.querySubscriptions[sub.Collection][hash]
		if existingSubID, ok := subs[c]; ok {
			sendDuplicateError(existingSubID)
			return // REJECT
		}
		subs[c] = sub.ID
		if c.hub.activeQueries[sub.Collection] == nil {
			c.hub.activeQueries[sub.Collection] = make(map[uint64]*QueryDSL)
		}
		c.hub.activeQueries[sub.Collection][hash] = sub.Query
	} else if sub.DocID != "" {
		docKey := fmt.Sprintf("%s:%s", sub.Collection, sub.DocID)
		if c.hub.documentSubscriptions[docKey] == nil {
			c.hub.documentSubscriptions[docKey] = make(ClientSubMap)
		}
		subs := c.hub.documentSubscriptions[docKey]
		if existingSubID, ok := subs[c]; ok {
			sendDuplicateError(existingSubID)
			return // REJECT
		}
		subs[c] = sub.ID
	} else {
		if c.hub.collectionSubscriptions[sub.Collection] == nil {
			c.hub.collectionSubscriptions[sub.Collection] = make(ClientSubMap)
		}
		subs := c.hub.collectionSubscriptions[sub.Collection]
		if existingSubID, ok := subs[c]; ok {
			sendDuplicateError(existingSubID)
			return // REJECT
		}
		subs[c] = sub.ID
	}

	c.topics[topic] = true
	c.subscriptions[sub.ID] = topic
	log.Printf("Client %p subscribed to topic: %s", c, topic)
	go c.fetchInitialData(db, sub)
}

// handleUnsubscribe contains all logic for removing a client from a subscription,
// including the crucial cleanup of empty map entries.
func (c *Client) handleUnsubscribe(sub Subscription) {
	topic, hash, err := c.getTopicAndHash(sub)
	if err != nil {
		log.Printf("Error getting topic for unsubscribe: %v", err)
		return
	}

	c.hub.mu.Lock()
	defer c.hub.mu.Unlock()

	// Only proceed if the client was actually subscribed to this topic.
	if !c.topics[topic] {
		return
	}

	if sub.Query != nil && sub.Query.Where != nil {
		if queriesForCollection, ok := c.hub.querySubscriptions[sub.Collection]; ok {
			if subs, ok := queriesForCollection[hash]; ok {
				delete(subs, c)
				if len(subs) == 0 {
					log.Printf("Last client for query hash %d unsubscribed. Cleaning up.", hash)
					delete(queriesForCollection, hash)
					if activeQueriesForColl, ok := c.hub.activeQueries[sub.Collection]; ok {
						delete(activeQueriesForColl, hash)
					}
					if len(queriesForCollection) == 0 {
						delete(c.hub.querySubscriptions, sub.Collection)
					}
					if len(c.hub.activeQueries[sub.Collection]) == 0 {
						delete(c.hub.activeQueries, sub.Collection)
					}
				}
			}
		}
	} else if sub.DocID != "" {
		docKey := fmt.Sprintf("%s:%s", sub.Collection, sub.DocID)
		if subs, ok := c.hub.documentSubscriptions[docKey]; ok {
			delete(subs, c)
			if len(subs) == 0 {
				log.Printf("Last client for document %s unsubscribed. Cleaning up.", topic)
				delete(c.hub.documentSubscriptions, docKey)
			}
		}
	} else {
		if subs, ok := c.hub.collectionSubscriptions[sub.Collection]; ok {
			delete(subs, c)
			if len(subs) == 0 {
				log.Printf("Last client for collection %s unsubscribed. Cleaning up.", topic)
				delete(c.hub.collectionSubscriptions, sub.Collection)
			}
		}
	}

	delete(c.topics, topic) // Remove from the client's local tracking list
	log.Printf("Client %p unsubscribed from topic: %s", c, topic)
}

// getTopicAndHash is a helper that centralizes the logic for creating the canonical
// topic string and hash for any given subscription.
func (c *Client) getTopicAndHash(sub Subscription) (string, uint64, error) {
	var topic string
	var hash uint64 = 0
	var err error

	if sub.Query != nil {
		if sub.Query.Where == nil {
			topic = fmt.Sprintf("coll:%s", sub.Collection)
		} else {
			hash, err = generateQueryHash(sub.RawWhereCondition)
			if err != nil {
				return "", 0, err
			}
			topic = fmt.Sprintf("query:%s:%d", sub.Collection, hash)
		}
	} else if sub.DocID != "" {
		topic = fmt.Sprintf("doc:%s:%s", sub.Collection, sub.DocID)
	} else {
		topic = fmt.Sprintf("coll:%s", sub.Collection)
	}
	return topic, hash, nil
}

// writePump sends messages from the hub to a client's WebSocket connection.
// func (c *Client) writePump() {
// 	defer c.conn.Close()
// 	for message := range c.send {
// 		if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
// 			return
// 		}
// 	}
// }

// writePump sends messages from the hub to a client's WebSocket connection.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		// 1. Handle Raw Bytes (JSON)
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Optimization: Batch write queued messages to reduce syscalls
			n := len(c.send)
			for range n {
				w.Write(asciiRecordSeparator)
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		// 2. Handle Prepared Messages (Zero-Copy Frame)
		case preparedMsg, ok := <-c.sendPrepared:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			// WritePreparedMessage writes the cached frame directly
			if err := c.conn.WritePreparedMessage(preparedMsg); err != nil {
				return
			}

		// 3. Heartbeat
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// unsafeStringToBytes performs a zero-copy conversion from a string to a byte slice.
// CAUTION: The returned byte slice MUST NOT be modified. It shares memory with the string.
func unsafeStringToRawJson(s string) json.RawMessage {
	// unsafe.StringData returns a pointer to the underlying bytes of the string.
	// unsafe.Slice creates a new slice backed by the same memory.
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// fetchInitialData bootstraps a subscription with full server-side protection
// and correctly handles client-provided limit and offset.
func (c *Client) fetchInitialData(db *sql.DB, sub Subscription) {
	var baseQuery string
	var args []any
	var err error

	// 1. Determine the client's desired data window.
	clientLimit := math.MaxInt32 // Default to "no limit".
	clientOffset := 0

	// 2. Prepare Schema Reconstruction Logic
	// We must fetch "id", the "data" blob, and all "native columns" defined in the schema.
	fields := GetCollectionFields(sub.Collection)

	// Build the column list: "id, json(data), col1, col2..."
	selectCols := []string{"id", "json(data)"}
	for _, f := range fields {
		selectCols = append(selectCols, f.Name)
	}
	// "SELECT id, json(data), status, total"
	selectClause := "SELECT " + strings.Join(selectCols, ", ")

	if sub.Query != nil {
		if sub.Query.Limit > 0 {
			clientLimit = sub.Query.Limit
		}
		if sub.Query.Offset > 0 {
			clientOffset = sub.Query.Offset
		}

		// Create a temporary query builder object WITHOUT the pagination fields.
		// This ensures buildQuery creates a "pure" base query we can add our own LIMIT/OFFSET to.
		queryBuilder := &QueryDSL{
			Where:   sub.Query.Where,
			OrderBy: sub.Query.OrderBy,
		}

		// Build the base query using "main." prefix for schema lookup
		baseQuery, args, err = buildQuery("main."+sub.Collection, queryBuilder)

		if err != nil {
			log.Printf("Error building query for sub %s: %v", sub.ID, err)
			errorPayload, _ := json.Marshal(SubscriptionErrorPayload{
				SubscriptionID: sub.ID,
				Type:           EventTypeSubscriptionError,
				Error:          "Failed to build query.",
			})
			c.send <- errorPayload
			return
		}

		// INJECTION: Replace the default "SELECT id, json(data)" with our full column list
		// so we can reconstruct the document.
		baseQuery = strings.Replace(baseQuery, "SELECT id, json(data)", selectClause, 1)

	} else if sub.DocID != "" {
		baseQuery = fmt.Sprintf("%s FROM main.%s WHERE id = ?", selectClause, sub.Collection)
		args = []any{sub.DocID}
	} else {
		baseQuery = fmt.Sprintf("%s FROM main.%s", selectClause, sub.Collection)
	}

	baseQuery = strings.TrimSuffix(baseQuery, ";")

	const batchSize = 100
	const batchInterval = 50 * time.Millisecond
	totalItemsFetched := 0

	for totalItemsFetched < clientLimit {
		itemsRemaining := clientLimit - totalItemsFetched
		currentBatchSize := batchSize
		if itemsRemaining < batchSize {
			currentBatchSize = itemsRemaining
		}

		currentOffset := clientOffset + totalItemsFetched

		paginatedQuery := fmt.Sprintf("%s LIMIT ? OFFSET ?;", baseQuery)
		queryArgs := append(args, currentBatchSize, currentOffset)

		rows, err := dbQuery(db, paginatedQuery, queryArgs...)
		if err != nil {
			log.Printf("Error fetching batch for sub %s: %v", sub.ID, err)
			break
		}

		// Prepare Scan Destinations (Reuse per batch)
		// [0]=id, [1]=dataBlob, [2...N]=columns
		scanDest := make([]any, len(selectCols))
		var id string
		var rawBlob []byte
		scanDest[0] = &id
		scanDest[1] = &rawBlob

		// Interface pointers for dynamic columns
		colValues := make([]any, len(fields))
		for i := range fields {
			scanDest[i+2] = &colValues[i]
		}

		batch := make([]Document, 0, currentBatchSize)
		rowsInBatch := 0

		for rows.Next() {
			rowsInBatch++

			if err := rows.Scan(scanDest...); err != nil {
				log.Printf("Error scanning initial data row: %v", err)
				continue
			}

			// RECONSTRUCTION: Merge columns back into JSON
			finalJSON, err := constructDocumentJSON(rawBlob, fields, colValues)
			if err != nil {
				log.Printf("Error constructing document JSON: %v", err)
				continue
			}

			doc := Document{
				ID:   id,
				Data: json.RawMessage(finalJSON),
			}
			batch = append(batch, doc)
		}
		rows.Close()

		if rowsInBatch > 0 {
			c.sendBatch(sub, batch)
			totalItemsFetched += rowsInBatch
			if rowsInBatch == currentBatchSize {
				time.Sleep(batchInterval)
			}
		}

		if rowsInBatch < currentBatchSize {
			break
		}
	}

	completionPayload, _ := json.Marshal(SubscriptionCompletionPayload{
		SubscriptionID: sub.ID,
		Type:           EventTypeInitialDataComplete,
		Collection:     sub.Collection,
	})

	c.send <- completionPayload
}

// sendBatch is now updated to use the high-performance struct.
func (c *Client) sendBatch(sub Subscription, batch []Document) {
	// Create an instance of our payload struct. This is extremely fast.
	payload := InitialDataBatchPayload{
		SubscriptionID: sub.ID,
		Type:           EventTypeInitialDataBatch,
		Collection:     sub.Collection,
		Documents:      batch,
	}

	// Marshal the struct. This will be much faster than marshaling a map.
	payloadBytes, _ := json.Marshal(payload)
	c.send <- payloadBytes
}
