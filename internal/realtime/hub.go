// hub.go
package realtime

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	json "github.com/goccy/go-json"

	"github.com/gorilla/websocket"
)

// A map of clients to their SINGLE subscription ID for this feed.
type ClientSubMap map[*Client]string

// A map of query hashes to the map of clients subscribed to that query.
type QueryClientSubMap map[uint64]ClientSubMap
type HashQueryMap map[uint64]*QueryDSL

// Hub uses three distinct, specialized maps for the highest subscription performance.
// This is the Hybrid Map Model, providing optimal lookups for each listener type.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan *RealTimeEvent
	register   chan *Client
	unregister chan *Client

	// Map for collection-wide listeners. Key: collection name (e.g., "posts").
	collectionSubscriptions map[string]ClientSubMap
	// Map for single-document listeners. Key: "collection:docId" (e.g., "posts:doc123").
	documentSubscriptions map[string]ClientSubMap
	// Nested map for query listeners, scoped by collection for performance.
	// Key: collection -> queryHash -> ClientSubSet.
	querySubscriptions map[string]QueryClientSubMap
	// Map to store the definitions of active queries, used for in-memory evaluation.
	activeQueries map[string]HashQueryMap
	mu            sync.RWMutex
}

// Client represents a connected WebSocket client and its set of subscriptions.
type Client struct {
	hub          *Hub
	conn         *websocket.Conn
	send         chan []byte
	sendPrepared chan *websocket.PreparedMessage
	// A set of topic strings (e.g., "coll:posts", "doc:posts:doc123") for efficient cleanup.
	topics map[string]bool
}

// A helper struct for the dispatch list inside the run loop
type dispatch struct {
	client *Client
	subID  string
	event  *RealTimeEvent
}

// DispatchListPool reuses slices to prevent allocating new arrays for every broadcast.
var DispatchListPool = sync.Pool{
	New: func() any {
		// Start with a reasonable capacity (e.g., 64 clients) to minimize growing
		return make([]dispatch, 0, 64)
	},
}

// newHub creates and initializes a new Hub instance.
func newHub() *Hub {
	return &Hub{
		clients:                 make(map[*Client]bool),
		broadcast:               make(chan *RealTimeEvent),
		register:                make(chan *Client),
		unregister:              make(chan *Client),
		collectionSubscriptions: make(map[string]ClientSubMap),
		documentSubscriptions:   make(map[string]ClientSubMap),
		querySubscriptions:      make(map[string]QueryClientSubMap),
		activeQueries:           make(map[string]HashQueryMap),
	}
}

// run is the hub's main event loop. It listens on its channels and routes events.
// This is the "hot path" of the real-time system, optimized for speed.
func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("Client connected. Total clients: %d", len(h.clients))
		case client := <-h.unregister:
			h.cleanupClient(client)
			log.Printf("Client disconnected. Total clients: %d", len(h.clients))
		case event := <-h.broadcast:
			// Track events that need to be freed after broadcast
			eventsToFree := []*RealTimeEvent{event}
			var removeEvent *RealTimeEvent = nil
			// OPTIMIZATION: Get slice from Pool
			dispatchList := DispatchListPool.Get().([]dispatch)
			// Reset length to 0, keep capacity
			dispatchList = dispatchList[:0]

			appendToDispatch := func(event *RealTimeEvent, subs ClientSubMap) {
				for client, subID := range subs {
					dispatchList = append(dispatchList, dispatch{client, subID, event})
				}
			}

			h.mu.RLock() // Use a read lock for the broadcast operation

			// 1. O(1) Lookup for Collection Listeners (fastest, no hashing)
			if subs, ok := h.collectionSubscriptions[event.Collection]; ok {
				appendToDispatch(event, subs)
			}

			documentKey := fmt.Sprintf("%s:%s", event.Collection, event.DocumentID)
			// 2. O(1) Lookup for Document Listeners (fastest, no hashing)
			if subs, ok := h.documentSubscriptions[documentKey]; ok {
				appendToDispatch(event, subs)
			}

			// 3. Optimized Path for Query Listeners (scoped, hashes only for queries)
			// First, check if there are ANY query subscriptions for this collection at all.
			if queriesForCollection, ok := h.querySubscriptions[event.Collection]; ok {
				activeQueriesForCollection := h.activeQueries[event.Collection]

				// Iterate over the small set of unique active queries for THIS collection only.
				for queryHash, queryDSL := range activeQueriesForCollection {
					if match, _ := evaluateWhere(event.Data, queryDSL.Where); match {
						// The lookup is now nested and scoped to the collection.
						if subs, ok := queriesForCollection[queryHash]; ok {
							appendToDispatch(event, subs)
						}
					} else if event.OldData != nil && (event.Operation == OperationUpdate || event.Operation == OperationDelete) {
						if oldMatch, _ := evaluateWhere(event.OldData, queryDSL.Where); oldMatch {
							if subs, ok := queriesForCollection[queryHash]; ok {
								if removeEvent == nil {
									removeEvent = fillRemoveEvent(event)
									eventsToFree = append(eventsToFree, removeEvent)
								}
								appendToDispatch(removeEvent, subs)
							}
						}
					}
				}
			}
			h.mu.RUnlock()

			// 4. Send the event to the final, de-duplicated set of clients.
			dispatchEventsToClients(dispatchList)
			freeDispatchList(dispatchList)
			freeEventsList(eventsToFree)
		}
	}
}

func fillRemoveEvent(event *RealTimeEvent) *RealTimeEvent {
	operation := OperationRemove
	if event.Operation == OperationDelete {
		operation = OperationDelete
	}
	// OPTIMIZATION: Get synthetic event from Pool
	removeEvent := EventPool.Get().(*RealTimeEvent)
	removeEvent.Operation = operation
	removeEvent.Collection = event.Collection
	removeEvent.DocumentID = event.DocumentID
	removeEvent.Data = event.Data
	removeEvent.OldData = event.OldData

	return removeEvent
}

func dispatchEventsToClients(dispatchList []dispatch) {
	if len(dispatchList) > 0 {

		// OPTIMIZATION: Event Cache
		// We map the *Pointer* of the event to its marshaled JSON bytes.
		// This handles the Main Event AND any shared Synthetic Events (e.g., 50 clients
		// getting the exact same REMOVE event) efficiently.
		// We typically expect 1 or 2 unique events per batch, so we init with small capacity.
		eventCache := make(map[*RealTimeEvent]json.RawMessage, 2)

		// Pre-seed the main event if desired, or let the loop handle it lazily.
		// Doing it lazily simplifies the loop logic.
		for _, dispatch := range dispatchList {
			// 1. Resolve Event Payload (Lazy Loading / Caching)
			eventPayload, exists := eventCache[dispatch.event]
			if !exists {
				// This is the first time we've seen this specific event object in this batch.
				// Marshal it and cache the result.
				eventBytes, err := json.Marshal(dispatch.event)
				if err != nil {
					log.Printf("Error marshaling event for dispatch: %v", err)
					continue
				}
				eventPayload = json.RawMessage(eventBytes) // Zero-copy cast
				eventCache[dispatch.event] = eventPayload
			}

			// 2. Wrap in the Update Payload
			// We use the cached RawMessage, so this Marshal only has to write
			// the SubscriptionID and Type strings, then append the pre-calculated bytes.
			finalPayload, err := json.Marshal(CachedRealTimeUpdatePayload{
				Type:           EventTypeRealTimeUpdate,
				SubscriptionID: dispatch.subID,
				Event:          eventPayload,
			})
			if err != nil {
				log.Printf("Error marshaling final payload: %v", err)
				continue
			}

			// 3. Non-blocking Send
			select {
			case dispatch.client.send <- finalPayload:
			default:
				log.Printf("Client send buffer full, dropping message for client %p", &dispatch.client)
			}
		}
	} else {
		// log.Println("No Clients")
	}
}

// OPTIMIZATION: Clean up and return events to pool
func freeEventsList(eventsToFree []*RealTimeEvent) {
	for _, e := range eventsToFree {
		// 1. Release backing arrays for JSON buffers (CRITICAL for memory)
		e.Data = nil
		e.OldData = nil

		// 2. Release string references (CRITICAL for GC hygiene)
		e.Collection = ""
		e.DocumentID = ""
		e.Operation = ""
		EventPool.Put(e)
	}
}

// OPTIMIZATION: Zero out references to prevent memory leaks
// The backing array holds these structs even after [:0].
// If we don't clear them, the GC cannot collect the Clients or Events.
func freeDispatchList(dispatchList []dispatch) {
	for i := range dispatchList {
		dispatchList[i].client = nil
		dispatchList[i].event = nil
		dispatchList[i].subID = ""
	}

	// Reset length to 0, keep capacity
	dispatchList = dispatchList[:0]

	// Return clean slice to pool
	DispatchListPool.Put(dispatchList)
}

// cleanupClient ensures a disconnected client is removed from all subscription maps to prevent memory leaks.
func (h *Hub) cleanupClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[client]; ok {
		for topic := range client.topics {
			parts := strings.SplitN(topic, ":", 3)
			topicType := parts[0]
			collection := parts[1]

			switch topicType {
			case "coll":
				if subs, ok := h.collectionSubscriptions[collection]; ok {
					delete(subs, client)
					if len(subs) == 0 {
						delete(h.collectionSubscriptions, collection)
					}
				}
			case "doc":
				key := fmt.Sprintf("%s:%s", collection, parts[2])
				if subs, ok := h.documentSubscriptions[key]; ok {
					delete(subs, client)
					if len(subs) == 0 {
						delete(h.documentSubscriptions, key)
					}
				}
			case "query":
				hash, _ := strconv.ParseUint(parts[2], 10, 64)
				if queriesForCollection, ok := h.querySubscriptions[collection]; ok {
					if subs, ok := queriesForCollection[hash]; ok {
						delete(subs, client)
						if len(subs) == 0 {
							// If this was the last client for this query, clean up the active query definition as well.
							delete(queriesForCollection, hash)
							if len(queriesForCollection) == 0 {
								delete(h.querySubscriptions, collection)
								delete(h.activeQueries, collection)
							}
						}
					}
				}
			}
		}
		delete(h.clients, client)
		close(client.send)
	}
}

func (h *Hub) BroadcastSystemMessage(msg []byte) {
	// 1. Prepare ONCE
	pm, _ := websocket.NewPreparedMessage(websocket.TextMessage, msg)

	// 2. Send to ALL clients zero-copy
	h.mu.RLock()
	for client := range h.clients {
		select {
		case client.sendPrepared <- pm:
		default:
		}
	}
	h.mu.RUnlock()
}
