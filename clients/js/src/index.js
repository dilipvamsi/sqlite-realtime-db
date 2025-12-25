// index.js

/**
 * =============================================================================
 * REAL-TIME SQLITE CLIENT LIBRARY
 * =============================================================================
 *
 * A lightweight, dependency-free client for interacting with the Real-Time SQLite
 * server. It handles REST API operations, WebSocket connection lifecycle management,
 * automatic reconnection, and state synchronization.
 *
 * Key Features:
 * 1. **Hybrid Data Structure**: Maintains both Array (for order) and Map (for O(1) lookup).
 * 2. **Reference Stability**: Mutates objects in-place to ensure UI frameworks (React/Vue)
 *    maintain references to items across updates.
 * 3. **Race-Condition Protection**: Prevents duplicate subscription payloads using socket tagging.
 * 4. **Zombie Connection Detection**: Uses side-channel HTTP health checks to detect dead sockets.
 */

(function (root) {
  // Use 'strict mode' inside the closure for better error catching
  "use strict";

  /**
   * A simple, lightweight event emitter for managing internal callback lists.
   * It follows the standard Node.js EventEmitter pattern but is built for the browser/client-side
   * to avoid polyfill dependencies.
   *
   * @internal Used internally to decouple the global WebSocket message stream from specific
   *           subscription instances.
   */
  class EventEmitter {
    constructor() {
      /**
       * Storage for event listeners.
       * Keys are event names, values are arrays of callback functions.
       * @type {Record<string, Array<Function>>}
       */
      this.events = {};
    }

    /**
     * Registers a listener callback for a specific named event.
     *
     * @param {string} eventName - The name of the event (e.g., 'connect', 'message', 'subscription:123').
     * @param {Function} listener - The callback function to execute when the event is emitted.
     */
    on(eventName, listener) {
      if (!this.events[eventName]) {
        this.events[eventName] = [];
      }
      this.events[eventName].push(listener);
    }

    /**
     * Unregisters a specific listener callback for a given event.
     * Note: You must pass the exact function reference that was used to subscribe.
     *
     * @param {string} eventName - The name of the event.
     * @param {Function} listenerToRemove - The specific callback function reference to remove.
     */
    off(eventName, listenerToRemove) {
      if (!this.events[eventName]) return;
      this.events[eventName] = this.events[eventName].filter(
        (l) => l !== listenerToRemove
      );
    }

    /**
     * Synchronously calls each of the listeners registered for the event.
     *
     * @param {string} eventName - The name of the event to emit.
     * @param {any} [data] - Optional data payload to pass to the listeners.
     */
    emit(eventName, data) {
      if (!this.events[eventName]) return;
      // We create a shallow copy ([...]) to allow listeners to safely unsubscribe
      // themselves during the iteration without breaking the loop index.
      [...this.events[eventName]].forEach((l) => l(data));
    }

    /**
     * Removes all listeners for a specific event.
     * Used for cleanup when a subscription is explicitly destroyed.
     *
     * @param {string} eventName - The name of the event to clear.
     */
    removeAllListeners(eventName) {
      if (this.events[eventName]) {
        delete this.events[eventName];
      }
    }
  }

  /**
   * A handle to an active real-time subscription.
   *
   * This class is returned when calling `.subscribe()` on the client. It acts as the
   * bridge between the user's application logic and the raw event stream coming from
   * the WebSocket. It contains the logic to "materialize" a stream of diffs into
   * a coherent current state.
   */
  class Subscription {
    /**
     * Creates a new Subscription instance.
     * @param {RealTimeSQLite} client - The main client instance.
     * @param {string} id - The unique generated ID associated with this subscription.
     */
    constructor(client, id) {
      this.client = client;
      this.id = id;
    }

    /**
     * Registers a callback for **raw** messages on this subscription.
     * This includes initial data batches, completion signals, errors, and single update events.
     * Useful for debugging or custom state management implementation.
     *
     * @param {Function} callback - Function receiving the raw `ServerMessage` object.
     * @returns {this} The Subscription instance for method chaining.
     */
    on(callback) {
      this.client.eventEmitter.on(`subscription:${this.id}`, callback);
      return this;
    }

    /**
     * Unregisters a specific callback from this subscription.
     *
     * @param {Function} callback - The callback function to remove.
     * @returns {this} The Subscription instance for method chaining.
     */
    off(callback) {
      this.client.eventEmitter.off(`subscription:${this.id}`, callback);
      return this;
    }

    /**
     * Registers a callback specifically for subscription errors.
     * Errors typically occur if a duplicate subscription ID is sent or the query syntax is invalid.
     *
     * @param {Function} callback - Function receiving the error message string.
     * @returns {this} The Subscription instance for method chaining.
     */
    onError(callback) {
      this.on((msg) => {
        if (msg.type === "subscription_error") {
          callback(msg.error);
        }
      });
      return this;
    }

    /**
     * A helper method that maintains a synchronized local cache of the data.
     *
     * **Architectural Details:**
     * 1. **Hybrid Data Structure**: It maintains both a `Map` (for O(1) access by ID) and
     *    an `Array` (to preserve the order returned by the DB).
     * 2. **Reference Stability**: When an `UPDATE` event arrives, it uses `Object.assign` to mutate
     *    the existing object in memory. This ensures that if a UI framework (like React) is holding
     *    a reference to an item in the list, that item updates automatically without needing a deep clone.
     * 3. **System ID Injection**: It injects `__id` into every object. It **never** touches or
     *    polyfills the `id` field, ensuring the user's data schema remains pure.
     *
     * @param {Function} callback - Called whenever the local data list changes.
     *                              Signature: `(list, map, isLoading)`
     * @returns {Function} A cleanup function that removes the listener (does not unsubscribe from server).
     */
    subscribeToData(callback) {
      // 1. The Map provides O(1) access for checking existence and updates.
      const lookup = new Map();
      // 2. The Array provides the specific DB sort order.
      const list = [];

      let isInitialLoad = true;

      // Helper: Safely prepares the object with __id without touching the user's 'id'.
      const prepareDoc = (docId, data) => {
        // We mutate the data object to inject __id (System Key)
        // We do NOT inject 'id'. If user data has 'id', it stays. If not, it stays undefined.
        if (!data.__id) data.__id = docId;
        return data;
      };

      const messageHandler = (msg) => {
        switch (msg.type) {
          // CASE 1: Initial Snapshot (Chunk)
          // The server sends existing data in batches (e.g., 100 items at a time).
          case "initial_data_batch":
            if (Array.isArray(msg.documents)) {
              msg.documents.forEach((doc) => {
                // Prevent duplicates if multiple batches overlap (rare but safe).
                if (lookup.has(doc.id)) return;

                const item = prepareDoc(doc.id, doc.data);

                // Add to BOTH structures to keep them in sync.
                lookup.set(doc.id, item);
                list.push(item);
              });
            }
            break;

          // CASE 2: Snapshot Finished
          // All existing data has been downloaded.
          case "initial_data_complete":
            isInitialLoad = false;
            callback(list, lookup, false);
            break;

          // CASE 3: Live Event
          // A Change-Data-Capture (CDC) event occurred in the database.
          case "realtime_update":
            // Defensive check against malformed payloads
            if (!msg.payload) return;

            const { operation, docId, data } = msg.payload;

            // --- INSERT / UPDATE Logic ---
            if (operation === "INSERT" || operation === "UPDATE") {
              if (data && docId) {
                const existingItem = lookup.get(docId);

                if (existingItem) {
                  // ⚡️ REFERENCE PRESERVATION ⚡️
                  // We do NOT replace the object. We mutate its properties.
                  // The 'list' array automatically sees these changes because it holds the reference.
                  Object.assign(existingItem, data);

                  // Re-inject __id in case the incoming update 'data' didn't have it (it usually doesn't).
                  if (!existingItem.__id) existingItem.__id = docId;
                } else {
                  // It's a new item (INSERT) or an item that just entered the query window.
                  // Create it, add to Map, add to end of List.
                  const newItem = prepareDoc(docId, data);
                  lookup.set(docId, newItem);
                  list.push(newItem);
                }
              }
            }
            // --- DELETE / REMOVE Logic ---
            else if (operation === "DELETE" || operation === "REMOVE") {
              if (docId && lookup.has(docId)) {
                // 1. Remove from Map (O(1))
                lookup.delete(docId);

                // 2. Remove from List (O(N))
                // Unavoidable O(N) because we must find the index to splice, preserving order.
                const index = list.findIndex((item) => item.__id === docId);
                if (index !== -1) {
                  list.splice(index, 1);
                }
              }
            }

            // Only trigger UI updates if the initial heavy load is finished.
            if (!isInitialLoad) {
              callback(list, lookup, false);
            }
            break;

          case "subscription_error":
            console.error(`[Sub Error ${this.id}]`, msg.error);
            break;
        }
      };

      // Attach the handler
      this.on(messageHandler);

      // Fire the callback immediately with empty structures to indicate "Loading Started"
      callback(list, lookup, true);

      // Return the cleanup closure
      return () => this.off(messageHandler);
    }

    /**
     * Unsubscribes from the real-time feed on the server.
     * This sends an 'unsubscribe' message to the WebSocket server and clears local listeners.
     */
    unsubscribe() {
      this.client.unsubscribe(this.id);
    }
  }

  /**
   * The main client for interacting with the Real-Time SQLite server.
   *
   * It acts as a singleton-like manager for:
   * 1. The HTTP REST API (CRUD operations).
   * 2. The WebSocket connection (Real-time events).
   * 3. Subscription multiplexing (Handling multiple feeds over one socket).
   * 4. Connection health and automatic recovery.
   */
  class RealTimeSQLite {
    /**
     * Creates a new client instance.
     * @param {string} baseURL - The root URL of the server (e.g., 'http://localhost:8080').
     */
    constructor(baseURL) {
      this.websocket = null;

      /**
       * Maps subscription IDs to their definition (payload) and connection state.
       * Used for auto-reconnection and race-condition handling.
       *
       * Structure:
       * Map<id, {
       *   payload: object,      // The JSON payload sent to server
       *   sub: Subscription,    // The Subscription instance
       *   socketId: WebSocket   // The specific socket instance this was sent to
       * }>
       *
       * @type {Map<string, { payload: object, sub: Subscription, socketId: WebSocket | null }>}
       */
      this.subscriptions = new Map();
      /**
       * @type {Map<string, Set<string>>}
       */
      this.topicSubscriptions = new Map();

      this.eventEmitter = new EventEmitter();
      this.shouldBeConnected = false;
      this.reconnectAttempts = 0;

      // Normalize URL (strip trailing slash)
      this.baseURL = baseURL.replace(/\/$/, "");
      this.dbURL = `${this.baseURL}/db`;

      // Infer WebSocket URL based on protocol (http -> ws, https -> wss)
      const wsProtocol = this.baseURL.startsWith("https") ? "wss" : "ws";
      const wsPath = this.baseURL.replace(/^https?:\/\//, "");
      this.wsURL = `${wsProtocol}://${wsPath}/ws`;

      // Attempt to auto-detect WebSocket in Node.js environments
      // This allows the client to work in Node without forcing the user to pass the constructor.
      if (typeof WebSocket === "undefined" && typeof require !== "undefined") {
        try {
          global.WebSocket = require("ws");
        } catch (e) {
          // Silent fail; user must install 'ws' manually if they want Node support.
        }
      }
    }

    /**
     * Internal helper for performing standardized fetch requests.
     * Handles error parsing and JSON conversion.
     * @private
     */
    async _fetch(endpoint, options = {}, isDbRoute = true) {
      const url = `${isDbRoute ? this.dbURL : this.baseURL}${endpoint}`;
      const headers = {
        "Content-Type": "application/json",
        ...options.headers,
      };

      try {
        const res = await fetch(url, { ...options, headers });
        if (!res.ok) {
          const txt = await res.text();
          throw new Error(`API Error ${res.status}: ${txt}`);
        }
        // Return null for 204 No Content, otherwise parse JSON
        return res.status !== 204 ? res.json() : null;
      } catch (error) {
        console.error(`HTTP Request Failed: ${url}`, error.message);
        throw error;
      }
    }

    // ===========================================================================
    // REST API WRAPPERS
    // ===========================================================================

    /** Checks server health. */
    getHealth() {
      return this._fetch("/health", {}, false);
    }
    /** Lists all collections. */
    getCollections() {
      return this._fetch("/collections");
    }
    /** Creates a new collection. */
    createCollection(name, schema) {
      const body = {};
      // Pass the schema if provided (e.g. [{name: 'status', type: 'text'}])
      if (schema) {
        body.schema = schema;
      }
      return this._fetch(`/collections/${name}`, {
        method: "POST",
        body: JSON.stringify(body),
      });
    }
    /** Update schema collection. */
    updateCollectionSchema(name, update) {
      return this._fetch(`/collections/${name}`, {
        method: "PATCH",
        body: JSON.stringify(update),
      });
    }
    /** Deletes a collection. */
    deleteCollection(name) {
      return this._fetch(`/collections/${name}`, { method: "DELETE" });
    }
    /** Creates an index. */
    createIndex(collectionName, options) {
      return this._fetch(`/indexes/${collectionName}`, {
        method: "POST",
        body: JSON.stringify(options),
      });
    }
    /** Gets a single document. */
    getDocument(collection, docId) {
      return this._fetch(`/data/${collection}/${docId}`);
    }
    /** Upserts a document (Full Replace). */
    setDocument(collection, docId, data) {
      return this._fetch(`/data/${collection}/${docId}`, {
        method: "PUT",
        body: JSON.stringify(data),
      });
    }
    /** Updates a document (JSON Patch). */
    updateDocument(collection, docId, patch) {
      return this._fetch(`/data/${collection}/${docId}`, {
        method: "PATCH",
        body: JSON.stringify(patch),
      });
    }
    /** Deletes a document. */
    deleteDocument(collection, docId) {
      return this._fetch(`/data/${collection}/${docId}`, { method: "DELETE" });
    }
    /** Executes a query. */
    queryCollection(collection, dsl) {
      return this._fetch(`/query/${collection}`, {
        method: "POST",
        body: JSON.stringify(dsl),
      });
    }

    // ===========================================================================
    // WEBSOCKET CONNECTION LOGIC
    // ===========================================================================

    /**
     * Internal method to establish WebSocket connection.
     * Contains logic to handle race conditions during subscription.
     * @private
     */
    _connect() {
      // 0 = CONNECTING, 1 = OPEN. If active, don't create new one.
      if (this.websocket && this.websocket.readyState < 2) return;

      this.shouldBeConnected = true;

      if (typeof WebSocket === "undefined") {
        console.error(
          "❌ WebSocket is NOT defined. If running in Node.js, install 'ws': npm install ws"
        );
        return;
      }

      this.websocket = new WebSocket(this.wsURL);

      this.websocket.onopen = () => {
        this.reconnectAttempts = 0;
        this.eventEmitter.emit("connect");

        // AUTO-SUBSCRIBE / RECONNECT LOGIC
        // Iterate over all registered subscriptions and send them to the server.
        this.subscriptions.forEach((subInfo) => {
          // FIX: RACE CONDITION PREVENTION
          // If we already sent this subscription to *this exact socket instance* (via the subscribe method),
          // then do NOT send it again here. This prevents the "Client already subscribed" error.
          // If 'socketId' differs (e.g., this is a new re-connection socket), we DO send.
          if (subInfo.socketId === this.websocket) return;

          this.websocket.send(JSON.stringify(subInfo.payload));
          subInfo.socketId = this.websocket; // Mark as synced
        });
      };

      this.websocket.onclose = () => {
        const wasConnected = this.shouldBeConnected;
        this.websocket = null;
        this.eventEmitter.emit("disconnect");
        // Clear topic map on disconnect as server will re-assign on reconnect
        this.topicSubscriptions.clear();
        // Trigger reconnect only if we didn't explicitly call .disconnect()
        if (wasConnected) this._handleReconnect();
      };

      this.websocket.onerror = (e) => this.eventEmitter.emit("error", e);

      this.websocket.onmessage = (e) => {
        // The server batches messages separated by newlines (\n).
        // We must split them to parse individual JSON objects.
        const messages = e.data.split('\n');
        for (const raw of messages) {
          if (!raw.trim()) continue;
          try {
            const msg = JSON.parse(raw);
            this.eventEmitter.emit("message", msg);

            // 1. Handle Subscription ACK (Map Topic -> SubIDs)
            if (msg.type === "subscribe_ack") {
              const { subscriptionId, topic } = msg;
              if (!this.topicSubscriptions.has(topic)) {
                this.topicSubscriptions.set(topic, new Set());
              }
              this.topicSubscriptions.get(topic).add(subscriptionId);
              continue; // ACK handled
            }

            // 2. Handle RealTime Updates (Fan-out by Topic)
            if (msg.type === "realtime_update") {
              const topic = msg.topic;
              const subIds = this.topicSubscriptions.get(topic);
              if (subIds) {
                subIds.forEach(id => {
                  // Inject SubID locally for the Subscription handler
                  // We must clone msg to avoid modifying it for other listeners if we mutated deeply (we don't here)
                  // but a shallow copy is safer.
                  const localMsg = { ...msg, subscriptionId: id };
                  this.eventEmitter.emit(`subscription:${id}`, localMsg);
                });
              }
              continue;
            }

            // 3. Handle Initial Data / Errors (Direct routing via subscriptionId)
            if (msg.subscriptionId) {
              this.eventEmitter.emit(`subscription:${msg.subscriptionId}`, msg);
            }
          } catch (err) { console.error("WS Parse Error", err); }
        }
      };
    }

    /**
     * Handles exponential backoff for reconnection.
     * Algorithm: 1s, 2s, 4s, 8s, 16s, capped at 30s.
     * @private
     */
    _handleReconnect() {
      if (!this.shouldBeConnected) return;
      this.reconnectAttempts++;
      const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
      setTimeout(() => {
        if (this.shouldBeConnected) this._connect();
      }, delay);
    }

    /**
     * Subscribes to a live query, document, or collection.
     * @param {object} options
     * @returns {Subscription}
     */
    subscribe({ collection, docId, query }) {
      const subscriptionId = `${collection}_${Date.now()}_${Math.random()
        .toString(36)
        .slice(2, 11)}`;
      const payload = {
        type: "subscribe",
        subscription: { subscriptionId, collection, docId, query },
      };

      // 1. Register locally with 'socketId: null' to mark it as pending for this connection.
      const subInfo = {
        payload,
        sub: new Subscription(this, subscriptionId),
        socketId: null,
      };
      this.subscriptions.set(subscriptionId, subInfo);

      // 2. Ensure connection process starts
      this._connect();

      // 3. RACE CONDITION PREVENTION
      // If socket is ALREADY OPEN, send immediately to reduce latency.
      // If socket is CONNECTING, do nothing (wait for onopen to handle it).
      if (this.websocket && this.websocket.readyState === 1) {
        this.websocket.send(JSON.stringify(payload));
        subInfo.socketId = this.websocket; // Mark as sent to this socket
      }

      return subInfo.sub;
    }

    /**
     * Cancels a subscription.
     * @param {string} subscriptionId
     */
    unsubscribe(subscriptionId) {
      const subInfo = this.subscriptions.get(subscriptionId);
      if (!subInfo) return;

      // Clean up Topic Map
      // We iterate because we don't store SubID->Topic mapping directly in this simple impl.
      // Optimally, Subscription class could know its topic.
      for (const [topic, subscriptionSet] of this.topicSubscriptions.entries()) {
        if (subscriptionSet.delete(subscriptionId)) {
          if (subscriptionSet.size === 0) {
            this.topicSubscriptions.delete(topic);
          }
          break;
        }
      }

      const payload = {
        type: "unsubscribe",
        subscription: subInfo.payload.subscription,
      };

      if (this.websocket && this.websocket.readyState === 1) {
        this.websocket.send(JSON.stringify(payload));
      }

      this.subscriptions.delete(subscriptionId);
      this.eventEmitter.removeAllListeners(`subscription:${subscriptionId}`);
    }

    /**
     * Closes connection and stops auto-reconnect.
     */
    disconnect() {
      this.shouldBeConnected = false;
      this.stopHealthMonitor();
      if (this.websocket) this.websocket.close();
    }

    // ===========================================================================
    // HEALTH MONITOR
    // ===========================================================================

    /**
     * Starts a background task to poll the health endpoint.
     * This is used to detect "Zombie" WebSocket connections where the socket
     * thinks it's open, but the network path is actually broken.
     *
     * @param {number} [intervalMs=5000]
     */
    startHealthMonitor(intervalMs = 5000) {
      if (this.healthCheckIntervalId) return;
      this.healthCheckIntervalId = setInterval(async () => {
        if (!this.shouldBeConnected) return;
        try {
          await this.getHealth();
          // If HTTP is up but WS is closed/closing, force reconnect logic
          if (this.websocket && this.websocket.readyState === 3)
            this._handleReconnect();
        } catch (error) {
          // If HTTP fails, assume network down. Close WS to trigger standard reconnect.
          if (this.websocket) this.websocket.close();
          else this._handleReconnect();
        }
      }, intervalMs);
    }

    /** Stops the background health polling. */
    stopHealthMonitor() {
      if (this.healthCheckIntervalId) {
        clearInterval(this.healthCheckIntervalId);
        this.healthCheckIntervalId = null;
      }
    }
  }

  // Attach helper classes to the main class for static access if needed
  RealTimeSQLite.Subscription = Subscription;
  RealTimeSQLite.EventEmitter = EventEmitter;

  // =============================================================================
  // UNIVERSAL EXPORT
  // =============================================================================

  // 1. Node.js / CommonJS
  if (typeof module !== "undefined" && module.exports) {
    module.exports = { RealTimeSQLite, Subscription, EventEmitter };
  }
  // 2. Browser Global (Namespaced)
  else if (typeof root !== "undefined") {
    root.RealTimeSQLite = RealTimeSQLite;
  }
})(typeof window !== "undefined" ? window : this);
