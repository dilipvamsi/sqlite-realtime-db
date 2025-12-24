// index.d.ts

/**
 * Validates the global variable 'RealTimeSQLite' for UMD/Script-tag usage.
 */
export as namespace RealTimeSQLite;

// =============================================================================
// 1. CORE TYPES
// =============================================================================

/**
 * Defines the type of database mutation that occurred.
 * - `INSERT`: A new document was added to the collection.
 * - `UPDATE`: An existing document was modified.
 * - `DELETE`: A document was permanently removed from the database.
 * - `REMOVE`: A document was updated such that it **no longer matches** the current query filter,
 *            though it still exists in the database.
 */
export type OperationType = "INSERT" | "UPDATE" | "DELETE" | "REMOVE";

/**
 * Defines the type for server-to-client WebSocket messages.
 * - `initial_data_batch`: A chunk of existing documents sent upon subscription.
 * - `initial_data_complete`: Sent when all existing documents have been sent.
 * - `realtime_update`: A live event (CUD) occurring after the snapshot.
 * - `subscription_error`: Sent if the subscription request was malformed or duplicates an existing one.
 */
export type EventType =
  | "realtime_update"
  | "initial_data_batch"
  | "initial_data_complete"
  | "subscription_error";

/**
 * Represents a document wrapper as returned by the server API.
 * The server wraps all document results in this structure containing ID and Data.
 * @template T - The shape of the actual data object stored.
 */
export interface Document<T = any> {
  /** The unique string ID of the document. */
  id: string;
  /** The actual JSON content of the document. */
  data: T;
}

/**
 * Represents a document stored locally in the client cache.
 * It is the intersection of the user's data type `T` and the system ID.
 *
 * It strictly guarantees the presence of `__id` (the system ID),
 * allowing you to access the document key safely even if your data type `T`
 * does not define an id field.
 *
 * @template T - The shape of the user data.
 */
export type LocalDocument<T = any> = T & { __id: string };

/**
 * Represents a live change event from the database.
 * @template T - The shape of the document data.
 */
export interface RealTimeEvent<T = any> {
  /** The type of operation (INSERT, UPDATE, DELETE, REMOVE). */
  operation: OperationType;
  /** The name of the collection. */
  collection: string;
  /** The ID of the affected document. */
  docId: string;
  /** The new state of the document (present on INSERT and UPDATE). */
  data?: T;
  /** The previous state of the document (present on UPDATE and DELETE). */
  oldData?: T;
}

/**
 * Union of all possible messages received from the server.
 * @template T - The shape of the document data.
 */
export interface ServerMessage<T = any> {
  type: EventType;
  subscriptionId?: string;
  payload?: RealTimeEvent<T>;
  documents?: Document<T>[];
  error?: string;
}

// =============================================================================
// 2. QUERY DSL (Strict Typing)
// =============================================================================

/**
 * A simple "Where" clause (e.g., `status == 'active'`).
 * **Note:** This interface explicitly forbids `$and` or `$or` keys to enforce strict typing.
 */
export interface SimpleWhere {
  /** The field name to filter on. */
  field: string;
  /** The comparison operator. */
  op: "==" | "=" | "!=" | ">" | ">=" | "<" | "<=";
  /** The value to compare against. */
  value: any;
  /** Forbidden in simple clause. */
  $and?: never;
  /** Forbidden in simple clause. */
  $or?: never;
}

/**
 * A logical AND grouping.
 * **Note:** This interface explicitly forbids `field`, `op`, or `value` keys.
 */
export interface LogicalAnd {
  /** Array of conditions that must all be true. */
  $and: (SimpleWhere | LogicalAnd | LogicalOr)[];
  field?: never;
  op?: never;
  value?: never;
  $or?: never;
}

/**
 * A logical OR grouping.
 * **Note:** This interface explicitly forbids `field`, `op`, or `value` keys.
 */
export interface LogicalOr {
  /** Array of conditions where at least one must be true. */
  $or: (SimpleWhere | LogicalAnd | LogicalOr)[];
  field?: never;
  op?: never;
  value?: never;
  $and?: never;
}

/**
 * The main Where type. Can be a simple condition or recursive logical groupings.
 */
export type Where = SimpleWhere | LogicalAnd | LogicalOr;

/**
 * The Query Domain Specific Language (DSL).
 * Used to filter, sort, and paginate data.
 */
export interface QueryDSL {
  /** The filter condition. */
  where?: Where;
  /** The sorting criteria. */
  orderBy?: { field: string; direction?: "asc" | "desc" }[];
  /** Maximum number of documents to return. */
  limit?: number;
  /** Number of documents to skip. */
  offset?: number;
}

// =============================================================================
// 3. API & CLIENT OPTIONS
// =============================================================================

/** Options required to start a subscription. */
export interface SubscriptionOptions {
  /** The name of the collection to subscribe to. */
  collection: string;
  /** If provided, subscribes only to this specific document ID. */
  docId?: string;
  /** If provided, subscribes to documents matching this query configuration. */
  query?: QueryDSL;
}

/** Options required to create an index. */
export interface IndexOptions {
  /** The name of the index to create. */
  name: string;
  /** The JSON fields to index. */
  fields: string[];
  /** Whether the index should enforce uniqueness. */
  unique?: boolean;
}

// --- Response Types ---

export interface CollectionResponse {
  status: "created" | "deleted";
  collection: string;
}
export interface IndexResponse {
  status: "created";
  collection: string;
  index: IndexOptions;
}
export interface PutDocumentResponse {
  status: "replaced";
  id: string;
}
export interface PatchDocumentResponse {
  status: "patched";
  id: string;
}
export interface DeleteDocumentResponse {
  status: "deleted";
  id: string;
}
export interface HealthStatus {
  status: string;
  database: string;
}

// =============================================================================
// 4. CLASSES
// =============================================================================

/**
 * A simple typed event emitter used internally.
 */
export class EventEmitter {
  on(eventName: string, listener: (data?: any) => void): void;
  off(eventName: string, listenerToRemove: (data?: any) => void): void;
  emit(eventName: string, data?: any): void;
}

/**
 * A handle to an active subscription.
 * Provides methods to listen to data streams, error handling, and lifecycle management.
 * @template T - The expected type of the document data.
 */
export class Subscription<T = any> {
  /** The unique ID of this subscription. */
  public readonly id: string;

  constructor(client: RealTimeSQLite, id: string);

  /**
   * Registers a callback for **raw** messages on this subscription.
   * Useful for debugging or custom handling of specific message types.
   */
  on(callback: (message: ServerMessage<T>) => void): this;

  /**
   * Unregisters a specific raw message callback.
   */
  off(callback: (message: ServerMessage<T>) => void): this;

  /**
   * Registers a callback specifically for subscription errors.
   */
  onError(callback: (error: string) => void): this;

  /**
   * Subscribes to the data feed using a **Hybrid Sync** strategy.
   * It maintains two synchronized structures: an Array for ordering and a Map for O(1) access.
   *
   * It guarantees object reference stability: when an update occurs, the existing object
   * in memory is mutated, so the object in the `list` and the `map` remains the same instance.
   *
   * @param callback - A function called whenever the data changes.
   *   - `list`: Array of items sorted by the database return order.
   *   - `map`: Map of items keyed by their system ID (`__id`) for instant lookup.
   *   - `isLoading`: True if the initial data snapshot is still downloading.
   *
   * @returns A cleanup function to stop listening (this does not unsubscribe from the server).
   */
  subscribeToData(
    callback: (
      list: LocalDocument<T>[],
      map: Map<string, LocalDocument<T>>,
      isLoading: boolean
    ) => void
  ): () => void;

  /**
   * Unsubscribes from the real-time feed on the server.
   * This stops all updates for this specific feed.
   */
  unsubscribe(): void;
}

/**
 * The main client for interacting with the Real-Time SQLite server.
 * Manages WebSocket connections, reconnection logic, and REST API calls.
 */
export class RealTimeSQLite {
  /**
   * Creates a new client instance.
   * @param baseURL - The root URL of the server (e.g., "http://localhost:8080").
   */
  constructor(baseURL: string);

  /** Checks server connectivity and database status. */
  getHealth(): Promise<HealthStatus>;

  /** Lists all available collections. */
  getCollections(): Promise<string[]>;

  /** Creates a new collection (table). */
  createCollection(name: string): Promise<CollectionResponse>;

  /** Deletes a collection. */
  deleteCollection(name: string): Promise<CollectionResponse>;

  /** Creates an index on specific fields. */
  createIndex(
    collectionName: string,
    options: IndexOptions
  ): Promise<IndexResponse>;

  /** Gets a single document via REST. */
  getDocument<T = any>(collection: string, docId: string): Promise<T>;

  /** Fully replaces or creates a document via REST. */
  setDocument<T = any>(
    collection: string,
    docId: string,
    data: T
  ): Promise<PutDocumentResponse>;

  /** Partially updates a document via REST using JSON Patch. */
  updateDocument<T = any>(
    collection: string,
    docId: string,
    patch: Partial<T>
  ): Promise<PatchDocumentResponse>;

  /** Deletes a document via REST. */
  deleteDocument(
    collection: string,
    docId: string
  ): Promise<DeleteDocumentResponse>;

  /**
   * Executes a one-time query via REST.
   * **Note:** The server returns an array of wrapper objects `{id, data}`.
   */
  queryCollection<T = any>(
    collection: string,
    dsl: QueryDSL
  ): Promise<Document<T>[]>;

  /**
   * Opens a WebSocket connection (if not open) and creates a new subscription.
   * @param options - Configuration for the subscription (collection, docId, or query).
   * @returns An object to manage the subscription.
   */
  subscribe<T = any>(options: SubscriptionOptions): Subscription<T>;

  /**
   * Manually cancels a subscription by ID.
   * (Usually handled by calling `.unsubscribe()` on the Subscription object).
   */
  unsubscribe(subscriptionId: string): void;

  /**
   * Closes the WebSocket connection and stops reconnection attempts.
   */
  disconnect(): void;

  /**
   * Starts a background task to poll the health endpoint.
   * If the endpoint fails, it forces a WebSocket reconnection.
   * @param intervalMs - How often to poll (default 5000ms).
   */
  startHealthMonitor(intervalMs?: number): void;

  /** Stops the background health polling. */
  stopHealthMonitor(): void;
}
