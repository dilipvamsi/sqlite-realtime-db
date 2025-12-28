# Real-Time SQLite

A high-performance, self-hosted, real-time database server built on top of SQLite. It provides **live subscriptions** to database queries over WebSockets, a full REST API, and a robust TypeScript client library.

It combines the simplicity of SQLite with the reactivity of Firebase, engineered for **maximum throughput** and **minimal latency**.

![Status](https://img.shields.io/badge/status-active-success) ![Go](https://img.shields.io/badge/go-1.23+-blue) ![SQLite](https://img.shields.io/badge/sqlite-embedded-blue) ![Architecture](https://img.shields.io/badge/arch-hybrid_schema-orange)

---

## 🚀 Key Features

*   **⚡ Real-Time Query Sync:** Subscribe to complex queries (e.g., `WHERE status='active' AND total > 100`). The server pushes `INSERT`, `UPDATE`, `DELETE`, and `REMOVE` (mismatch) events instantly.
*   **🏗 Hybrid Schema:**
    *   **Typed Columns:** Promote specific fields to native SQL columns (`INT`, `TEXT`, `REAL`) for blazing-fast indexing and sorting.
    *   **JSON Document Store:** Store arbitrary unstructured data in a catch-all `BLOB` column.
    *   **Zero-Cost Migrations:** Use the API to promote/demote fields dynamically without downtime.
*   **🧠 Smart Client:** The JS/TS client maintains a synchronized local cache with **O(1) lookups** and **preserved order**, mutating objects in-place for React/Vue stability.
*   **🚅 High Performance Architecture:**
    *   **Multi-Hub Sharding:** Distributes WebSocket connections across CPU cores to eliminate lock contention.
    *   **Zero-Allocation Broadcast:** Uses `PreparedMessage` to frame WebSocket packets once per topic, copying bytes directly to 50k+ clients.
    *   **Application-Side CDC:** Bypasses slow SQLite triggers by handling Change Data Capture logic in Go.
    *   **SingleFlight:** Coalesces concurrent read requests ("Thundering Herds") into a single database query.
    *   **Split Storage:** Automatically strips promoted fields from the JSON blob to save disk space and IO.
*   **🛠 Built-in Tools:**
    *   **Studio:** A visual dashboard to manage data and debug queries (`/studio`).
    *   **Simulator:** A traffic generator to load-test the system (`/simulator`).
*   **📦 Portable:** Compiles to a single static binary (Linux/Windows/Mac) with **zero dependencies** (Static Musl/CGO build).

---

## ⚡ Quick Start

### Prerequisites
*   Go 1.23+
*   Make (optional, for build automation)
*   Docker (optional, for static Linux builds)

### Running Locally

1.  **Clone the repository**
2.  **Run with Make** (This prepares assets and runs the server):
    ```bash
    make run-dev
    ```
    *Or manually:*
    ```bash
    cp clients/js/src/index.js internal/realtime/public/realtime.js
    go run cmd/realtime/main.go
    ```

3.  **Access the Dashboard:**
    Open [http://localhost:17050](http://localhost:17050) in your browser.

---

## 🖥️ Built-in Tools

The server embeds two powerful UI tools accessible via the browser:

1.  **Real-Time Studio** (`/studio`)
    *   View live data streams.
    *   Test JSON queries.
    *   Debug raw WebSocket events.

2.  **Traffic Simulator** (`/simulator`)
    *   Simulates a live environment (e.g., an Orders system).
    *   Generates random Inserts, Updates, and Deletes.
    *   Useful for verifying query filters and performance.

---

## 📦 JavaScript/TypeScript Client

The server serves its own client library at `/realtime.js`.

### Installation
You can include it directly in HTML:
```html
<script src="http://localhost:17050/realtime.js"></script>
```
Or use the files in `clients/js` for your Node.js/Bundler projects.

### Usage Example

```typescript
// 1. Initialize
const client = new RealTimeSQLite("http://localhost:17050");

// 2. Subscribe to a Query
const subscription = client.subscribe({
  collection: "orders",
  query: {
    where: {
      $and: [
        { field: "status", op: "==", value: "pending" },
        { field: "total", op: ">", value: 100 }
      ]
    },
    orderBy: [{ field: "total", direction: "desc" }]
  }
});

// 3. Listen for synchronized data (Recommended)
// 'list' is an Array (for UI rendering)
// 'map' is a Map (for O(1) lookups by ID)
subscription.subscribeToData((list, map, isLoading) => {
  if (isLoading) return;
  console.log("Current High-Value Pending Orders:", list);
});

// 4. Update Data (Triggers real-time updates for all clients)
await client.updateDocument("orders", "ord_123", { status: "shipped" });
```

---

## 🔍 Query DSL

The query language is JSON-based and strictly typed.

| Operator | Description |
| :--- | :--- |
| `==`, `=` | Equality |
| `!=` | Inequality |
| `>`, `>=` | Greater than (numeric/string) |
| `<`, `<=` | Less than (numeric/string) |

### Complex Query Example
```json
{
  "where": {
    "$or": [
      { "field": "category", "op": "==", "value": "electronics" },
      {
        "$and": [
          { "field": "category", "op": "==", "value": "books" },
          { "field": "price", "op": "<", "value": 20 }
        ]
      }
    ]
  },
  "orderBy": [
    { "field": "price", "direction": "asc" }
  ],
  "limit": 50
}
```

---

## 🛠️ Architecture Deep Dive

### 1. Attached Databases (Split Storage)
The system uses two separate SQLite files to maximize performance and manageability:
*   **`realtime.db` (Main):** Stores the actual data (`orders`, `users`). Uses WAL mode and `synchronous=NORMAL`.
*   **`logs.db` (Audit):** Stores the `changelog` (CDC) and `system_state`. This prevents the main database from fragmentation due to high-churn log writes.

### 2. Application-Side CDC
Instead of slow SQL Triggers, the Go application handles the logic:
1.  **Handler:** Receives `PUT` request.
2.  **Logic:** Calculates the JSON patch and split columns in memory (Go CPU).
3.  **Transaction:** Writes to Data Table AND Audit Log in a single SQL transaction.
4.  **Result:** ~30-50% higher write throughput compared to Trigger-based CDC.

### 3. Multi-Hub Sharding
The WebSocket Hub is sharded (default 16 shards) based on Collection Name hash.
*   **Benefit:** A massive broadcast on the `orders` collection does not block a user subscribing to `chats`.
*   **Scalability:** Allows the Go runtime to schedule query matching across all available CPU cores.

### 4. SingleFlight (Request Coalescing)
To protect the database during "Thundering Herd" events (e.g., thousands of clients reconnecting simultaneously):
*   **Mechanism:** If 5,000 clients request the exact same query snapshot at the same time, the server executes the SQL **once**.
*   **Result:** The memory buffer is shared across all 5,000 goroutines. Memory allocation drops from O(N) to O(1), virtually eliminating Garbage Collection spikes.

### 5. Passive WAL Checkpointing
A background janitor runs `PRAGMA wal_checkpoint(PASSIVE)` on a timer.
*   **Why:** Standard SQLite checkpoints can sometimes block readers/writers if the WAL grows too large.
*   **Benefit:** This keeps the WAL file compact and ensures consistent p99 write latency without "stuttering" during heavy load.

---

## 🏗️ Building for Production

### 1. Dynamic Build (Local Machine)
```bash
make build-dynamic
# Output: bin/realtime-server-dynamic.bin (or .exe)
```

### 2. Static Build (Host OS)
Includes C libraries statically. Requires system headers (`glibc-static` or `musl`).
```bash
make build
# Output: bin/realtime-server.bin (or .exe)
```

### 3. Portable Linux Build (Docker) - **Recommended**
Creates a binary that runs on **any** Linux distribution (Ubuntu, Alpine, CentOS, etc.) by compiling with Alpine/Musl inside Docker.
```bash
make docker-build
# Output: bin/realtime-server-linux-portable.bin
```

---

## 📂 Project Structure

```bash
.
├── clients/js          # TypeScript client source code
├── cmd/realtime        # Entry point (main.go)
├── internal/realtime   # Core logic
│   ├── public          # Embedded static assets (Studio, Simulator)
│   ├── db.go           # SQLite connection & schema
│   ├── hub.go          # WebSocket subscription hub
│   ├── query.go        # JSON Query DSL parser
│   └── ws_server.go    # WebSocket handler
├── data                # Database files (created on runtime)
└── Makefile            # Build automation
```

## 📜 REST API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Server health check |
| `GET` | `/db/collections` | List all collections with schema |
| `GET` | `/db/collections/{collection}` | Get collection with schema |
| `POST` | `/db/collections` | Create a new collection (optional schema) |
| `PATCH` | `/db/collections/{collection}` | Migrate schema (Promote/Demote columns) |
| `DELETE` | `/db/collections/{collection}` | Delete the collection |
| `POST` | `/db/indexes/{collection}` | Create an index |
| `GET` | `/db/data/{collection}/{id}` | Get document |
| `PUT` | `/db/data/{collection}/{id}` | Upsert document (Replace) |
| `PATCH`| `/db/data/{collection}/{id}` | Update document (Merge Patch) |
| `DELETE`| `/db/data/{collection}/{id}` | Delete document |
| `POST` | `/db/batch` | Execute multiple operations atomically |
| `POST` | `/db/query/{collection}` | Execute one-time query |

---

## 🔮 Future Roadmap

We are actively working on making Real-Time SQLite a complete production-ready alternative to heavy cloud providers. Here is what's on the horizon:

### 🛡️ Security & Authorization
*   **Pluggable Authorization:** Flexible hooks to extract and validate identity from request headers (e.g., Bearer tokens, API Keys) before establishing connections.
*   **Collection-Level Security (CLS):** Define granular access policies per collection using a declarative syntax (e.g., `allow read: if auth.uid == resource.owner_id`).
*   **CORS Configuration:** Fine-grained control over allowed origins, methods, and headers for both WebSocket and REST endpoints.

### 🔌 Client Enhancements
*   **Offline Persistence:** Update the JS Client to persist the local cache to `IndexedDB` or `localStorage`, allowing apps to work offline and sync when online.

### ⚡ Performance & Engine
*   **Full-Text Search (FTS5):** Expose SQLite's powerful FTS5 engine via the Query DSL for high-performance text search.
*   **Binary Protocol:** Optional support for **MessagePack** or **Protobuf** over WebSockets to reduce payload size by 30-50%.

### 🌍 Scalability
*   **LiteFS Integration:** Native support for [LiteFS](https://fly.io/docs/litefs/) to allow distributed, replicated SQLite across multiple regions.
*   **S3 Backups:** Automatic scheduled backups of the SQLite database to S3-compatible storage.

### 📦 New SDKs
*   **Dart / Flutter:** For native mobile applications.
*   **Python:** For data science and backend integration.
*   **React Hooks:** A dedicated `@realtime-sqlite/react` package with `useSubscription` and `useQuery` hooks.

## 🛡️ License

MIT
