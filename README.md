# Real-Time SQLite

A lightweight, self-hosted, real-time database server built on top of SQLite. It provides **live subscriptions** to database queries over WebSockets, a full REST API, and a robust TypeScript client library.

Think of it as a pocket-sized Firebase or Supabase that runs as a single binary.

![Status](https://img.shields.io/badge/status-active-success) ![Go](https://img.shields.io/badge/go-1.23+-blue) ![SQLite](https://img.shields.io/badge/sqlite-embedded-blue)

## 🚀 Features

*   **Real-Time Subscriptions:** Subscribe to entire collections, specific documents, or complex filtered queries.
*   **Reactive Query Engine:** Clients receive `INSERT`, `UPDATE`, `DELETE`, and `REMOVE` events instantly.
*   **Hybrid Client:** The JS/TS client maintains a local synchronized cache with **O(1) lookups** and **preserved order**.
*   **JSON Document Store:** Store arbitrary JSON data with schema-less flexibility.
*   **Built-in Tools:**
    *   **Studio:** A visual dashboard to manage data and debug queries.
    *   **Simulator:** A traffic generator to test real-time performance.
*   **Portable:** Compiles to a single static binary (Linux/Windows/Mac) with zero dependencies.

---

## 🛠️ Architecture

1.  **Write Path:** Clients write data via the REST API (`PUT`, `PATCH`, `DELETE`).
2.  **CDC (Change Data Capture):** SQLite Triggers automatically record changes into a `changelog` table.
3.  **Event Processing:** A Go background worker tails the changelog.
4.  **Broadcast:** The WebSocket Hub matches changes against active query subscriptions and pushes updates to connected clients.

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
    make run
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

The query language is JSON-based.

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

## 🏗️ Building for Production

Use the included `Makefile` to generate optimized binaries.

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
| `GET` | `/db/collections` | List all collections |
| `POST` | `/db/collections` | Create a new collection |
| `POST` | `/db/indexes/{coll}` | Create an index |
| `GET` | `/db/data/{coll}/{id}` | Get document |
| `PUT` | `/db/data/{coll}/{id}` | Upsert document (Replace) |
| `PATCH`| `/db/data/{coll}/{id}` | Update document (Merge Patch) |
| `DEL` | `/db/data/{coll}/{id}` | Delete document |
| `POST` | `/db/query/{coll}` | Execute one-time query |

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
