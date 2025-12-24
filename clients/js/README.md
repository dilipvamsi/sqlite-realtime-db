# Real-Time SQLite Client

The official JavaScript/TypeScript client for **Real-Time SQLite**.

This library handles WebSocket connection management, automatic reconnection, and provides a powerful **Hybrid Data Sync** engine that maintains a local, sorted, and up-to-date cache of your database queries.

## ✨ Features

*   **Universal:** Works in Browsers and Node.js.
*   **Reactive Data:** Automatically handles `INSERT`, `UPDATE`, `DELETE`, and `REMOVE` (query mismatch) events.
*   **Hybrid State:** Provides data as both an **Ordered Array** (for UI rendering) and a **Map** (for O(1) lookups).
*   **Reference Stability:** Updates objects in-place. Essential for React `memo` and Vue/Svelte fine-grained reactivity.
*   **Type-Safe:** First-class TypeScript support with generics.
*   **Resilient:** Automatic exponential backoff reconnection and "Zombie connection" detection via HTTP health checks.

---

## 📦 Installation

### Option 1: Browser (Zero Build)
The server serves the client automatically. Just add this to your HTML:

```html
<script src="http://localhost:17050/realtime.js"></script>
<script>
  const client = new RealTimeSQLite("http://localhost:17050");
</script>
```

### Option 2: Node.js / Bundlers
If you are copying the source into your project or using it in a Node.js script:

1.  **Install WebSocket polyfill (Node.js only):**
    ```bash
    npm install ws
    ```
2.  **Import:**
    ```typescript
    import { RealTimeSQLite } from './path/to/client/index';
    ```

---

## 🚀 Quick Start

### 1. Initialize
```typescript
const client = new RealTimeSQLite("http://localhost:17050");
```

### 2. Subscribe to Data
This is the core feature. You subscribe to a query, and the client gives you a synchronized local view.

```typescript
interface Order {
  status: string;
  total: number;
}

const sub = client.subscribe<Order>({
  collection: "orders",
  query: {
    where: { field: "status", op: "==", value: "pending" },
    orderBy: [{ field: "total", direction: "desc" }]
  }
});

// The magic happens here:
sub.subscribeToData((list, map, isLoading) => {
  if (isLoading) {
    console.log("Loading snapshot...");
    return;
  }

  // 'list' is an Array (Use for UI Rendering)
  console.log("Pending Orders:", list);

  // 'map' is a Map (Use for looking up specific IDs)
  const order123 = map.get("ord_123");
});
```

### 3. Mutate Data
The client wraps the REST API for convenience. Writing data triggers real-time updates for all subscribers instantly.

```typescript
// Create
await client.setDocument("orders", "ord_100", { status: "pending", total: 50 });

// Update (Partial Patch)
await client.updateDocument("orders", "ord_100", { status: "shipped" });

// Delete
await client.deleteDocument("orders", "ord_100");
```

---

## 🧠 Core Concepts

### Hybrid Data Structure (`list` vs `map`)
When you use `subscribeToData`, the callback provides two structures:

1.  **`list` (Array):** Contains items sorted exactly as the database returned them (based on your `orderBy`). Use this for `v-for` (Vue) or `.map()` (React).
2.  **`map` (Map):** Keyed by the document ID. Use this for `O(1)` access when you need to find a specific item without iterating the array.

### `_id` vs `id`
The client **automatically injects** a field named `_id` into every document. This holds the system Document ID (the key).
*   It **does not** touch your data's `id` field if it exists.
*   This ensures you always have a reliable way to reference the document key, even if your JSON payload doesn't contain it.

### Reference Stability
When an update arrives (e.g., changing `status` from 'pending' to 'shipped'), the client **mutates the existing object in memory**.
*   **React:** If you pass an item to a memoized component, it won't re-render unless that specific item changed.
*   **Vue/Svelte:** Fine-grained reactivity works out of the box because the object reference stays the same.

---

## 🔍 Query DSL

The query language is JSON-based and strictly typed in TypeScript.

| Property | Type | Description |
| :--- | :--- | :--- |
| `where` | `object` | Filter conditions. |
| `limit` | `number` | Max records to fetch. |
| `offset` | `number` | Pagination offset. |
| `orderBy`| `array` | Sort fields. |

### Operators
`==`, `=`, `!=`, `>`, `>=`, `<`, `<=`

### Complex Logic (`$and` / `$or`)
You can nest conditions arbitrarily.

```javascript
const query = {
  where: {
    $or: [
      { field: "category", op: "==", value: "urgent" },
      {
        $and: [
          { field: "status", op: "==", value: "pending" },
          { field: "total", op: ">", value: 1000 }
        ]
      }
    ]
  },
  orderBy: [{ field: "total", direction: "desc" }],
  limit: 50
};
```

---

## 📚 API Reference

### `RealTimeSQLite`
*   `subscribe<T>(options)`: Returns a `Subscription` handle.
*   `unsubscribe(subId)`: Manually cancels a subscription.
*   `disconnect()`: Closes the WebSocket.
*   `startHealthMonitor(interval)`: Starts HTTP polling to detect broken connections.

### `Subscription`
*   `subscribeToData(callback)`: **Recommended.** Binds to the synchronized data cache. Returns a cleanup function.
*   `on(callback)`: Listen to raw events (`realtime_update`, `initial_data_batch`).
*   `unsubscribe()`: Stops this subscription.

---

## 🛡️ License

MIT
