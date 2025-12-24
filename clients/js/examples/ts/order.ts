// example.ts

import { RealTimeSQLite } from "../../src/index";

// =============================================================================
// 1. DEFINE DATA INTERFACES
// =============================================================================

// Define the shape of our 'Order' document
interface Order {
  status: "draft" | "pending" | "shipped" | "delivered";
  total: number;
  region: "US" | "EU" | "APAC";
  customerName?: string;
  // Note: We don't need to define 'id' or '__id' here.
  // The client library adds __id automatically via LocalDocument<T>.
}

// =============================================================================
// 2. CONFIGURATION
// =============================================================================

const PORT = 17050;
const client = new RealTimeSQLite(`http://localhost:${PORT}`);

// Helpers
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const printHeader = (msg: string) =>
  console.log(`\n${"=".repeat(60)}\n📌 ${msg}\n${"=".repeat(60)}`);

async function main() {
  console.log(`🚀 Real-Time SQLite TypeScript Demo Starting...\n`);

  try {
    // =========================================================================
    // 3. DATABASE PREPARATION
    // =========================================================================
    printHeader("STEP 1: Database Schema");

    await client.deleteCollection("realtime_order_demo").catch(() => {});
    await client.createCollection("realtime_order_demo");

    await client.createIndex("realtime_order_demo", {
      name: "idx_status",
      fields: ["status"],
    });
    await client.createIndex("realtime_order_demo", {
      name: "idx_total",
      fields: ["total"],
    });

    console.log(`✅ Collection 'realtime_order_demo' ready.`);

    // =========================================================================
    // 4. SUBSCRIPTION REGISTRATION (Type-Safe)
    // =========================================================================
    printHeader("STEP 2: Registering Subscriptions");

    /**
     * CASE A: WHOLE COLLECTION
     * We pass <Order> generic to get typed data back.
     */
    const subAdmin = client.subscribe<Order>({ collection: "realtime_order_demo" });

    // Callback signature: (list, map, isLoading)
    subAdmin.subscribeToData((list, map, isLoading) => {
      if (isLoading) return;

      // 'list' is strictly typed as LocalDocument<Order>[]
      // 'map'  is strictly typed as Map<string, LocalDocument<Order>>

      const summary = list.map((d) => `#${d.__id}`).join(", ");
      console.log(
        `📋 [ADMIN] Count: ${list.length} | IDs: ${summary || "(empty)"}`
      );
    });

    /**
     * CASE B: COMPLEX QUERY (High Value EU Orders)
     */
    const subVIP = client.subscribe<Order>({
      collection: "realtime_order_demo",
      query: {
        where: {
          $and: [
            { field: "region", op: "==", value: "EU" },
            { field: "total", op: ">", value: 100 },
          ],
        },
        orderBy: [{ field: "total", direction: "desc" }],
      },
    });

    subVIP.subscribeToData((list, _map, isLoading) => {
      if (isLoading) return;
      // Accessing properties is type-safe
      const summary = list.map((d) => `#${d.__id} ($${d.total})`).join(", ");
      console.log(
        `💎 [VIP EU] Count: ${list.length} | Items: ${summary || "(empty)"}`
      );
    });

    await sleep(1000);

    // =========================================================================
    // 5. SCENARIO TESTING
    // =========================================================================
    printHeader("STEP 3: Running Scenarios");

    // --- SCENARIO 1: Insert (US Order) ---
    // Should appear in ADMIN, but NOT VIP
    console.log(`\n🔹 Action: Insert US Order #100 ($50)`);
    await client.setDocument<Order>("realtime_order_demo", "ord_100", {
      status: "pending",
      total: 50,
      region: "US",
      customerName: "Alice",
    });
    await sleep(600);

    // --- SCENARIO 2: Insert (EU High Value) ---
    // Should appear in BOTH
    console.log(`\n🔹 Action: Insert EU Order #200 ($500)`);
    await client.setDocument<Order>("realtime_order_demo", "ord_200", {
      status: "pending",
      total: 500,
      region: "EU",
      customerName: "Bob",
    });
    await sleep(600);

    // --- SCENARIO 3: O(1) Lookup & Reference Check ---
    console.log(`\n🔹 Check: Verifying Map Lookup and Object Identity`);
    // We can access the internal map of the subscription manually if needed via a one-off subscription logic,
    // but here we just simulate the logic inside the callback:
    subVIP.subscribeToData((list, map, isLoading) => {
      if (isLoading || list.length === 0) return;

      // O(1) Access
      const item = map.get("ord_200");
      if (item) {
        console.log(
          `   ✅ Map Lookup Success: ${item.customerName} - $${item.total}`
        );

        // Reference Check
        const listItem = list.find((i) => i.__id === "ord_200");
        console.log(`   ✅ Reference Equality: ${item === listItem}`); // Should be true
      }
    });
    await sleep(200);

    // --- SCENARIO 4: Transition Out (Value Change) ---
    // Reduce total to 50 -> Should disappear from VIP view
    console.log(`\n🔹 Action: Discount #200 to $50 (Should vanish from VIP)`);
    await client.updateDocument<Order>("realtime_order_demo", "ord_200", { total: 50 });
    await sleep(600);

    // --- SCENARIO 5: Unsubscribe ---
    console.log(`\n🛑 Unsubscribing Admin Feed...`);
    subAdmin.unsubscribe();
    await sleep(500);

    console.log(`\n🔹 Action: Delete #100 (Should NOT log Admin update)`);
    await client.deleteDocument("realtime_order_demo", "ord_100");
    await sleep(1000);

    // =========================================================================
    // 6. TEARDOWN
    // =========================================================================
    printHeader("TEST COMPLETE");
    client.disconnect();
  } catch (error) {
    console.error("❌ Error:", error);
  }
}

main().catch(console.error);
