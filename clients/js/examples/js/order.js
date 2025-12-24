// =============================================================================
// 1. ENVIRONMENT SETUP
// =============================================================================
if (typeof WebSocket === "undefined") {
  try {
    global.WebSocket = require("ws");
  } catch (e) {
    console.error("❌ Error: Please install 'ws' package: npm install ws");
    process.exit(1);
  }
}

const { RealTimeSQLite } = require("../../src/index");

// !!! CONFIGURATION !!!
const PORT = 17050;
const client = new RealTimeSQLite(`http://localhost:${PORT}`);

// Helpers for clean output
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const printHeader = (msg) =>
  console.log(`\n${"=".repeat(60)}\n📌 ${msg}\n${"=".repeat(60)}`);
const logEvent = (viewName, count, items) => {
  console.log(
    `${viewName.padEnd(25)} | Count: ${String(count).padEnd(
      2
    )} | Data: ${items}`
  );
};

async function main() {
  console.log(`🚀 Real-Time SQLite Comprehensive Test Suite Starting...\n`);

  try {
    // =========================================================================
    // 2. DATABASE PREPARATION
    // =========================================================================
    printHeader("STEP 1: Database Schema & Indexes");

    // Clean start
    await client.deleteCollection("realtime_order_demo").catch(() => {});
    await client.createCollection("realtime_order_demo");

    // Indexes for high-performance querying
    await client.createIndex("realtime_order_demo", {
      name: "idx_status",
      fields: ["status"],
    });
    await client.createIndex("realtime_order_demo", {
      name: "idx_total",
      fields: ["total"],
    });
    await client.createIndex("realtime_order_demo", {
      name: "idx_region",
      fields: ["region"],
    });

    console.log(`✅ Collection 'realtime_order_demo' ready with 3 indexes.`);

    // =========================================================================
    // 3. SUBSCRIPTION REGISTRATION
    // =========================================================================
    printHeader("STEP 2: Registering 5 Distinct Subscriptions");

    /**
     * CASE A: WHOLE COLLECTION
     * Context: Admin Audit Log
     */
    const subAdmin = client.subscribe({ collection: "realtime_order_demo" });
    subAdmin.subscribeToData((data, dataMap, isLoading) => {
      if (isLoading) return;
      const items = data.map((d) => `#${d.__id}`).join(", ");
      logEvent("📋 [ADMIN: ALL ORDERS]", data.length, items || "(empty)");
    });

    /**
     * CASE B: SINGLE DOCUMENT
     * Context: User tracking their specific order page
     */
    // Pre-create the doc so it exists (optional, but realistic)
    await client.setDocument("realtime_order_demo", "ord_100", {
      status: "draft",
      total: 0,
      region: "US",
    });

    const subTracker = client.subscribe({
      collection: "realtime_order_demo",
      docId: "ord_100",
    });
    subTracker.subscribeToData((data, dataMap, isLoading) => {
      if (isLoading) return;
      const d = data[0];
      const info = d ? `${d.status.toUpperCase()} ($${d.total})` : "(deleted)";
      logEvent("🎯 [USER: TRACKER #100]", data.length, info);
    });

    /**
     * CASE C: SIMPLE QUERY
     * Context: Kitchen View (Show only 'pending' realtime_order_demo)
     */
    const subKitchen = client.subscribe({
      collection: "realtime_order_demo",
      query: { where: { field: "status", op: "==", value: "pending" } },
    });
    subKitchen.subscribeToData((data, dataMap, isLoading) => {
      if (isLoading) return;
      const items = data.map((d) => `#${d.__id}`).join(", ");
      logEvent("👨‍🍳 [KITCHEN: PENDING]", data.length, items || "(empty)");
    });

    /**
     * CASE D: COMPLEX QUERY ($and) & SORTING
     * Context: VIP Dashboard (High Value Orders in EU)
     * Logic: Region == 'EU' AND Total > 100
     */
    const subVIP = client.subscribe({
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
    subVIP.subscribeToData((data, dataMap, isLoading) => {
      if (isLoading) return;
      // Client-side sort is often needed for live updates unless re-fetching
      const sorted = data.sort((a, b) => b.total - a.total);
      const items = sorted.map((d) => `#${d.__id}($${d.total})`).join(", ");
      logEvent("💎 [VIP: EU > $100]", data.length, items || "(empty)");
    });

    /**
     * CASE E: COMPLEX QUERY ($or)
     * Context: Logistics (Show 'shipped' OR 'delivered')
     */
    const subLogistics = client.subscribe({
      collection: "realtime_order_demo",
      query: {
        where: {
          $or: [
            { field: "status", op: "==", value: "shipped" },
            { field: "status", op: "==", value: "delivered" },
          ],
        },
      },
    });
    subLogistics.subscribeToData((data, dataMap, isLoading) => {
      if (isLoading) return;
      const items = data.map((d) => `#${d.__id}(${d.status})`).join(", ");
      logEvent("🚚 [LOGISTICS: SHIP/DLV]", data.length, items || "(empty)");
    });

    await sleep(1000); // Settle

    // =========================================================================
    // 4. SCENARIO TESTING
    // =========================================================================
    printHeader("STEP 3: Running Scenarios");

    // --- SCENARIO 1: Insert Matching Simple Query ---
    console.log(`\n🔹 Action: New Order #200 (Pending, US, $50)`);
    console.log(`   Expect: Appears in ADMIN and KITCHEN.`);
    await client.setDocument("realtime_order_demo", "ord_200", {
      status: "pending",
      total: 50,
      region: "US",
    });
    await sleep(600);

    // --- SCENARIO 2: Insert Matching Complex AND Query ---
    console.log(`\n🔹 Action: New Order #300 (Pending, EU, $250)`);
    console.log(
      `   Expect: Appears in ADMIN, KITCHEN (Pending), and VIP (EU > 100).`
    );
    await client.setDocument("realtime_order_demo", "ord_300", {
      status: "pending",
      total: 250,
      region: "EU",
    });
    await sleep(600);

    // --- SCENARIO 3: Single Doc Update (Transition In) ---
    console.log(`\n🔹 Action: Update #100 (Draft -> Pending)`);
    console.log(`   Expect: TRACKER shows status change. KITCHEN adds #100.`);
    await client.updateDocument("realtime_order_demo", "ord_100", { status: "pending" });
    await sleep(600);

    // --- SCENARIO 4: Transition Out (Pending -> Shipped) ---
    console.log(`\n🔹 Action: Update #300 (Pending -> Shipped)`);
    console.log(
      `   Expect: KITCHEN removes #300 (Transition Out). LOGISTICS adds #300 (Transition In via $or).`
    );
    await client.updateDocument("realtime_order_demo", "ord_300", { status: "shipped" });
    await sleep(600);

    // --- SCENARIO 5: Value Change affecting Filter ---
    console.log(`\n🔹 Action: Discount #300 (Total $250 -> $50)`);
    console.log(
      `   Expect: VIP removes #300 (Total < 100). LOGISTICS updates data.`
    );
    await client.updateDocument("realtime_order_demo", "ord_300", { total: 50 });
    await sleep(600);

    // --- SCENARIO 6: Unsubscribe Specific Feed ---
    printHeader("STEP 4: Unsubscribe Test");
    console.log(`🛑 Unsubscribing from VIP Dashboard...`);
    subVIP.unsubscribe();
    await sleep(500);

    console.log(`\n🔹 Action: Update #300 Total back to $500`);
    console.log(
      `   Expect: VIP should NOT log anything. LOGISTICS should show update.`
    );
    await client.updateDocument("realtime_order_demo", "ord_300", { total: 500 });
    await sleep(600);

    // --- SCENARIO 7: Deletion ---
    printHeader("STEP 5: Deletion Test");
    console.log(`\n🔹 Action: Delete Order #100`);
    console.log(`   Expect: Removed from ADMIN, TRACKER, and KITCHEN.`);
    await client.deleteDocument("realtime_order_demo", "ord_100");
    await sleep(600);

    // =========================================================================
    // 5. TEARDOWN
    // =========================================================================
    printHeader("TEST COMPLETE");
    console.log("Check the logs above to verify all expectations were met.");
    client.disconnect();
  } catch (error) {
    console.error("❌ Critical Error:", error);
  }
}

main().catch(console.error);
