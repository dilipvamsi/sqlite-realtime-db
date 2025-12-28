// main is the application's entrypoint.
package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"

	"sqlite-realtime-db/internal/realtime"
)

func main() {
	// OPTIMIZATION: Reduce GC pressure.
	// Default is 100. Setting to 200 tells Go to wait until the heap doubles
	// before sweeping. This trades slightly more RAM usage for significantly
	// lower CPU usage and latency spikes during high event throughput.
	debug.SetGCPercent(200)

	// 1. Database Configuration
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./data/realtime.db"
	}

	logPath := os.Getenv("LOG_DB_PATH")
	if logPath == "" {
		// By default, store logs in a separate file next to the main DB
		logPath = "./data/logs.db"
	}

	// 2. Initialize Database (Main + Attached Logs)
	// This sets up the connection pool, schemas, and in-memory caches.
	db := realtime.InitDB(dbPath, logPath)
	defer db.Close()

	// 3. Server Configuration
	host := os.Getenv("HOST")
	if host == "" {
		host = "localhost"
	}

	var port uint16 = 17050
	strPort := os.Getenv("PORT")
	if strPort != "" {
		val64, err := strconv.ParseUint(strPort, 10, 16)
		if err != nil {
			fmt.Printf("Error parsing PORT '%s': %v\n", strPort, err)
			return
		}
		port = uint16(val64)
	}

	// 4. MultiHub Shard Configuration
	// Use CPU cores for parallelism, but enforce a minimum of 8 to reduce lock contention on small VMs.
	numShards := uint32(max(runtime.NumCPU(), 8))

	// Allow override via environment variable
	strShards := os.Getenv("HUB_SHARDS")
	if strShards != "" {
		val64, err := strconv.ParseUint(strShards, 10, 32)
		if err != nil || val64 == 0 {
			fmt.Printf("Invalid HUB_SHARDS '%s', defaulting to %d\n", strShards, numShards)
		} else {
			numShards = uint32(val64)
		}
	}

	// 4. Start Server
	fmt.Printf("Starting server on %s:%d with %d Hub Shards...\n", host, port, numShards)
	realtime.Server(db, host, port, numShards)
}
