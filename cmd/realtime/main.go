// main is the application's entrypoint.
package main

import (
	"fmt"
	"sqlite-realtime-db/internal/realtime"
	"os"
	"strconv"
)

func main() {

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./data/realtime.db"
	}

	db := realtime.InitDB(dbPath)
	defer db.Close()

	host := os.Getenv("HOST")
	if host == "" {
		host = "localhost"
	}

	var port uint16 = 17050
	strPort := os.Getenv("PORT")
	if strPort != "" {
		val64, err := strconv.ParseUint(strPort, 10, 16)
		if err != nil {
			// This block will execute if the string is not a valid number
			// or if the number is out of the uint16 range (0-65535).
			fmt.Printf("Error parsing string: %v\n", err)
			return
		}
		port = uint16(val64)
	}
	realtime.Server(db, host, port)
}
