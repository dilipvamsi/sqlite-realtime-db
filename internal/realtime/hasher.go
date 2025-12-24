// hashing.go
package realtime

import (
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/cespare/xxhash/v2"
)

// generateQueryHash creates a high-performance xxhash64 hash from a Subscription's raw query part.
// This hash serves as the unique key for the query in the Hub's subscription maps.
// It assumes the client sends the JSON query with a consistent key order.
func generateQueryHash(rawQuery json.RawMessage) (uint64, error) {
	if len(rawQuery) == 0 || string(rawQuery) == "null" {
		return 0, fmt.Errorf("cannot hash a nil or empty query")
	}
	return xxhash.Sum64(rawQuery), nil
}
