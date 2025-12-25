package realtime

import (
	"database/sql"
	"encoding/json"
	"log"
	"sync"
)

// DataType restricts the allowed column types in the schema.
type DataType string

const (
	TypeText     DataType = "text"
	TypeFloat    DataType = "float"
	TypeInt      DataType = "int"
	TypeBool     DataType = "bool"
	TypeDateTime DataType = "datetime"
)

// IsValid checks if the provided type is one of the allowed primitives.
func (d DataType) IsValid() bool {
	switch d {
	case TypeText, TypeFloat, TypeInt, TypeBool, TypeDateTime:
		return true
	}
	return false
}

// FieldDef defines a column (matches what is stored in system_schema)
type FieldDef struct {
	Name     string   `json:"name"`
	Type     DataType `json:"type"` // Uses the custom type
	Required bool     `json:"required,omitempty"`
	Unique   bool     `json:"unique,omitempty"`
}

// SchemaCache stores the field definitions for every collection in memory.
// Structure: map[CollectionName] -> []FieldDef
var (
	schemaCache = make(map[string][]FieldDef)
	schemaMutex sync.RWMutex
)

// LoadSchemaCache reads system_schema into memory. Call this on InitDB.
func LoadSchemaCache(db *sql.DB) error {
	schemaMutex.Lock()
	defer schemaMutex.Unlock()

	// Clear existing
	for k := range schemaCache {
		delete(schemaCache, k)
	}

	rows, err := db.Query("SELECT name, schema FROM main.system_schema")
	if err != nil {
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var schemaJSON []byte
		if err := rows.Scan(&name, &schemaJSON); err != nil {
			continue
		}

		var fields []FieldDef
		if err := json.Unmarshal(schemaJSON, &fields); err == nil {
			schemaCache[name] = fields
		}
	}
	log.Printf("Schema Cache Loaded: %d collections", len(schemaCache))
	return nil
}

// GetCollectionFields returns the typed columns for a collection
func GetCollectionFields(collection string) []FieldDef {
	schemaMutex.RLock()
	defer schemaMutex.RUnlock()
	return schemaCache[collection]
}

// SetCollectionSchema updates the in-memory cache for a single collection.
// This avoids reloading the entire schema from the database.
func SetCollectionSchema(collection string, fields []FieldDef) {
	schemaMutex.Lock()
	defer schemaMutex.Unlock()
	schemaCache[collection] = fields
}

// RemoveCollectionSchema removes a collection from the in-memory cache.
func RemoveCollectionSchema(collection string) {
	schemaMutex.Lock()
	defer schemaMutex.Unlock()
	delete(schemaCache, collection)
}
