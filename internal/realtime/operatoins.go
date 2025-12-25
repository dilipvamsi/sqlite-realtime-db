// operations.go
package realtime

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ApplySetDocument performs a "Replace" (Upsert) operation within a transaction.
// It handles schema validation, split storage (Columns vs Blob), and Audit Logging.
func ApplySetDocument(tx *sql.Tx, collection, docID string, bodyBytes []byte) (OperationType, error) {
	fields := GetCollectionFields(collection)

	// 1. Prepare Columns & Blob
	cols := []string{"id", "data"}
	placeholders := []string{"?", "jsonb(?)"}
	args := []any{docID}
	updateSets := []string{"data=excluded.data"}

	// Blob starts as full body; we will strip schema fields from it
	blobBytes := bodyBytes

	for _, field := range fields {
		cols = append(cols, field.Name)
		placeholders = append(placeholders, "?")
		updateSets = append(updateSets, fmt.Sprintf("%s=excluded.%s", field.Name, field.Name))

		// Extract value for Column
		res := gjson.GetBytes(bodyBytes, field.Name)
		var val any
		switch field.Type {
		case TypeInt:
			val = res.Int()
		case TypeFloat:
			val = res.Float()
		case TypeBool:
			if res.Bool() {
				val = 1
			} else {
				val = 0
			}
		case TypeDateTime:
			v, err := validateAndNormalizeDateTime(res.String())
			if err != nil {
				return "", fmt.Errorf("field '%s' invalid date: %w", field.Name, err)
			}
			val = v
		default:
			val = res.String()
		}
		args = append(args, val)

		// STRIP field from Blob (Strict Split Storage)
		var err error
		blobBytes, err = sjson.DeleteBytes(blobBytes, field.Name)
		if err != nil {
			return "", fmt.Errorf("failed to strip field %s: %w", field.Name, err)
		}
	}

	// Inject the modified blob into args at index 1
	finalArgs := []any{docID, blobBytes}
	finalArgs = append(finalArgs, args[1:]...)

	query := fmt.Sprintf(
		"INSERT INTO main.%s (%s) VALUES (%s) ON CONFLICT(id) DO UPDATE SET %s",
		collection,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updateSets, ", "),
	)

	// 2. Fetch Old Data (Must Reconstruct) for Audit
	oldDataBytes, err := fetchAndReconstruct(tx, collection, docID, fields)
	var oldData sql.NullString
	if err == nil {
		oldData.String = string(oldDataBytes)
		oldData.Valid = true
	} else if err != sql.ErrNoRows {
		return "", fmt.Errorf("failed to read old data: %w", err)
	}

	opType := OperationUpdate
	if !oldData.Valid {
		opType = OperationInsert
	}

	// 3. Write Data to Main DB
	if _, err := tx.Exec(query, finalArgs...); err != nil {
		return "", fmt.Errorf("write failed: %w", err)
	}

	// 4. Write Full Audit Log
	_, err = tx.Exec(
		`INSERT INTO audit.changelog (operation, collection_name, document_id, new_data, old_data) VALUES (?, ?, ?, json(?), json(?))`,
		opType, collection, docID, bodyBytes, oldData,
	)
	if err != nil {
		return "", fmt.Errorf("audit log failed: %w", err)
	}

	return opType, nil
}

// ApplyUpdateDocument performs a "Merge Patch" operation within a transaction.
// It selectively updates Typed Columns and merges the rest into the JSON Blob.
func ApplyUpdateDocument(tx *sql.Tx, collection, docID string, patchBytes []byte) error {
	fields := GetCollectionFields(collection)
	patchResult := gjson.ParseBytes(patchBytes)

	// We need a patch for the blob that excludes the native columns
	blobPatchBytes := patchBytes

	updateParts := []string{}
	args := []any{}

	for _, field := range fields {
		valRes := patchResult.Get(field.Name)
		if valRes.Exists() {
			// 1. Add to SQL SET for Column
			updateParts = append(updateParts, fmt.Sprintf("%s = ?", field.Name))

			var val any
			switch field.Type {
			case TypeInt:
				val = valRes.Int()
			case TypeFloat:
				val = valRes.Float()
			case TypeBool:
				if valRes.Bool() {
					val = 1
				} else {
					val = 0
				}
			case TypeDateTime:
				v, err := validateAndNormalizeDateTime(valRes.String())
				if err != nil {
					return fmt.Errorf("field '%s' invalid date: %w", field.Name, err)
				}
				val = v
			default:
				val = valRes.String()
			}
			args = append(args, val)

			// 2. Remove from Blob Patch
			var err error
			blobPatchBytes, err = sjson.DeleteBytes(blobPatchBytes, field.Name)
			if err != nil {
				return fmt.Errorf("failed to process patch for %s: %w", field.Name, err)
			}
		}
	}

	// Only apply jsonb_patch if there is data left in the patch
	if len(blobPatchBytes) > 2 { // "{}" is 2 bytes
		updateParts = append(updateParts, "data = jsonb_patch(data, jsonb(?))")
		args = append(args, blobPatchBytes)
	}

	if len(updateParts) == 0 {
		return nil // Nothing to update
	}

	args = append(args, docID)

	// 1. Get Old Data
	oldDataBytes, err := fetchAndReconstruct(tx, collection, docID, fields)
	if err != nil {
		return fmt.Errorf("document not found: %w", err)
	}

	// 2. Execute SQL Update
	sqlStr := fmt.Sprintf("UPDATE main.%s SET %s WHERE id = ?", collection, strings.Join(updateParts, ", "))
	if _, err := tx.Exec(sqlStr, args...); err != nil {
		return fmt.Errorf("update exec failed: %w", err)
	}

	// 3. Get New Data (Full Reconstruction) to log
	newDataBytes, err := fetchAndReconstruct(tx, collection, docID, fields)
	if err != nil {
		return fmt.Errorf("failed to reconstruct new state: %w", err)
	}

	// 4. Audit Log
	_, err = tx.Exec(
		`INSERT INTO audit.changelog (operation, collection_name, document_id, new_data, old_data) VALUES (?, ?, ?, json(?), json(?))`,
		"UPDATE", collection, docID, newDataBytes, oldDataBytes,
	)
	if err != nil {
		return fmt.Errorf("audit log failed: %w", err)
	}

	return nil
}

// ApplyDeleteDocument removes a document and logs the deletion within a transaction.
func ApplyDeleteDocument(tx *sql.Tx, collection, docID string) error {
	// 1. Get Full Old Data
	fields := GetCollectionFields(collection)
	oldDataBytes, err := fetchAndReconstruct(tx, collection, docID, fields)
	if err != nil {
		return fmt.Errorf("document not found or read error: %w", err)
	}

	// 2. Delete
	if _, err := tx.Exec(fmt.Sprintf("DELETE FROM main.%s WHERE id = ?", collection), docID); err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	// 3. Audit
	if _, err := tx.Exec(`INSERT INTO audit.changelog (operation, collection_name, document_id, new_data, old_data) VALUES (?, ?, ?, NULL, json(?))`, "DELETE", collection, docID, oldDataBytes); err != nil {
		return fmt.Errorf("audit log failed: %w", err)
	}

	return nil
}
