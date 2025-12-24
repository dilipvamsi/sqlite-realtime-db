// query.go
package realtime

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	json "github.com/goccy/go-json"
	"github.com/tidwall/gjson"
)

// Where represents a combined structure for a simple condition or a logical grouping.
// Pointers are used to distinguish between a simple clause (Field, Op are non-nil)
// and a logical clause (And or Or are non-nil).
type Where struct {
	// Fields for a simple condition
	Field *string `json:"field,omitempty"`
	Op    *string `json:"op,omitempty"`
	Value any     `json:"value,omitempty"`

	// Fields for a logical grouping
	And *[]Where `json:"$and,omitempty"`
	Or  *[]Where `json:"$or,omitempty"`
}

// OrderBy defines a single sorting criterion.
type OrderBy struct {
	Field     string `json:"field"`
	Direction string `json:"direction"`
}

// QueryDSL represents the full query structure sent by the client.
type QueryDSL struct {
	Where   *Where    `json:"where,omitempty"`
	OrderBy []OrderBy `json:"orderBy,omitempty"`
	Limit   int       `json:"limit,omitempty"`
	Offset  int       `json:"offset,omitempty"`
}

// Validate checks the structural and content integrity of the entire QueryDSL object.
func (q *QueryDSL) Validate() error {
	// 1. Validate the OrderBy clause
	for _, ob := range q.OrderBy {
		if !identifierSanitizer.MatchString(ob.Field) {
			return fmt.Errorf("invalid character in orderBy field: '%s'", ob.Field)
		}
	}

	// 2. Validate the Limit and Offset
	if q.Limit < 0 {
		return errors.New("limit cannot be negative")
	}
	if q.Offset < 0 {
		return errors.New("offset cannot be negative")
	}

	// 3. Delegate validation to the recursive Where.Validate method
	if q.Where != nil {
		if err := q.Where.Validate(); err != nil {
			return fmt.Errorf("where clause validation failed: %w", err)
		}
	}

	return nil
}

// Validate is a recursive method that checks the integrity of a Where clause.
func (w *Where) Validate() error {
	// A Where clause must be EXCLUSIVELY one of:
	// - A simple condition (field, op, value)
	// - A logical AND condition ($and)
	// - A logical OR condition ($or)

	modeCount := 0
	isSimple := w.Field != nil || w.Op != nil || w.Value != nil
	isAnd := w.And != nil
	isOr := w.Or != nil

	if isSimple {
		modeCount++
	}
	if isAnd {
		modeCount++
	}
	if isOr {
		modeCount++
	}

	if modeCount == 0 {
		return errors.New("clause cannot be empty")
	}
	if modeCount > 1 {
		return errors.New("clause cannot mix simple (field/op/value) and logical ($and/$or) conditions")
	}

	// --- Validate based on the detected mode ---

	if isSimple {
		// If it's a simple condition, all three parts are required.
		if w.Field == nil {
			return errors.New("simple condition requires 'field'")
		}
		if w.Op == nil {
			return errors.New("simple condition requires 'op'")
		}
		if w.Value == nil {
			return errors.New("simple condition requires 'value'")
		}

		// Validate the content of the simple condition.
		if !identifierSanitizer.MatchString(*w.Field) {
			return fmt.Errorf("invalid character in field name: '%s'", *w.Field)
		}
		switch *w.Op {
		case "==", "=", "!=", ">", ">=", "<", "<=":
			// Operator is valid
		default:
			return fmt.Errorf("unsupported operator: '%s'", *w.Op)
		}
	}

	if isAnd {
		// $and clause must not be an empty array
		if len(*w.And) == 0 {
			return errors.New("$and condition cannot be an empty array")
		}
		// Recursively validate each sub-clause
		for _, subClause := range *w.And {
			if err := subClause.Validate(); err != nil {
				return err
			}
		}
	}

	if isOr {
		// $or clause must not be an empty array
		if len(*w.Or) == 0 {
			return errors.New("$or condition cannot be an empty array")
		}
		// Recursively validate each sub-clause
		for _, subClause := range *w.Or {
			if err := subClause.Validate(); err != nil {
				return err
			}
		}
	}

	return nil
}

// --- New Helper Function for Parsing and Validation ---

// ParseAndValidateQuery combines strict JSON parsing with custom validation logic.
func ParseAndValidateQuery(jsonData json.RawMessage) (*QueryDSL, error) {
	var query QueryDSL

	// 1. Enforce strict parsing: fail if there are any unknown fields.
	decoder := json.NewDecoder(bytes.NewReader(jsonData))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&query); err != nil {
		return nil, fmt.Errorf("json parsing error: %w", err)
	}

	// 2. Run our custom validation logic on the successfully parsed struct.
	if err := query.Validate(); err != nil {
		return nil, fmt.Errorf("query validation error: %w", err)
	}

	return &query, nil
}

// buildQuery translates a QueryDSL object into a parameterized SQL query and a slice of arguments.
func buildQuery(collection string, dsl *QueryDSL) (string, []any, error) {
	// This sanitizer regex should be defined in your package, e.g., in a `var` block.
	// var collectionNameSanitizer = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if !collectionNameSanitizer.MatchString(collection) {
		return "", nil, fmt.Errorf("invalid collection name")
	}

	var args []any
	var whereClause string
	var err error

	// The logic here remains the same, but it now passes the typed struct.
	if dsl.Where != nil {
		whereClause, args, err = parseWhereClause(dsl.Where)
		if err != nil {
			return "", nil, err
		}
	}

	sql := fmt.Sprintf("SELECT id, data FROM %s", collection)
	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	// This sanitizer regex should also be defined in your package.
	// var identifierSanitizer = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if len(dsl.OrderBy) > 0 {
		sql += " ORDER BY "
		var orderByParts []string
		for _, ob := range dsl.OrderBy {
			if !identifierSanitizer.MatchString(ob.Field) {
				return "", nil, fmt.Errorf("invalid order by field: %s", ob.Field)
			}
			dir := "ASC"
			if strings.ToUpper(ob.Direction) == "DESC" {
				dir = "DESC"
			}
			orderByParts = append(orderByParts, fmt.Sprintf("json_extract(data, ?)%s", dir))
			args = append(args, "$."+ob.Field)
		}
		sql += strings.Join(orderByParts, ", ")
	}

	if dsl.Limit > 0 {
		sql += " LIMIT ?"
		args = append(args, dsl.Limit)
	}
	if dsl.Offset > 0 {
		sql += " OFFSET ?"
		args = append(args, dsl.Offset)
	}

	sql += ";"

	return sql, args, nil
}

// parseWhereClause is a recursive function that safely builds the WHERE clause from the typed Where struct.
func parseWhereClause(w *Where) (string, []any, error) {
	// Handle logical AND
	if w.And != nil {
		var parts []string
		var args []any
		for _, cond := range *w.And {
			// We pass the address of the condition struct in the slice
			subClause, subArgs, err := parseWhereClause(&cond)
			if err != nil {
				return "", nil, err
			}
			parts = append(parts, subClause)
			args = append(args, subArgs...)
		}
		return "(" + strings.Join(parts, " AND ") + ")", args, nil
	}

	// Handle logical OR
	if w.Or != nil {
		var parts []string
		var args []any
		for _, cond := range *w.Or {
			subClause, subArgs, err := parseWhereClause(&cond)
			if err != nil {
				return "", nil, err
			}
			parts = append(parts, subClause)
			args = append(args, subArgs...)
		}
		return "(" + strings.Join(parts, " OR ") + ")", args, nil
	}

	// Base case: handle a simple condition
	if w.Field != nil && w.Op != nil && w.Value != nil {
		field := *w.Field
		op := *w.Op
		value := w.Value

		if !identifierSanitizer.MatchString(field) {
			return "", nil, fmt.Errorf("invalid field name in condition: %s", field)
		}

		var safeOp string
		switch op {
		case "==", "=":
			safeOp = "="
		case "!=":
			safeOp = "!="
		case ">":
			safeOp = ">"
		case ">=":
			safeOp = ">="
		case "<":
			safeOp = "<"
		case "<=":
			safeOp = "<="
		default:
			return "", nil, fmt.Errorf("unsupported operator: %s", op)
		}

		sql := fmt.Sprintf("json_extract(data, ?)%s?", safeOp)
		args := []any{"$." + field, value}
		return sql, args, nil
	}

	return "", nil, fmt.Errorf("invalid where clause object")
}

// Helper to attempt numeric conversion
func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case string:
		// Attempt to parse string as float if the query expects a number comparison
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	}
	return 0, false
}

// evaluateWhere recursively evaluates if a document matches a typed 'where' clause.
func evaluateWhere(jsonBytes []byte, w *Where) (bool, error) {
	if len(jsonBytes) == 0 {
		return false, nil // Cannot evaluate a nil document for INSERT/UPDATE
	}
	// If there is no where clause, it's an automatic match.
	if w == nil {
		return true, nil
	}

	// Handle logical AND
	if w.And != nil {
		for _, cond := range *w.And {
			match, err := evaluateWhere(jsonBytes, &cond)
			if err != nil || !match {
				return false, err // Short-circuit on first false
			}
		}
		return true, nil
	}

	// Handle logical OR
	if w.Or != nil {
		for _, cond := range *w.Or {
			match, err := evaluateWhere(jsonBytes, &cond)
			if err == nil && match {
				return true, nil // Short-circuit on first true
			}
		}
		return false, nil
	}

	// Base case: evaluate a simple condition
	if w.Field != nil && w.Op != nil && w.Value != nil {
		field := *w.Field
		op := *w.Op
		queryValue := w.Value

		// OPTIMIZATION: gjson lookup (Zero Allocation)
		// GetBytes searches the JSON without parsing the whole tree.
		result := gjson.GetBytes(jsonBytes, field)

		if !result.Exists() {
			return false, nil // Field doesn't exist in the document.
		}

		// 1. Numeric Comparison
		// We check if the Query Value is a number.
		queryNum, queryIsNum := toFloat(queryValue)

		if queryIsNum && result.Type == gjson.Number {
			docNum := result.Float()

			switch op {
			case "==", "=":
				return docNum == queryNum, nil
			case "!=":
				return docNum != queryNum, nil
			case ">":
				return docNum > queryNum, nil
			case ">=":
				return docNum >= queryNum, nil
			case "<":
				return docNum < queryNum, nil
			case "<=":
				return docNum <= queryNum, nil
			}
		}
		// --- NUMERIC COMPARISON LOGIC END ---

		// 2. Fallback to String comparison for non-numbers
		docValStr := result.String()
		queryValStr := fmt.Sprintf("%v", queryValue)

		switch op {
		case "==", "=":
			return docValStr == queryValStr, nil
		case "!=":
			return docValStr != queryValStr, nil
		case ">":
			return docValStr > queryValStr, nil
		case ">=":
			return docValStr >= queryValStr, nil
		case "<":
			return docValStr < queryValStr, nil
		case "<=":
			return docValStr <= queryValStr, nil
		default:
			return false, fmt.Errorf("unsupported operator: %s", op)
		}
	}

	return false, fmt.Errorf("invalid where clause object for evaluation")
}
