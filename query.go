package dsorm

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/altlimit/dsorm/ds"
)

// QueryBuilder represents a common query interface for dsorm.
type QueryBuilder struct {
	kind      string
	filters   []ds.Filter
	orders    []ds.Order
	limit     int
	offset    int
	keysOnly  bool
	ancestor  *datastore.Key
	cursorStr string
	namespace string
}

// NewQuery creates a new query for a specific kind.
func NewQuery(kind string) *QueryBuilder {
	return &QueryBuilder{kind: kind}
}

// FilterField adds a field-specific filter to the query.
func (q *QueryBuilder) FilterField(fieldName, operator string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, ds.Filter{
		Field: fieldName,
		Op:    operator,
		Value: value,
	})
	return q
}

// Namespace sets the namespace for the query.
func (q *QueryBuilder) Namespace(ns string) *QueryBuilder {
	q.namespace = ns
	return q
}

// Order adds an order to the query.
func (q *QueryBuilder) Order(fieldName string) *QueryBuilder {
	dir := "asc"
	if len(fieldName) > 0 && fieldName[0] == '-' {
		dir = "desc"
		fieldName = fieldName[1:]
	}
	q.orders = append(q.orders, ds.Order{
		Field:     fieldName,
		Direction: dir,
	})
	return q
}

// Limit sets the maximum number of items to return.
func (q *QueryBuilder) Limit(limit int) *QueryBuilder {
	q.limit = limit
	return q
}

// Offset sets the number of items to skip.
func (q *QueryBuilder) Offset(offset int) *QueryBuilder {
	q.offset = offset
	return q
}

// KeysOnly makes the query return only keys.
func (q *QueryBuilder) KeysOnly() *QueryBuilder {
	q.keysOnly = true
	return q
}

// Ancestor sets the ancestor datastore key to queries.
func (q *QueryBuilder) Ancestor(ancestor *datastore.Key) *QueryBuilder {
	q.ancestor = ancestor
	return q
}

// Start sets the cursor string where the query will begin.
func (q *QueryBuilder) Start(cursor string) *QueryBuilder {
	q.cursorStr = cursor
	return q
}

// Data Getters for drivers
func (q *QueryBuilder) Kind() string                { return q.kind }
func (q *QueryBuilder) Filters() []ds.Filter        { return q.filters }
func (q *QueryBuilder) Orders() []ds.Order          { return q.orders }
func (q *QueryBuilder) GetLimit() int               { return q.limit }
func (q *QueryBuilder) GetOffset() int              { return q.offset }
func (q *QueryBuilder) IsKeysOnly() bool            { return q.keysOnly }
func (q *QueryBuilder) GetAncestor() *datastore.Key { return q.ancestor }
func (q *QueryBuilder) GetCursor() string           { return q.cursorStr }
func (q *QueryBuilder) GetNamespace() string        { return q.namespace }

// Hash generates a deterministic hash of the query builder state.
func (q *QueryBuilder) Hash() string {
	h := sha256.New()

	// Write basic properties
	h.Write([]byte(fmt.Sprintf("kind:%s|ns:%s|limit:%d|offset:%d|keysOnly:%t|cursor:%s|", q.kind, q.namespace, q.limit, q.offset, q.keysOnly, q.cursorStr)))

	if q.ancestor != nil {
		h.Write([]byte(fmt.Sprintf("ancestor:%s,%s,%d|", q.ancestor.Kind, q.ancestor.Name, q.ancestor.ID)))
	}

	for _, f := range q.filters {
		h.Write([]byte(fmt.Sprintf("filter:%s,%s,%v|", f.Field, f.Op, f.Value)))
	}

	for _, o := range q.orders {
		h.Write([]byte(fmt.Sprintf("order:%s,%s|", o.Field, o.Direction)))
	}

	return hex.EncodeToString(h.Sum(nil))
}
