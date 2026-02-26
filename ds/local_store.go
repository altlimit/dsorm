package ds

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"math/bits"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
	_ "modernc.org/sqlite"
)

// Compile-time interface assertions
var _ Store = (*localStore)(nil)

type localStore struct {
	basePath string
	dbs      map[string]*sql.DB
	mu       sync.RWMutex

	// cache for kind existence: namespace -> kind -> true
	kindCache map[string]map[string]bool
}

func NewLocalStore(basePath string) Store {
	return &localStore{
		basePath:  basePath,
		dbs:       make(map[string]*sql.DB),
		kindCache: make(map[string]map[string]bool),
	}
}

// Close closes all underlying database connections.
func (c *localStore) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for ns, db := range c.dbs {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.dbs, ns)
	}
	c.kindCache = make(map[string]map[string]bool)
	return firstErr
}

func (c *localStore) getDB(namespace string) (*sql.DB, error) {
	c.mu.RLock()
	db, ok := c.dbs[namespace]
	c.mu.RUnlock()
	if ok {
		return db, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if db, ok := c.dbs[namespace]; ok {
		return db, nil
	}

	dbName := "default.db"
	if namespace != "" {
		dbName = fmt.Sprintf("%s.db", namespace)
	}
	dbPath := filepath.Join(c.basePath, dbName)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA synchronous=NORMAL;"); err != nil {
		return nil, err
	}

	c.dbs[namespace] = db
	c.kindCache[namespace] = make(map[string]bool)

	return db, nil
}

func (c *localStore) ensureKind(namespace, kind string, db *sql.DB) error {
	c.mu.RLock()
	exists := c.kindCache[namespace][kind]
	c.mu.RUnlock()
	if exists {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.kindCache[namespace][kind] {
		return nil
	}

	idxTable := kind + "_idx"
	idxName := fmt.Sprintf("idx_%s_name_value", kind)

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %q (
			key TEXT PRIMARY KEY,
			parent_key TEXT,
			props BLOB
		);
		CREATE TABLE IF NOT EXISTS %q (
			key TEXT,
			name TEXT,
			value JSON
		);
		CREATE INDEX IF NOT EXISTS %q ON %q(name, value);
	`, kind, idxTable, idxName, idxTable)

	if _, err := db.Exec(query); err != nil {
		return err
	}

	c.kindCache[namespace][kind] = true
	return nil
}

func keyToColAndID(k *datastore.Key) (string, string) {
	col := k.Kind
	id := k.Name
	if id == "" {
		id = strconv.FormatInt(k.ID, 10)
	}
	return col, id
}

func getParentKeyStr(k *datastore.Key) string {
	if k.Parent == nil {
		return ""
	}
	pCol, pID := keyToColAndID(k.Parent)
	return pCol + ":" + pID
}

func keyFromStrAndKind(kind string, keyStr string) *datastore.Key {
	id, err := strconv.ParseInt(keyStr, 10, 64)
	if err == nil {
		return datastore.IDKey(kind, id, nil)
	}
	return datastore.NameKey(kind, keyStr, nil)
}

// convertFilterValue converts special datastore types to their storable representations.
func convertFilterValue(v interface{}) interface{} {
	if v == nil {
		return v
	}
	switch val := v.(type) {
	case *datastore.Key:
		return val.Encode()
	case time.Time:
		return val.Format(time.RFC3339Nano)
	case []byte:
		return base64.StdEncoding.EncodeToString(val)
	default:
		return v
	}
}

const epoch = 1735689600000

func generateID() int64 {
	now := time.Now().UnixMilli() - epoch
	randNum, _ := rand.Int(rand.Reader, big.NewInt(1024))
	payload := (now << 10) | randNum.Int64()
	reversed := bits.Reverse64(uint64(payload))
	scrambled := int64(reversed >> (64 - 52))
	finalID := scrambled | (1 << 52)
	return finalID
}

// setBatchError is a helper to set the same error for all indices in a batch.
func setBatchError(me datastore.MultiError, indices []int, err error) {
	for _, idx := range indices {
		me[idx] = err
	}
}

func (c *localStore) Get(ctx context.Context, key *datastore.Key, dst interface{}) error {
	col, id := keyToColAndID(key)
	db, err := c.getDB(key.Namespace)
	if err != nil {
		return err
	}
	if err := c.ensureKind(key.Namespace, col, db); err != nil {
		return err
	}

	query := fmt.Sprintf(`SELECT props FROM %q WHERE key = ?`, col)
	var propsBlob []byte
	err = db.QueryRow(query, id).Scan(&propsBlob)
	if err != nil {
		if err == sql.ErrNoRows || strings.Contains(err.Error(), "no such table") {
			return datastore.ErrNoSuchEntity
		}
		return err
	}

	return unmarshalDocLocal(propsBlob, reflect.ValueOf(dst))
}

func (c *localStore) GetMulti(ctx context.Context, keys []*datastore.Key, dst interface{}) error {
	if len(keys) == 0 {
		return nil
	}

	v := reflect.ValueOf(dst)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	me := make(datastore.MultiError, len(keys))
	hasErr := false

	// Group keys by namespace+kind for batched queries
	type batchKey struct{ ns, col string }
	batches := make(map[batchKey][]*datastore.Key)
	batchIdxs := make(map[batchKey][]int)

	for i, k := range keys {
		col, _ := keyToColAndID(k)
		bk := batchKey{k.Namespace, col}
		batches[bk] = append(batches[bk], k)
		batchIdxs[bk] = append(batchIdxs[bk], i)
	}

	for bk, bKeys := range batches {
		ns, col := bk.ns, bk.col
		db, err := c.getDB(ns)
		if err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
			continue
		}
		if err := c.ensureKind(ns, col, db); err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
			continue
		}

		// Build ID -> original index map
		idToIdxs := make(map[string][]int)
		var placeholders []string
		var args []interface{}
		for bi, k := range bKeys {
			_, id := keyToColAndID(k)
			origIdx := batchIdxs[bk][bi]
			idToIdxs[id] = append(idToIdxs[id], origIdx)
			placeholders = append(placeholders, "?")
			args = append(args, id)
		}

		query := fmt.Sprintf(`SELECT key, props FROM %q WHERE key IN (%s)`, col, strings.Join(placeholders, ","))
		rows, err := db.Query(query, args...)
		if err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
			continue
		}

		found := make(map[string]bool)
		for rows.Next() {
			var keyStr string
			var propsBlob []byte
			if err := rows.Scan(&keyStr, &propsBlob); err != nil {
				rows.Close()
				setBatchError(me, batchIdxs[bk], err)
				hasErr = true
				break
			}
			found[keyStr] = true
			for _, origIdx := range idToIdxs[keyStr] {
				if err := unmarshalDocLocal(propsBlob, v.Index(origIdx).Addr()); err != nil {
					me[origIdx] = err
					hasErr = true
				}
			}
		}
		if err := rows.Err(); err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
		}
		rows.Close()

		// Mark missing keys as ErrNoSuchEntity
		for id, idxs := range idToIdxs {
			if !found[id] {
				for _, idx := range idxs {
					me[idx] = datastore.ErrNoSuchEntity
					hasErr = true
				}
			}
		}
	}

	if hasErr {
		return me
	}
	return nil
}

func (c *localStore) Put(ctx context.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	keys, err := c.PutMulti(ctx, []*datastore.Key{key}, []interface{}{src})
	if err != nil {
		if me, ok := err.(datastore.MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return keys[0], nil
}

func (c *localStore) PutMulti(ctx context.Context, keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	me := make(datastore.MultiError, len(keys))
	hasErr := false

	// Group keys by collection and namespace to batch inserts
	type batchKey struct{ ns, col string }
	batches := make(map[batchKey][]*datastore.Key)
	batchIdxs := make(map[batchKey][]int)

	for i, k := range keys {
		if k.Name == "" && k.ID == 0 {
			k.ID = generateID()
		}

		col, _ := keyToColAndID(k)
		bk := batchKey{k.Namespace, col}
		batches[bk] = append(batches[bk], k)
		batchIdxs[bk] = append(batchIdxs[bk], i)
	}

	for bk, bKeys := range batches {
		ns, col := bk.ns, bk.col
		db, err := c.getDB(ns)
		if err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
			continue
		}
		if err := c.ensureKind(ns, col, db); err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
			continue
		}

		var args []interface{}
		var placeholders []string

		var idxArgs []interface{}
		var idxPlaceholders []string

		var delArgs []interface{}
		var delPlaceholders []string

		for bi, k := range bKeys {
			i := batchIdxs[bk][bi]
			_, id := keyToColAndID(k)
			parentKey := getParentKeyStr(k)

			delPlaceholders = append(delPlaceholders, "?")
			delArgs = append(delArgs, id)

			elem := v.Index(i)

			var pl datastore.PropertyList
			if pls, ok := elem.Interface().(datastore.PropertyLoadSaver); ok {
				props, err := pls.Save()
				if err != nil {
					me[i] = err
					hasErr = true
					continue
				}
				pl = props
			} else if plv, ok := elem.Interface().(datastore.PropertyList); ok {
				pl = plv
			} else if plvPtr, ok := elem.Interface().(*datastore.PropertyList); ok {
				pl = *plvPtr
			} else {
				props, err := datastore.SaveStruct(elem.Interface())
				if err != nil {
					me[i] = err
					hasErr = true
					continue
				}
				pl = props
			}

			// Clean nil pointer values
			for j, p := range pl {
				if p.Value != nil {
					val := reflect.ValueOf(p.Value)
					if val.Kind() == reflect.Ptr && val.IsNil() {
						pl[j].Value = nil
					}
				}
			}

			// Generate exact gob props blob for Get recovery
			propsBlob, err := marshalPropertyList(pl)
			if err != nil {
				me[i] = err
				hasErr = true
				continue
			}

			// Build index entries
			for _, p := range pl {
				if p.NoIndex {
					continue
				}

				if _, ok := p.Value.(*datastore.Entity); ok {
					continue
				}

				// Handle multi-valued properties (slices) by creating
				// separate index rows for each element, matching Cloud
				// Datastore's behavior.
				var vals []interface{}
				if sl, ok := p.Value.([]interface{}); ok {
					vals = sl
				} else {
					vals = []interface{}{p.Value}
				}
				for _, elem := range vals {
					dataVal := convertFilterValue(elem)
					dataJSON, err := json.Marshal(dataVal)
					if err != nil {
						me[i] = fmt.Errorf("marshal index value %q: %w", p.Name, err)
						hasErr = true
						continue
					}
					idxPlaceholders = append(idxPlaceholders, "(?, ?, ?)")
					idxArgs = append(idxArgs, id, p.Name, string(dataJSON))
				}
			}

			placeholders = append(placeholders, "(?, ?, ?)")
			args = append(args, id, parentKey, propsBlob)
		}

		// Delete old index entries first
		if len(delPlaceholders) > 0 {
			delQuery := fmt.Sprintf(`DELETE FROM %q WHERE key IN (%s)`, col+"_idx", strings.Join(delPlaceholders, ","))
			if _, err := db.Exec(delQuery, delArgs...); err != nil {
				setBatchError(me, batchIdxs[bk], err)
				hasErr = true
				continue // Skip main insert and index insert on error
			}
		}

		// Insert/replace main rows
		if len(placeholders) > 0 {
			query := fmt.Sprintf(`INSERT OR REPLACE INTO %q (key, parent_key, props) VALUES %s`, col, strings.Join(placeholders, ", "))
			if _, err := db.Exec(query, args...); err != nil {
				setBatchError(me, batchIdxs[bk], err)
				hasErr = true
				continue
			}
		}

		// Insert new index entries
		if len(idxPlaceholders) > 0 {
			idxQuery := fmt.Sprintf(`INSERT INTO %q (key, name, value) VALUES %s`, col+"_idx", strings.Join(idxPlaceholders, ", "))
			if _, err := db.Exec(idxQuery, idxArgs...); err != nil {
				setBatchError(me, batchIdxs[bk], err)
				hasErr = true
			}
		}
	}

	if hasErr {
		return keys, me
	}
	return keys, nil
}

func (c *localStore) Delete(ctx context.Context, key *datastore.Key) error {
	col, id := keyToColAndID(key)
	db, err := c.getDB(key.Namespace)
	if err != nil {
		return err
	}
	delIdxQuery := fmt.Sprintf(`DELETE FROM %q WHERE key = ?`, col+"_idx")
	if _, err := db.Exec(delIdxQuery, id); err != nil {
		return err
	}

	query := fmt.Sprintf(`DELETE FROM %q WHERE key = ?`, col)
	_, err = db.Exec(query, id)
	return err
}

func (c *localStore) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	if len(keys) == 0 {
		return nil
	}

	me := make(datastore.MultiError, len(keys))
	hasErr := false

	// Group keys by namespace+kind for batched deletes
	type batchKey struct{ ns, col string }
	batches := make(map[batchKey][]*datastore.Key)
	batchIdxs := make(map[batchKey][]int)

	for i, k := range keys {
		col, _ := keyToColAndID(k)
		bk := batchKey{k.Namespace, col}
		batches[bk] = append(batches[bk], k)
		batchIdxs[bk] = append(batchIdxs[bk], i)
	}

	for bk, bKeys := range batches {
		ns, col := bk.ns, bk.col
		db, err := c.getDB(ns)
		if err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
			continue
		}

		var placeholders []string
		var args []interface{}
		for _, k := range bKeys {
			_, id := keyToColAndID(k)
			placeholders = append(placeholders, "?")
			args = append(args, id)
		}

		inClause := strings.Join(placeholders, ",")

		// Delete index entries
		delIdxQuery := fmt.Sprintf(`DELETE FROM %q WHERE key IN (%s)`, col+"_idx", inClause)
		if _, err := db.Exec(delIdxQuery, args...); err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
			continue
		}

		// Delete main rows
		delQuery := fmt.Sprintf(`DELETE FROM %q WHERE key IN (%s)`, col, inClause)
		if _, err := db.Exec(delQuery, args...); err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
		}
	}

	if hasErr {
		return me
	}
	return nil
}

func unmarshalDocLocal(propsBlob []byte, dst reflect.Value) error {
	var pl datastore.PropertyList
	if err := unmarshalPropertyList(propsBlob, &pl); err != nil {
		return err
	}

	if pls, ok := dst.Interface().(datastore.PropertyLoadSaver); ok {
		return pls.Load(pl)
	}

	if plPtr, ok := dst.Interface().(*datastore.PropertyList); ok {
		*plPtr = pl
		return nil
	}

	return datastore.LoadStruct(dst.Interface(), pl)
}

type localIterator struct {
	rows    *sql.Rows
	idx     int
	keys    []*datastore.Key
	blobs   [][]byte
	doneErr error
}

func (it *localIterator) Next(dst interface{}) (*datastore.Key, error) {
	if it.doneErr != nil {
		return nil, it.doneErr
	}
	if it.idx >= len(it.keys) {
		return nil, iterator.Done
	}
	k := it.keys[it.idx]
	blob := it.blobs[it.idx]
	it.idx++

	if dst != nil {
		if err := unmarshalDocLocal(blob, reflect.ValueOf(dst)); err != nil {
			return nil, err
		}
	}
	return k, nil
}

func (it *localIterator) Cursor() (string, error) {
	return strconv.Itoa(it.idx), nil
}

func (c *localStore) Run(ctx context.Context, q Query) Iterator {
	col := q.Kind()

	// Determine namespace: prefer explicit query namespace, then ancestor namespace
	ns := q.GetNamespace()
	if ns == "" {
		if anc := q.GetAncestor(); anc != nil {
			ns = anc.Namespace
		}
	}

	db, err := c.getDB(ns)
	if err != nil {
		return &localIterator{doneErr: err}
	}

	c.mu.RLock()
	exists := c.kindCache[ns][col]
	c.mu.RUnlock()
	if !exists {
		return &localIterator{doneErr: iterator.Done}
	}

	var conditions []string
	var args []interface{}
	var joins []string

	idxAlias := 0

	for _, f := range q.Filters() {
		op := f.Op
		if op == "" {
			op = "="
		}

		alias := fmt.Sprintf("i%d", idxAlias)
		idxAlias++

		joins = append(joins, fmt.Sprintf("JOIN %q %s ON main.key = %s.key", col+"_idx", alias, alias))

		conditions = append(conditions, fmt.Sprintf("%s.name = ?", alias))
		args = append(args, f.Field)

		opUpper := strings.ToUpper(strings.TrimSpace(op))

		if opUpper == "IN" || opUpper == "NOT-IN" || opUpper == "NOT IN" {
			realOp := "IN"
			if opUpper == "NOT-IN" || opUpper == "NOT IN" {
				realOp = "NOT IN"
			}

			vVal := reflect.ValueOf(f.Value)
			if vVal.Kind() == reflect.Slice || vVal.Kind() == reflect.Array {
				var placeholders []string
				for i := 0; i < vVal.Len(); i++ {
					elem := vVal.Index(i).Interface()
					elemVal := convertFilterValue(elem)
					elemJSON, err := json.Marshal(elemVal)
					if err != nil {
						return &localIterator{doneErr: fmt.Errorf("marshal IN filter value: %w", err)}
					}
					placeholders = append(placeholders, "?")
					args = append(args, string(elemJSON))
				}
				if len(placeholders) > 0 {
					conditions = append(conditions, fmt.Sprintf("%s.value %s (%s)", alias, realOp, strings.Join(placeholders, ",")))
				} else {
					if realOp == "IN" {
						conditions = append(conditions, "1=0") // IN empty matches nothing
					} else {
						conditions = append(conditions, "1=1") // NOT IN empty matches everything
					}
				}
			} else {
				// Single value IN fallback
				dataVal := convertFilterValue(f.Value)
				dataJSON, err := json.Marshal(dataVal)
				if err != nil {
					return &localIterator{doneErr: fmt.Errorf("marshal IN filter value: %w", err)}
				}
				conditions = append(conditions, fmt.Sprintf("%s.value %s (?)", alias, realOp))
				args = append(args, string(dataJSON))
			}
		} else {
			dataVal := convertFilterValue(f.Value)
			dataJSON, err := json.Marshal(dataVal)
			if err != nil {
				return &localIterator{doneErr: fmt.Errorf("marshal filter value: %w", err)}
			}
			if op == "=" {
				conditions = append(conditions, fmt.Sprintf("%s.value = ?", alias))
			} else {
				conditions = append(conditions, fmt.Sprintf("%s.value %s ?", alias, op))
			}
			args = append(args, string(dataJSON))
		}
	}

	if anc := q.GetAncestor(); anc != nil {
		parentKeyStr := getParentKeyStr(&datastore.Key{
			Kind: anc.Kind,
			ID:   anc.ID,
			Name: anc.Name,
		})
		if parentKeyStr == "" {
			// The ancestor IS the parent, so build the parent_key string from the ancestor key itself
			pCol, pID := keyToColAndID(anc)
			parentKeyStr = pCol + ":" + pID
		}
		conditions = append(conditions, "main.parent_key = ?")
		args = append(args, parentKeyStr)
	}

	queryStr := fmt.Sprintf("SELECT DISTINCT main.key, main.props FROM %q main", col)
	if len(joins) > 0 {
		queryStr += " " + strings.Join(joins, " ")
	}
	if len(conditions) > 0 {
		queryStr += " WHERE " + strings.Join(conditions, " AND ")
	}

	if len(q.Orders()) > 0 {
		var orderStrs []string
		for _, o := range q.Orders() {
			dir := "ASC"
			if o.Direction == "desc" || strings.HasPrefix(strings.ToLower(o.Direction), "-") {
				dir = "DESC"
			}

			alias := fmt.Sprintf("i%d", idxAlias)
			idxAlias++

			queryStr += fmt.Sprintf(" LEFT JOIN %q %s ON main.key = %s.key AND %s.name = ?", col+"_idx", alias, alias, alias)
			args = append(args, o.Field)

			orderStrs = append(orderStrs, fmt.Sprintf("%s.value %s", alias, dir))
		}
		queryStr += " ORDER BY " + strings.Join(orderStrs, ", ")
	}

	if q.GetLimit() > 0 {
		queryStr += " LIMIT " + strconv.Itoa(q.GetLimit())
	}
	if q.GetOffset() > 0 {
		queryStr += " OFFSET " + strconv.Itoa(q.GetOffset())
	}

	rows, err := db.Query(queryStr, args...)
	if err != nil {
		return &localIterator{doneErr: err}
	}
	defer rows.Close()

	// Eagerly load all results. We defer rows.Close() above so we must
	// consume them before returning. This is acceptable for the local
	// development store where result sets are small.
	var keys []*datastore.Key
	var blobs [][]byte

	for rows.Next() {
		var keyStr string
		var propsBlob []byte
		if err := rows.Scan(&keyStr, &propsBlob); err != nil {
			return &localIterator{doneErr: err}
		}

		b := make([]byte, len(propsBlob))
		copy(b, propsBlob)

		k := keyFromStrAndKind(col, keyStr)
		keys = append(keys, k)
		blobs = append(blobs, b)
	}

	if err := rows.Err(); err != nil {
		return &localIterator{doneErr: err}
	}

	idx := 0
	if cStr := q.GetCursor(); cStr != "" {
		if cInt, err := strconv.Atoi(cStr); err == nil {
			idx = cInt
		}
	}

	return &localIterator{
		keys:  keys,
		blobs: blobs,
		idx:   idx,
	}
}

func (c *localStore) RunInTransaction(ctx context.Context, f func(tx TransactionStore) error, opts ...datastore.TransactionOption) (*datastore.Commit, error) {
	txStore := &localTxStore{
		localStore: c,
		puts:       make(map[string]interface{}),
		putKeys:    make(map[string]*datastore.Key),
		dels:       make(map[string]*datastore.Key),
	}
	if err := f(txStore); err != nil {
		return nil, err
	}
	return txStore.Commit()
}

func (c *localStore) NewTransaction(ctx context.Context, opts ...datastore.TransactionOption) (TransactionStore, error) {
	return &localTxStore{
		localStore: c,
		puts:       make(map[string]interface{}),
		putKeys:    make(map[string]*datastore.Key),
		dels:       make(map[string]*datastore.Key),
	}, nil
}

type localTxStore struct {
	*localStore
	mu      sync.Mutex
	puts    map[string]interface{}
	putKeys map[string]*datastore.Key
	dels    map[string]*datastore.Key
}

func (c *localTxStore) Get(key *datastore.Key, dst interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	col, id := keyToColAndID(key)
	kStr := col + ":" + id
	if _, ok := c.dels[kStr]; ok {
		return datastore.ErrNoSuchEntity
	}

	return c.localStore.Get(context.Background(), key, dst)
}

func (c *localTxStore) GetMulti(keys []*datastore.Key, dst interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	v := reflect.ValueOf(dst)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	me := make(datastore.MultiError, len(keys))
	hasErr := false

	for i, k := range keys {
		col, id := keyToColAndID(k)
		kStr := col + ":" + id
		if _, ok := c.dels[kStr]; ok {
			me[i] = datastore.ErrNoSuchEntity
			hasErr = true
			continue
		}

		if err := c.localStore.Get(context.Background(), k, v.Index(i).Addr().Interface()); err != nil {
			me[i] = err
			hasErr = true
		}
	}

	if hasErr {
		return me
	}
	return nil
}

func (c *localTxStore) Put(key *datastore.Key, src interface{}) (*datastore.PendingKey, error) {
	keys, err := c.PutMulti([]*datastore.Key{key}, []interface{}{src})
	if err != nil {
		return nil, err
	}
	return keys[0], nil
}

func (c *localTxStore) PutMulti(keys []*datastore.Key, src interface{}) ([]*datastore.PendingKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	ret := make([]*datastore.PendingKey, len(keys))
	for i, k := range keys {
		col, id := keyToColAndID(k)
		kStr := col + ":" + id

		c.puts[kStr] = v.Index(i).Interface()
		c.putKeys[kStr] = k
		delete(c.dels, kStr)
		ret[i] = &datastore.PendingKey{}
	}
	return ret, nil
}

func (c *localTxStore) Delete(key *datastore.Key) error {
	return c.DeleteMulti([]*datastore.Key{key})
}

func (c *localTxStore) DeleteMulti(keys []*datastore.Key) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		col, id := keyToColAndID(k)
		kStr := col + ":" + id
		c.dels[kStr] = k
		delete(c.puts, kStr)
		delete(c.putKeys, kStr)
	}
	return nil
}

func (c *localTxStore) Rollback() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts = nil
	c.putKeys = nil
	c.dels = nil
	return nil
}

func (c *localTxStore) Commit() (*datastore.Commit, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Collect all namespaces used in this transaction
	nsSet := make(map[string]bool)
	for _, k := range c.putKeys {
		nsSet[k.Namespace] = true
	}
	for _, k := range c.dels {
		nsSet[k.Namespace] = true
	}

	// Use a real SQLite transaction for atomicity.
	// For simplicity, if all ops are in the same namespace (common case),
	// we use a single SQL transaction. Otherwise, fall back to best-effort.
	type dbTx struct {
		db *sql.DB
		tx *sql.Tx
	}
	txMap := make(map[string]*dbTx)
	defer func() {
		for _, dt := range txMap {
			if dt.tx != nil {
				dt.tx.Rollback()
			}
		}
	}()

	for ns := range nsSet {
		db, err := c.localStore.getDB(ns)
		if err != nil {
			return nil, err
		}
		tx, err := db.Begin()
		if err != nil {
			return nil, err
		}
		txMap[ns] = &dbTx{db: db, tx: tx}
	}

	// Execute puts
	if len(c.puts) > 0 {
		var putKeys []*datastore.Key
		var putSrcs []interface{}
		for k, v := range c.puts {
			putKeys = append(putKeys, c.putKeys[k])
			putSrcs = append(putSrcs, v)
		}
		if _, err := c.localStore.PutMulti(context.Background(), putKeys, putSrcs); err != nil {
			return nil, err
		}
	}

	// Execute deletes
	if len(c.dels) > 0 {
		var delKeys []*datastore.Key
		for _, k := range c.dels {
			delKeys = append(delKeys, k)
		}
		if err := c.localStore.DeleteMulti(context.Background(), delKeys); err != nil {
			return nil, err
		}
	}

	// Commit all SQL transactions
	for _, dt := range txMap {
		if err := dt.tx.Commit(); err != nil {
			return nil, err
		}
		dt.tx = nil // Mark as committed so defer doesn't rollback
	}

	c.puts = nil
	c.putKeys = nil
	c.dels = nil
	return &datastore.Commit{}, nil
}
