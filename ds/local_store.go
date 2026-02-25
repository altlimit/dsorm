package ds

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
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
		dbName = fmt.Sprintf("default-%s.db", namespace)
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
	v := reflect.ValueOf(dst)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	me := make(datastore.MultiError, len(keys))
	hasErr := false

	for i, k := range keys {
		err := c.Get(ctx, k, v.Index(i).Addr().Interface())
		if err != nil {
			me[i] = err
			hasErr = true
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
			k.ID = rand.Int63n(1<<62) + 1 // High ID generation
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
			for _, idx := range batchIdxs[bk] {
				me[idx] = err
				hasErr = true
			}
			continue
		}
		if err := c.ensureKind(ns, col, db); err != nil {
			for _, idx := range batchIdxs[bk] {
				me[idx] = err
				hasErr = true
			}
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

			// Build index map
			for _, p := range pl {
				if p.NoIndex {
					continue
				}

				var dataVal interface{} = p.Value
				if p.Value != nil {
					switch val := p.Value.(type) {
					case *datastore.Key:
						dataVal = val.Encode()
					case time.Time:
						dataVal = val.Format(time.RFC3339Nano)
					case []byte:
						dataVal = base64.StdEncoding.EncodeToString(val)
					case *datastore.Entity:
						continue
					}
				}

				dataJSON, err := json.Marshal(dataVal)
				if err == nil {
					idxPlaceholders = append(idxPlaceholders, "(?, ?, ?)")
					idxArgs = append(idxArgs, id, p.Name, string(dataJSON))
				}
			}

			placeholders = append(placeholders, "(?, ?, ?)")
			args = append(args, id, parentKey, propsBlob)
		}

		if len(delPlaceholders) > 0 {
			delQuery := fmt.Sprintf(`DELETE FROM %q WHERE key IN (%s)`, col+"_idx", strings.Join(delPlaceholders, ","))
			if _, err := db.Exec(delQuery, delArgs...); err != nil {
				for _, idx := range batchIdxs[bk] {
					me[idx] = err
					hasErr = true
				}
			}
		}

		if len(placeholders) > 0 {
			query := fmt.Sprintf(`INSERT OR REPLACE INTO %q (key, parent_key, props) VALUES %s`, col, strings.Join(placeholders, ", "))
			if _, err := db.Exec(query, args...); err != nil {
				for _, idx := range batchIdxs[bk] {
					me[idx] = err
					hasErr = true
				}
			}
		}

		if len(idxPlaceholders) > 0 {
			idxQuery := fmt.Sprintf(`INSERT INTO %q (key, name, value) VALUES %s`, col+"_idx", strings.Join(idxPlaceholders, ", "))
			if _, err := db.Exec(idxQuery, idxArgs...); err != nil {
				for _, idx := range batchIdxs[bk] {
					me[idx] = err
					hasErr = true
				}
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
	me := make(datastore.MultiError, len(keys))
	hasErr := false
	for i, k := range keys {
		if err := c.Delete(ctx, k); err != nil {
			me[i] = err
			hasErr = true
		}
	}
	if hasErr {
		return me
	}
	return nil
}

func (c *localStore) Mutate(ctx context.Context, muts ...*datastore.Mutation) ([]*datastore.Key, error) {
	ret := make([]*datastore.Key, len(muts))
	return ret, nil
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
	ns := ""
	if anc := q.GetAncestor(); anc != nil {
		ns = anc.Namespace
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

		var dataVal interface{} = f.Value
		if f.Value != nil {
			switch v := f.Value.(type) {
			case *datastore.Key:
				dataVal = v.Encode()
			case time.Time:
				dataVal = v.Format(time.RFC3339Nano)
			case []byte:
				dataVal = base64.StdEncoding.EncodeToString(v)
			}
		}

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
					elemVal := elem
					switch ev := elem.(type) {
					case *datastore.Key:
						elemVal = ev.Encode()
					case time.Time:
						elemVal = ev.Format(time.RFC3339Nano)
					case []byte:
						elemVal = base64.StdEncoding.EncodeToString(ev)
					}
					elemJSON, _ := json.Marshal(elemVal)
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
				dataJSON, _ := json.Marshal(dataVal)
				conditions = append(conditions, fmt.Sprintf("%s.value %s (?)", alias, realOp))
				args = append(args, string(dataJSON))
			}
		} else {
			dataJSON, _ := json.Marshal(dataVal)
			if op == "=" {
				conditions = append(conditions, fmt.Sprintf("%s.value = ?", alias))
			} else {
				conditions = append(conditions, fmt.Sprintf("%s.value %s ?", alias, op))
			}
			args = append(args, string(dataJSON))
		}
	}

	if anc := q.GetAncestor(); anc != nil {
		_, pID := keyToColAndID(anc)
		conditions = append(conditions, "main.parent_key = ?")
		args = append(args, pID)
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
	muts    []*datastore.Mutation
}

func (c *localTxStore) Get(key *datastore.Key, dst interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	col, id := keyToColAndID(key)
	kStr := col + ":" + id
	if _, ok := c.dels[kStr]; ok {
		return datastore.ErrNoSuchEntity
	}

	// Depending on Datastore semantics, changes in TX might not be visible to Get.
	// But dsorm uses standard transactional Get. We just read from DB.
	return c.localStore.Get(context.Background(), key, dst)
}

func (c *localTxStore) GetMulti(keys []*datastore.Key, dst interface{}) error {
	return c.localStore.GetMulti(context.Background(), keys, dst)
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

func (c *localTxStore) Mutate(muts ...*datastore.Mutation) ([]*datastore.PendingKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.muts = append(c.muts, muts...)
	return make([]*datastore.PendingKey, len(muts)), nil
}

func (c *localTxStore) Rollback() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts = nil
	c.putKeys = nil
	c.dels = nil
	c.muts = nil
	return nil
}

func (c *localTxStore) Commit() (*datastore.Commit, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

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

	if len(c.dels) > 0 {
		var delKeys []*datastore.Key
		for _, k := range c.dels {
			delKeys = append(delKeys, k)
		}
		if err := c.localStore.DeleteMulti(context.Background(), delKeys); err != nil {
			return nil, err
		}
	}

	if len(c.muts) > 0 {
		if _, err := c.localStore.Mutate(context.Background(), c.muts...); err != nil {
			return nil, err
		}
	}

	c.puts = nil
	c.putKeys = nil
	c.dels = nil
	c.muts = nil
	return &datastore.Commit{}, nil
}
