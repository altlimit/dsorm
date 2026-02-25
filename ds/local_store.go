package ds

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/datastore"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/api/iterator"
	_ "modernc.org/sqlite"
)

type localStore struct {
	basePath string
	dbs      map[string]*sql.DB
	mu       sync.RWMutex

	// cache for kind existence: namespace -> kind -> true
	kindCache map[string]map[string]bool
	// cache for index fields: namespace -> kind -> field -> true
	idxCache map[string]map[string]map[string]bool
}

func NewLocalStore(basePath string) Store {
	return &localStore{
		basePath:  basePath,
		dbs:       make(map[string]*sql.DB),
		kindCache: make(map[string]map[string]bool),
		idxCache:  make(map[string]map[string]map[string]bool),
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
	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous=NORMAL;")

	c.dbs[namespace] = db
	c.kindCache[namespace] = make(map[string]bool)
	c.idxCache[namespace] = make(map[string]map[string]bool)

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

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q (
		_key TEXT PRIMARY KEY,
		_id INTEGER,
		_idstr TEXT,
		data JSON,
		_props BLOB
	)`, kind)
	if _, err := db.Exec(query); err != nil {
		return err
	}

	c.kindCache[namespace][kind] = true
	c.idxCache[namespace][kind] = make(map[string]bool)
	return nil
}

func (c *localStore) ensureIndex(namespace, kind, field string, db *sql.DB) error {
	c.mu.RLock()
	exists := c.idxCache[namespace][kind][field]
	c.mu.RUnlock()
	if exists {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.idxCache[namespace][kind][field] {
		return nil
	}

	idxName := fmt.Sprintf("idx_%s_%s", kind, field)
	query := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %q ON %q(json_extract(data, '$.%s'))`, idxName, kind, field)
	if _, err := db.Exec(query); err != nil {
		return err
	}

	c.idxCache[namespace][kind][field] = true
	return nil
}

func keyToColAndID(k *datastore.Key) (string, string) {
	col := k.Kind
	id := k.Name
	if id == "" {
		id = strconv.FormatInt(k.ID, 10)
	}
	if k.Parent != nil {
		pCol, pID := keyToColAndID(k.Parent)
		id = pCol + "_" + pID + "_" + id
	}
	return col, id
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

	query := fmt.Sprintf(`SELECT _props FROM %q WHERE _key = ?`, col)
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

	for i, k := range keys {
		col, id := keyToColAndID(k)
		db, err := c.getDB(k.Namespace)
		if err != nil {
			me[i] = err
			hasErr = true
			continue
		}
		if err := c.ensureKind(k.Namespace, col, db); err != nil {
			me[i] = err
			hasErr = true
			continue
		}

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

		for j, p := range pl {
			if p.Value != nil {
				val := reflect.ValueOf(p.Value)
				if val.Kind() == reflect.Ptr && val.IsNil() {
					pl[j].Value = nil
				}
			}
		}

		propsBlob, err := marshalPropertyList(pl)
		if err != nil {
			me[i] = err
			hasErr = true
			continue
		}

		docMap := make(map[string]interface{})
		for _, p := range pl {
			if !p.NoIndex {
				docMap[p.Name] = p.Value
				if err := c.ensureIndex(k.Namespace, col, p.Name, db); err != nil {
					// Ignore index creation errors for now
				}
			}
		}

		dataJSON, err := json.Marshal(docMap)
		if err != nil {
			me[i] = err
			hasErr = true
			continue
		}

		if id == "0" || id == "" {
			if k.Name == "" {
				k.ID = rand.Int63n(1<<62) + 1
				id = strconv.FormatInt(k.ID, 10)
			} else {
				k.Name = uuid.NewV4().String()
				id = k.Name
			}
			col, id = keyToColAndID(k)
		}

		query := fmt.Sprintf(`INSERT OR REPLACE INTO %q (_key, _id, _idstr, data, _props) VALUES (?, ?, ?, ?, ?)`, col)
		if _, err := db.Exec(query, id, k.ID, k.Name, dataJSON, propsBlob); err != nil {
			me[i] = err
			hasErr = true
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
	if err := c.ensureKind(key.Namespace, col, db); err != nil {
		return err
	}
	query := fmt.Sprintf(`DELETE FROM %q WHERE _key = ?`, col)
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

	for _, f := range q.Filters() {
		op := f.Op
		if op == "" {
			op = "="
		}
		conditions = append(conditions, fmt.Sprintf("json_extract(data, '$.%s') %s ?", f.Field, op))
		args = append(args, f.Value)
	}

	if anc := q.GetAncestor(); anc != nil {
		_, pID := keyToColAndID(anc)
		conditions = append(conditions, "_key LIKE ?")
		args = append(args, pID+"_%")
	}

	queryStr := fmt.Sprintf("SELECT _id, _idstr, _props FROM %q", col)
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
			orderStrs = append(orderStrs, fmt.Sprintf("json_extract(data, '$.%s') %s", o.Field, dir))
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
		var idInt sql.NullInt64
		var idStr sql.NullString
		var propsBlob []byte
		if err := rows.Scan(&idInt, &idStr, &propsBlob); err != nil {
			return &localIterator{doneErr: err}
		}

		k := datastore.NameKey(col, idStr.String, nil)
		if idInt.Int64 != 0 {
			k = datastore.IDKey(col, idInt.Int64, nil)
		}
		keys = append(keys, k)
		blobs = append(blobs, propsBlob)
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
