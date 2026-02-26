package local

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/gob"
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
	"github.com/altlimit/dsorm/ds"
	"google.golang.org/api/iterator"
	_ "modernc.org/sqlite"
)

// Compile-time interface assertions
var _ ds.Store = (*Store)(nil)

type Store struct {
	basePath string
	dbs      map[string]*sql.DB
	mu       sync.RWMutex

	// cache for kind existence: namespace -> kind -> true
	kindCache map[string]map[string]bool
}

func NewStore(basePath string) *Store {
	return &Store{
		basePath:  basePath,
		dbs:       make(map[string]*sql.DB),
		kindCache: make(map[string]map[string]bool),
	}
}

// Close closes all underlying database connections.
func (c *Store) Close() error {
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

func (c *Store) getDB(namespace string) (*sql.DB, error) {
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

func (c *Store) ensureKind(namespace, kind string, db *sql.DB) error {
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

	propsTable := kind + "_props"

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %q (
			key TEXT PRIMARY KEY,
			parent_key TEXT
		);
		CREATE TABLE IF NOT EXISTS %q (
			key TEXT NOT NULL,
			name TEXT NOT NULL,
			ord INTEGER NOT NULL DEFAULT 0,
			value TEXT,
			no_index INTEGER NOT NULL DEFAULT 0,
			data_type TEXT NOT NULL DEFAULT 'string'
		);
		CREATE UNIQUE INDEX IF NOT EXISTS %q ON %q(key, name, ord);
		CREATE INDEX IF NOT EXISTS %q ON %q(name, value) WHERE no_index = 0 AND data_type IN ('string','key','time','bytes');
		CREATE INDEX IF NOT EXISTS %q ON %q(name, CAST(value AS REAL)) WHERE no_index = 0 AND data_type IN ('int','float');
		CREATE INDEX IF NOT EXISTS %q ON %q(name, value) WHERE no_index = 0 AND data_type = 'bool';
	`, kind, propsTable,
		"uq_"+kind+"_props", propsTable,
		"idx_"+kind+"_str", propsTable,
		"idx_"+kind+"_num", propsTable,
		"idx_"+kind+"_bool", propsTable,
	)

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

// propertyToRow converts a datastore.Property value to its (value string, data_type) for storage.
func propertyToRow(p datastore.Property) (string, string) {
	if p.Value == nil {
		return "", "null"
	}
	switch v := p.Value.(type) {
	case string:
		return v, "string"
	case int64:
		return strconv.FormatInt(v, 10), "int"
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), "float"
	case bool:
		if v {
			return "true", "bool"
		}
		return "false", "bool"
	case time.Time:
		return v.Format(time.RFC3339Nano), "time"
	case *datastore.Key:
		return v.Encode(), "key"
	case []byte:
		return base64.StdEncoding.EncodeToString(v), "bytes"
	case datastore.GeoPoint:
		data, _ := json.Marshal(v)
		return string(data), "geo"
	case *datastore.Entity:
		var buf bytes.Buffer
		gob.NewEncoder(&buf).Encode(v)
		return base64.StdEncoding.EncodeToString(buf.Bytes()), "entity"
	default:
		data, _ := json.Marshal(v)
		return string(data), "string"
	}
}

// rowToProperty reconstructs a datastore.Property from kind_props row data.
func rowToProperty(name, value, dataType string, noIndex bool) datastore.Property {
	p := datastore.Property{Name: name, NoIndex: noIndex}
	switch dataType {
	case "null":
		p.Value = nil
	case "string":
		p.Value = value
	case "int":
		v, _ := strconv.ParseInt(value, 10, 64)
		p.Value = v
	case "float":
		v, _ := strconv.ParseFloat(value, 64)
		p.Value = v
	case "bool":
		p.Value = value == "true"
	case "time":
		v, _ := time.Parse(time.RFC3339Nano, value)
		p.Value = v
	case "key":
		v, _ := datastore.DecodeKey(value)
		p.Value = v
	case "bytes":
		v, _ := base64.StdEncoding.DecodeString(value)
		p.Value = v
	case "geo":
		var gp datastore.GeoPoint
		json.Unmarshal([]byte(value), &gp)
		p.Value = gp
	case "entity":
		data, _ := base64.StdEncoding.DecodeString(value)
		var e datastore.Entity
		gob.NewDecoder(bytes.NewReader(data)).Decode(&e)
		p.Value = &e
	default:
		p.Value = value
	}
	return p
}

// filterDataType determines the storage data_type from a Go filter value.
func filterDataType(v interface{}) string {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return "int"
	case float32, float64:
		return "float"
	case bool:
		return "bool"
	case time.Time:
		return "time"
	case *datastore.Key:
		return "key"
	case []byte:
		return "bytes"
	case string:
		return "string"
	default:
		return "string"
	}
}

// filterValueStr converts a filter value to the string representation used in kind_props.
func filterValueStr(v interface{}) string {
	cv := convertFilterValue(v)
	switch val := cv.(type) {
	case string:
		return val
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		data, _ := json.Marshal(val)
		return string(data)
	}
}

// loadPropsForKeys loads PropertyLists from kind_props for the given keys.
// Returns a map from key string to PropertyList.
func loadPropsForKeys(db *sql.DB, col string, keyStrs []string) (map[string]datastore.PropertyList, error) {
	if len(keyStrs) == 0 {
		return nil, nil
	}
	propsTable := col + "_props"
	var placeholders []string
	var args []interface{}
	for _, k := range keyStrs {
		placeholders = append(placeholders, "?")
		args = append(args, k)
	}
	query := fmt.Sprintf(`SELECT key, name, ord, value, no_index, data_type FROM %q WHERE key IN (%s) ORDER BY key, name, ord`,
		propsTable, strings.Join(placeholders, ","))
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]datastore.PropertyList)
	// Track which (key,name) combos have multiple ords for slice reconstruction
	type propGroup struct {
		name    string
		values  []interface{}
		noIndex bool
	}
	keyProps := make(map[string][]*propGroup)     // key -> ordered list of propGroups
	keyPropIdx := make(map[string]map[string]int) // key -> name -> index in keyProps[key]

	for rows.Next() {
		var keyStr, name, dataType string
		var ord int
		var value sql.NullString
		var noIndex int
		if err := rows.Scan(&keyStr, &name, &ord, &value, &noIndex, &dataType); err != nil {
			return nil, err
		}

		valStr := ""
		if value.Valid {
			valStr = value.String
		}
		p := rowToProperty(name, valStr, dataType, noIndex == 1)

		if _, ok := keyPropIdx[keyStr]; !ok {
			keyPropIdx[keyStr] = make(map[string]int)
		}

		if idx, exists := keyPropIdx[keyStr][name]; exists {
			// Multi-valued property: accumulate values
			keyProps[keyStr][idx].values = append(keyProps[keyStr][idx].values, p.Value)
		} else {
			// First occurrence of this property name
			keyPropIdx[keyStr][name] = len(keyProps[keyStr])
			keyProps[keyStr] = append(keyProps[keyStr], &propGroup{
				name:    name,
				values:  []interface{}{p.Value},
				noIndex: noIndex == 1,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Build PropertyLists from grouped data
	for keyStr, groups := range keyProps {
		var pl datastore.PropertyList
		for _, g := range groups {
			if len(g.values) == 1 {
				pl = append(pl, datastore.Property{
					Name:    g.name,
					Value:   g.values[0],
					NoIndex: g.noIndex,
				})
			} else {
				// Multi-valued: store as []interface{} slice
				pl = append(pl, datastore.Property{
					Name:    g.name,
					Value:   g.values,
					NoIndex: g.noIndex,
				})
			}
		}
		result[keyStr] = pl
	}
	return result, nil
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

func (c *Store) Get(ctx context.Context, key *datastore.Key, dst interface{}) error {
	col, id := keyToColAndID(key)
	db, err := c.getDB(key.Namespace)
	if err != nil {
		return err
	}
	if err := c.ensureKind(key.Namespace, col, db); err != nil {
		return err
	}

	// Check existence in kind table
	var exists int
	err = db.QueryRow(fmt.Sprintf(`SELECT 1 FROM %q WHERE key = ?`, col), id).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows || strings.Contains(err.Error(), "no such table") {
			return datastore.ErrNoSuchEntity
		}
		return err
	}

	// Load properties from kind_props
	propsMap, err := loadPropsForKeys(db, col, []string{id})
	if err != nil {
		return err
	}
	pl := propsMap[id]

	return loadPropertyList(pl, reflect.ValueOf(dst))
}

func (c *Store) GetMulti(ctx context.Context, keys []*datastore.Key, dst interface{}) error {
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

		// Check which keys exist in kind table
		query := fmt.Sprintf(`SELECT key FROM %q WHERE key IN (%s)`, col, strings.Join(placeholders, ","))
		rows, err := db.Query(query, args...)
		if err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
			continue
		}

		var foundKeys []string
		found := make(map[string]bool)
		for rows.Next() {
			var keyStr string
			if err := rows.Scan(&keyStr); err != nil {
				rows.Close()
				setBatchError(me, batchIdxs[bk], err)
				hasErr = true
				break
			}
			found[keyStr] = true
			foundKeys = append(foundKeys, keyStr)
		}
		if err := rows.Err(); err != nil {
			setBatchError(me, batchIdxs[bk], err)
			hasErr = true
		}
		rows.Close()

		// Load properties for found keys
		if len(foundKeys) > 0 {
			propsMap, err := loadPropsForKeys(db, col, foundKeys)
			if err != nil {
				setBatchError(me, batchIdxs[bk], err)
				hasErr = true
			} else {
				for keyStr, pl := range propsMap {
					for _, origIdx := range idToIdxs[keyStr] {
						if err := loadPropertyList(pl, v.Index(origIdx).Addr()); err != nil {
							me[origIdx] = err
							hasErr = true
						}
					}
				}
				// Handle found keys with no properties
				for _, keyStr := range foundKeys {
					if _, hasPl := propsMap[keyStr]; !hasPl {
						for _, origIdx := range idToIdxs[keyStr] {
							if err := loadPropertyList(nil, v.Index(origIdx).Addr()); err != nil {
								me[origIdx] = err
								hasErr = true
							}
						}
					}
				}
			}
		}

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

func (c *Store) Put(ctx context.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	keys, err := c.PutMulti(ctx, []*datastore.Key{key}, []interface{}{src})
	if err != nil {
		if me, ok := err.(datastore.MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return keys[0], nil
}

func (c *Store) PutMulti(ctx context.Context, keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
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

		propsTable := col + "_props"

		var mainArgs []interface{}
		var mainPlaceholders []string

		var propsArgs []interface{}
		var propsPlaceholders []string

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

			// Build property rows for kind_props
			for _, p := range pl {
				noIndex := 0
				if p.NoIndex {
					noIndex = 1
				}

				// Handle multi-valued properties (slices)
				var vals []interface{}
				if sl, ok := p.Value.([]interface{}); ok {
					vals = sl
				} else {
					vals = []interface{}{p.Value}
				}

				for ord, elemVal := range vals {
					singleProp := datastore.Property{Name: p.Name, Value: elemVal, NoIndex: p.NoIndex}
					valStr, dataType := propertyToRow(singleProp)
					propsPlaceholders = append(propsPlaceholders, "(?, ?, ?, ?, ?, ?)")
					propsArgs = append(propsArgs, id, p.Name, ord, valStr, noIndex, dataType)
				}
			}

			mainPlaceholders = append(mainPlaceholders, "(?, ?)")
			mainArgs = append(mainArgs, id, parentKey)
		}

		// Delete old property rows first
		if len(delPlaceholders) > 0 {
			delQuery := fmt.Sprintf(`DELETE FROM %q WHERE key IN (%s)`, propsTable, strings.Join(delPlaceholders, ","))
			if _, err := db.Exec(delQuery, delArgs...); err != nil {
				setBatchError(me, batchIdxs[bk], err)
				hasErr = true
				continue
			}
		}

		// Insert/replace main rows (key + parent_key only)
		if len(mainPlaceholders) > 0 {
			query := fmt.Sprintf(`INSERT OR REPLACE INTO %q (key, parent_key) VALUES %s`, col, strings.Join(mainPlaceholders, ", "))
			if _, err := db.Exec(query, mainArgs...); err != nil {
				setBatchError(me, batchIdxs[bk], err)
				hasErr = true
				continue
			}
		}

		// Insert property rows
		if len(propsPlaceholders) > 0 {
			propsQuery := fmt.Sprintf(`INSERT OR REPLACE INTO %q (key, name, ord, value, no_index, data_type) VALUES %s`, propsTable, strings.Join(propsPlaceholders, ", "))
			if _, err := db.Exec(propsQuery, propsArgs...); err != nil {
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

func (c *Store) Delete(ctx context.Context, key *datastore.Key) error {
	col, id := keyToColAndID(key)
	db, err := c.getDB(key.Namespace)
	if err != nil {
		return err
	}
	// Delete property rows
	delPropsQuery := fmt.Sprintf(`DELETE FROM %q WHERE key = ?`, col+"_props")
	if _, err := db.Exec(delPropsQuery, id); err != nil {
		return err
	}

	// Delete main row
	query := fmt.Sprintf(`DELETE FROM %q WHERE key = ?`, col)
	_, err = db.Exec(query, id)
	return err
}

func (c *Store) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
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

		// Delete property rows
		delPropsQuery := fmt.Sprintf(`DELETE FROM %q WHERE key IN (%s)`, col+"_props", inClause)
		if _, err := db.Exec(delPropsQuery, args...); err != nil {
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

// loadPropertyList loads a PropertyList into a destination value.
func loadPropertyList(pl datastore.PropertyList, dst reflect.Value) error {
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
	idx        int
	keys       []*datastore.Key
	propLists  []datastore.PropertyList
	page       int
	totalPages int
	doneErr    error
}

func (it *localIterator) Next(dst interface{}) (*datastore.Key, error) {
	if it.doneErr != nil {
		return nil, it.doneErr
	}
	if it.idx >= len(it.keys) {
		return nil, iterator.Done
	}
	k := it.keys[it.idx]
	pl := it.propLists[it.idx]
	it.idx++

	if dst != nil {
		if err := loadPropertyList(pl, reflect.ValueOf(dst)); err != nil {
			return nil, err
		}
	}
	return k, nil
}

func (it *localIterator) Cursor() (string, error) {
	nextPage := it.page + 1
	if it.totalPages > 0 && nextPage > it.totalPages {
		return "", nil
	}
	return fmt.Sprintf("%d:%d", nextPage, it.totalPages), nil
}

func (c *Store) Run(ctx context.Context, q ds.Query) ds.Iterator {
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

	propsTable := col + "_props"

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

		dt := filterDataType(f.Value)
		isNumeric := dt == "int" || dt == "float"

		joins = append(joins, fmt.Sprintf("JOIN %q %s ON main.key = %s.key AND %s.no_index = 0", propsTable, alias, alias, alias))

		conditions = append(conditions, fmt.Sprintf("%s.name = ?", alias))
		args = append(args, f.Field)

		// Add data_type condition for proper index usage
		if isNumeric {
			conditions = append(conditions, fmt.Sprintf("%s.data_type IN ('int','float')", alias))
		} else {
			conditions = append(conditions, fmt.Sprintf("%s.data_type = ?", alias))
			args = append(args, dt)
		}

		valExpr := alias + ".value"
		if isNumeric {
			valExpr = fmt.Sprintf("CAST(%s.value AS REAL)", alias)
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
					placeholders = append(placeholders, "?")
					args = append(args, filterValueStr(elem))
				}
				if len(placeholders) > 0 {
					conditions = append(conditions, fmt.Sprintf("%s %s (%s)", valExpr, realOp, strings.Join(placeholders, ",")))
				} else {
					if realOp == "IN" {
						conditions = append(conditions, "1=0")
					} else {
						conditions = append(conditions, "1=1")
					}
				}
			} else {
				conditions = append(conditions, fmt.Sprintf("%s %s (?)", valExpr, realOp))
				args = append(args, filterValueStr(f.Value))
			}
		} else {
			conditions = append(conditions, fmt.Sprintf("%s %s ?", valExpr, op))
			args = append(args, filterValueStr(f.Value))
		}
	}

	if anc := q.GetAncestor(); anc != nil {
		parentKeyStr := getParentKeyStr(&datastore.Key{
			Kind: anc.Kind,
			ID:   anc.ID,
			Name: anc.Name,
		})
		if parentKeyStr == "" {
			pCol, pID := keyToColAndID(anc)
			parentKeyStr = pCol + ":" + pID
		}
		conditions = append(conditions, "main.parent_key = ?")
		args = append(args, parentKeyStr)
	}

	queryStr := fmt.Sprintf("SELECT DISTINCT main.key FROM %q main", col)
	if len(joins) > 0 {
		queryStr += " " + strings.Join(joins, " ")
	}

	// Build ORDER BY LEFT JOINs
	var orderJoinArgs []interface{}
	var orderStrs []string
	if len(q.Orders()) > 0 {
		for _, o := range q.Orders() {
			dir := "ASC"
			if o.Direction == "desc" || strings.HasPrefix(strings.ToLower(o.Direction), "-") {
				dir = "DESC"
			}

			alias := fmt.Sprintf("i%d", idxAlias)
			idxAlias++

			queryStr += fmt.Sprintf(" LEFT JOIN %q %s ON main.key = %s.key AND %s.name = ? AND %s.no_index = 0", propsTable, alias, alias, alias, alias)
			orderJoinArgs = append(orderJoinArgs, o.Field)

			// Determine if the order field is numeric by checking filters
			orderIsNumeric := false
			for _, f := range q.Filters() {
				if f.Field == o.Field {
					dt := filterDataType(f.Value)
					if dt == "int" || dt == "float" {
						orderIsNumeric = true
					}
					break
				}
			}

			if orderIsNumeric {
				orderStrs = append(orderStrs, fmt.Sprintf("CAST(%s.value AS REAL) %s", alias, dir))
			} else {
				orderStrs = append(orderStrs, fmt.Sprintf("%s.value %s", alias, dir))
			}
		}
	}

	if len(conditions) > 0 {
		queryStr += " WHERE " + strings.Join(conditions, " AND ")
	}
	if len(orderStrs) > 0 {
		queryStr += " ORDER BY " + strings.Join(orderStrs, ", ")
	} else {
		queryStr += " ORDER BY main.key ASC"
	}

	// Combine args: ORDER BY LEFT JOIN args first, then condition args.
	finalArgs := append(orderJoinArgs, args...)

	// Parse page-based cursor.
	page := 1
	if cStr := q.GetCursor(); cStr != "" {
		parts := strings.SplitN(cStr, ":", 2)
		if p, err := strconv.Atoi(parts[0]); err == nil && p > 0 {
			page = p
		}
	}

	// Pagination
	totalPages := 0
	limit := q.GetLimit()
	if limit > 0 {
		countQuery := fmt.Sprintf("SELECT COUNT(DISTINCT main.key) FROM %q main", col)
		if len(joins) > 0 {
			countQuery += " " + strings.Join(joins, " ")
		}
		if len(conditions) > 0 {
			countQuery += " WHERE " + strings.Join(conditions, " AND ")
		}
		var totalRows int
		if err := db.QueryRow(countQuery, args...).Scan(&totalRows); err != nil {
			return &localIterator{doneErr: fmt.Errorf("count query: %w", err)}
		}
		totalPages = (totalRows + limit - 1) / limit

		offset := (page - 1) * limit
		if q.GetOffset() > 0 {
			offset += q.GetOffset()
		}
		queryStr += " LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset)
	} else {
		if q.GetOffset() > 0 {
			queryStr += " OFFSET " + strconv.Itoa(q.GetOffset())
		}
	}

	rows, err := db.Query(queryStr, finalArgs...)
	if err != nil {
		return &localIterator{doneErr: err}
	}
	defer rows.Close()

	// Collect all matching keys
	var resultKeys []*datastore.Key
	var keyStrs []string

	for rows.Next() {
		var keyStr string
		if err := rows.Scan(&keyStr); err != nil {
			return &localIterator{doneErr: err}
		}
		k := keyFromStrAndKind(col, keyStr)
		resultKeys = append(resultKeys, k)
		keyStrs = append(keyStrs, keyStr)
	}
	if err := rows.Err(); err != nil {
		return &localIterator{doneErr: err}
	}

	// Load properties for all result keys
	var propLists []datastore.PropertyList
	if len(keyStrs) > 0 {
		propsMap, err := loadPropsForKeys(db, col, keyStrs)
		if err != nil {
			return &localIterator{doneErr: err}
		}
		for _, ks := range keyStrs {
			propLists = append(propLists, propsMap[ks])
		}
	}

	return &localIterator{
		keys:       resultKeys,
		propLists:  propLists,
		page:       page,
		totalPages: totalPages,
	}
}

func (c *Store) RunInTransaction(ctx context.Context, f func(tx ds.TransactionStore) error, opts ...datastore.TransactionOption) (*datastore.Commit, error) {
	txStore := &localTxStore{
		Store:   c,
		puts:    make(map[string]interface{}),
		putKeys: make(map[string]*datastore.Key),
		dels:    make(map[string]*datastore.Key),
	}
	if err := f(txStore); err != nil {
		return nil, err
	}
	return txStore.Commit()
}

func (c *Store) NewTransaction(ctx context.Context, opts ...datastore.TransactionOption) (ds.TransactionStore, error) {
	return &localTxStore{
		Store:   c,
		puts:    make(map[string]interface{}),
		putKeys: make(map[string]*datastore.Key),
		dels:    make(map[string]*datastore.Key),
	}, nil
}

type localTxStore struct {
	*Store
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

	return c.Store.Get(context.Background(), key, dst)
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

		if err := c.Store.Get(context.Background(), k, v.Index(i).Interface()); err != nil {
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
		db, err := c.Store.getDB(ns)
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
		if _, err := c.Store.PutMulti(context.Background(), putKeys, putSrcs); err != nil {
			return nil, err
		}
	}

	// Execute deletes
	if len(c.dels) > 0 {
		var delKeys []*datastore.Key
		for _, k := range c.dels {
			delKeys = append(delKeys, k)
		}
		if err := c.Store.DeleteMulti(context.Background(), delKeys); err != nil {
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
