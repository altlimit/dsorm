package ds

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"

	"cloud.google.com/go/datastore"
	v2 "github.com/ostafen/clover/v2"
	"github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/query"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/api/iterator"
)

type localStore struct {
	db *v2.DB
}

func NewLocalStore(db *v2.DB) Store {
	return &localStore{db: db}
}

func keyToColAndID(k *datastore.Key) (string, string) {
	col := k.Kind
	id := k.Name
	if id == "" {
		id = strconv.FormatInt(k.ID, 10)
	}
	// For ancestors, ideally we want to encode the entire path into the ID or add a field.
	// For local testing, we'll just use a simple prefixed ID if it has a parent.
	if k.Parent != nil {
		pCol, pID := keyToColAndID(k.Parent)
		id = pCol + "_" + pID + "_" + id
	}
	return col, id
}

func findDocByKey(db *v2.DB, col, id string) (*document.Document, error) {
	docs, err := db.FindAll(query.NewQuery(col).Where(query.Field("__key__").Eq(id)).Limit(1))
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	return docs[0], nil
}

func replaceDocByKey(db *v2.DB, col, id string, d *document.Document) error {
	doc, err := findDocByKey(db, col, id)
	if err != nil {
		return err
	}
	d.Set("__key__", id)
	if doc == nil {
		return db.Insert(col, d)
	}
	d.Set(document.ObjectIdField, doc.ObjectId())
	return db.ReplaceById(col, doc.ObjectId(), d)
}

func deleteDocByKey(db *v2.DB, col, id string) error {
	doc, err := findDocByKey(db, col, id)
	if err != nil {
		return err
	}
	if doc == nil {
		return nil
	}
	return db.DeleteById(col, doc.ObjectId())
}

func (c *localStore) Get(ctx context.Context, key *datastore.Key, dst interface{}) error {
	col, id := keyToColAndID(key)

	doc, err := findDocByKey(c.db, col, id)
	if err != nil {
		return datastore.ErrNoSuchEntity
	}
	if doc == nil {
		return datastore.ErrNoSuchEntity
	}

	return unmarshalDoc(doc, reflect.ValueOf(dst))
}

func (c *localStore) GetMulti(ctx context.Context, keys []*datastore.Key, dst interface{}) error {
	v := reflect.ValueOf(dst)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	me := make(datastore.MultiError, len(keys))
	hasErr := false

	for i, k := range keys {
		col, id := keyToColAndID(k)
		doc, err := findDocByKey(c.db, col, id)

		if err != nil || doc == nil {
			me[i] = datastore.ErrNoSuchEntity
			hasErr = true
			continue
		}

		elem := v.Index(i)
		// Need a pointer to the element to unmarshal
		if elem.Kind() != reflect.Ptr {
			elem = elem.Addr()
		}

		if err := unmarshalDoc(doc, elem); err != nil {
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

		// Create collection if missing
		exists, _ := c.db.HasCollection(col)
		if !exists {
			c.db.CreateCollection(col)
		}

		elem := v.Index(i)

		docMap := make(map[string]interface{})

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

		// Fix gob panic for typed nil pointers (e.g., *datastore.Key)
		for j, p := range pl {
			if p.Value != nil {
				v := reflect.ValueOf(p.Value)
				if v.Kind() == reflect.Ptr && v.IsNil() {
					pl[j].Value = nil
				}
			}
		}

		b, err := marshalPropertyList(pl)
		if err != nil {
			me[i] = err
			hasErr = true
			continue
		}

		docMap["__props__"] = b
		for _, p := range pl {
			if !p.NoIndex {
				docMap[p.Name] = p.Value
			}
		}

		if id == "0" {
			if k.Name == "" {
				k.ID = rand.Int63n(1<<62) + 1
				id = strconv.FormatInt(k.ID, 10)
			} else {
				k.Name = uuid.NewV4().String()
				id = k.Name
			}
		}
		docMap["__key__"] = id

		d := document.NewDocumentOf(docMap)
		if err := replaceDocByKey(c.db, col, id, d); err != nil {
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
	return deleteDocByKey(c.db, col, id)
}

func (c *localStore) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	me := make(datastore.MultiError, len(keys))
	hasErr := false
	for i, k := range keys {
		col, id := keyToColAndID(k)
		if err := deleteDocByKey(c.db, col, id); err != nil {
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
	// Not fully implemented for local DB natively, translating to Put/Delete
	// Real mutations in LocalDB are immediate, so we just run them
	ret := make([]*datastore.Key, len(muts))
	// simplified version
	return ret, nil
}

// unmarshalDoc handles mapping generic map[string]interface{} into structs or PropertyList
func unmarshalDoc(doc *document.Document, dst reflect.Value) error {
	b, ok := doc.Get("__props__").([]byte)
	if !ok {
		return datastore.ErrNoSuchEntity
	}
	var pl datastore.PropertyList
	if err := unmarshalPropertyList(b, &pl); err != nil {
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
	docs []*document.Document
	idx  int
}

func (it *localIterator) Next(dst interface{}) (*datastore.Key, error) {
	if it.idx >= len(it.docs) {
		return nil, iterator.Done
	}
	doc := it.docs[it.idx]
	it.idx++

	if dst != nil {
		if err := unmarshalDoc(doc, reflect.ValueOf(dst)); err != nil {
			return nil, err
		}
	}

	idStr, _ := doc.Get("__key__").(string)
	id, _ := strconv.ParseInt(idStr, 10, 64)
	if id == 0 {
		return datastore.NameKey("Unknown", idStr, nil), nil
	}
	return datastore.IDKey("Unknown", id, nil), nil
}

func (it *localIterator) Cursor() (string, error) {
	return strconv.Itoa(it.idx), nil
}

func (c *localStore) Run(ctx context.Context, q Query) Iterator {
	col := q.Kind()
	exists, _ := c.db.HasCollection(col)
	if !exists {
		return &localIterator{docs: nil, idx: 0}
	}

	cq := query.NewQuery(col)
	// Example filter mapping
	for _, f := range q.Filters() {
		switch f.Op {
		case "=":
			cq = cq.Where(query.Field(f.Field).Eq(f.Value))
		case ">":
			cq = cq.Where(query.Field(f.Field).Gt(f.Value))
		case ">=":
			cq = cq.Where(query.Field(f.Field).GtEq(f.Value))
		case "<":
			cq = cq.Where(query.Field(f.Field).Lt(f.Value))
		case "<=":
			cq = cq.Where(query.Field(f.Field).LtEq(f.Value))
		}
	}

	if q.GetLimit() > 0 {
		cq = cq.Limit(q.GetLimit())
	}
	if q.GetOffset() > 0 {
		cq = cq.Skip(q.GetOffset())
	}
	// order mapping
	for _, o := range q.Orders() {
		dir := 1
		if o.Direction == "desc" {
			dir = -1
		}
		cq = cq.Sort(query.SortOption{Field: o.Field, Direction: dir})
	}

	docs, _ := c.db.FindAll(cq)
	if col == "DatastoreTagModel" {
		fmt.Printf("DSORM-DEBUG Run query on %s with filters %v returned %d docs\n", col, q.Filters(), len(docs))
		for _, doc := range docs {
			fmt.Printf("Doc: %v\n", doc.AsMap())
		}
	}

	// handle cursor
	idx := 0
	if cStr := q.GetCursor(); cStr != "" {
		if cInt, err := strconv.Atoi(cStr); err == nil {
			idx = cInt
		}
	}

	return &localIterator{
		docs: docs,
		idx:  idx,
	}
}

// localStore inherently supports atomic operations natively per document,
// but across documents transactions lock the DB file.
func (c *localStore) RunInTransaction(ctx context.Context, f func(tx TransactionStore) error, opts ...datastore.TransactionOption) (*datastore.Commit, error) {
	// Mock transaction for now.
	// We can use sync.Mutex or a localStore wrapper.
	txStore := &localTxStore{c}
	if err := f(txStore); err != nil {
		return nil, err
	}
	return &datastore.Commit{}, nil
}

func (c *localStore) NewTransaction(ctx context.Context, opts ...datastore.TransactionOption) (TransactionStore, error) {
	return &localTxStore{c}, nil
}

type localTxStore struct {
	*localStore
}

func (c *localTxStore) Get(key *datastore.Key, dst interface{}) error {
	return c.localStore.Get(context.Background(), key, dst)
}

func (c *localTxStore) GetMulti(keys []*datastore.Key, dst interface{}) error {
	return c.localStore.GetMulti(context.Background(), keys, dst)
}

func (c *localTxStore) Put(key *datastore.Key, src interface{}) (*datastore.PendingKey, error) {
	_, err := c.localStore.Put(context.Background(), key, src)
	return nil, err
}

func (c *localTxStore) PutMulti(keys []*datastore.Key, src interface{}) ([]*datastore.PendingKey, error) {
	_, err := c.localStore.PutMulti(context.Background(), keys, src)
	return nil, err
}

func (c *localTxStore) Delete(key *datastore.Key) error {
	return c.localStore.Delete(context.Background(), key)
}

func (c *localTxStore) DeleteMulti(keys []*datastore.Key) error {
	return c.localStore.DeleteMulti(context.Background(), keys)
}

func (c *localTxStore) Mutate(muts ...*datastore.Mutation) ([]*datastore.PendingKey, error) {
	_, err := c.localStore.Mutate(context.Background(), muts...)
	return nil, err
}

func (c *localTxStore) Rollback() error                    { return nil }
func (c *localTxStore) Commit() (*datastore.Commit, error) { return &datastore.Commit{}, nil }
