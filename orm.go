package dsorm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/altlimit/dsorm/cache/memcache"
	"github.com/altlimit/dsorm/cache/memory"
	"github.com/altlimit/dsorm/cache/redis"
	ds "github.com/altlimit/dsorm/ds"
	"github.com/altlimit/dsorm/internal/encryption"
	"github.com/altlimit/dsorm/internal/structtag"
	"github.com/altlimit/dsorm/internal/util"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine/v2"
)

type (
	// Model defines the basic methods required for a struct to be managed by dsorm.
	Model interface {
		Init(context.Context, any)
		InitProps([]datastore.Property)
		IsNew() bool
	}

	OnLoad interface {
		OnLoad(context.Context) error
	}

	BeforeSave interface {
		BeforeSave(context.Context, Model) error
	}

	AfterSave interface {
		AfterSave(context.Context) error
	}

	// Base provides default implementation for Model interface and common fields.
	// Embed this in your struct to use dsorm.
	Base struct {
		Key *datastore.Key `datastore:"-" json:"-"`

		initProps []datastore.Property `datastore:"-"`
		entity    any                  `datastore:"-"`
		ctx       context.Context      `datastore:"-"`
	}
)

func (b *Base) Init(ctx context.Context, e any) {
	b.ctx = ctx
	if b.entity == nil {
		b.entity = e
	}
	if b.Key != nil {
		if kl, ok := e.(datastore.KeyLoader); ok {
			kl.LoadKey(b.Key)
		}
	}
}

// Load implements datastore.PropertyLoadSaver.
func (b *Base) Load(ps []datastore.Property) error {
	err := loadModel(b.entity, ps)
	if err != nil {
		return err
	}
	if ol, ok := b.entity.(OnLoad); ok {
		return ol.OnLoad(b.ctx)
	}
	return nil
}

// Save implements datastore.PropertyLoadSaver.
func (b *Base) Save() ([]datastore.Property, error) {
	if bs, ok := b.entity.(BeforeSave); ok {
		val := reflect.New(reflect.ValueOf(b.entity).Elem().Type()).Interface()
		ctx := b.ctx
		m, ok := val.(Model)
		if ok {
			m.Init(ctx, val)
		}
		if len(b.initProps) > 0 {
			if err := loadModel(val, b.initProps); err != nil {
				return nil, err
			}
			if v, ok := val.(datastore.KeyLoader); ok {
				v.LoadKey(b.Key)
			}
		}
		if err := bs.BeforeSave(ctx, m); err != nil {
			return nil, err
		}
	}
	return saveModel(b.entity)
}

// LoadKey implements datastore.KeyLoader.
func (b *Base) LoadKey(k *datastore.Key) error {
	b.Key = k
	t := reflect.TypeOf(b.entity)
	v := reflect.ValueOf(b.entity)
	if t.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	for _, field := range structtag.GetFieldsByTag(b.entity, "model") {
		tag := field.Tag
		switch tag {
		case "id":
			vv := v.Field(field.Index)
			var val reflect.Value
			switch vv.Kind() {
			case reflect.String:
				val = reflect.ValueOf(k.Name)
			case reflect.Int64:
				val = reflect.ValueOf(k.ID)
			}
			vv.Set(val)
		case "parent":
			vv := v.Field(field.Index)
			if p, ok := vv.Interface().(*datastore.Key); ok {
				if p != nil && p.Namespace != "" {
					k.Namespace = p.Namespace
				}
				vv.Set(reflect.ValueOf(k.Parent))
			}
		case "ns":
			if k.Namespace == "" {
				continue
			}
			vv := v.Field(field.Index)
			var val reflect.Value
			switch vv.Kind() {
			case reflect.String:
				val = reflect.ValueOf(k.Namespace)
			case reflect.Int64:
				n, err := strconv.ParseInt(k.Namespace, 10, 64)
				if err != nil {
					return err
				}
				val = reflect.ValueOf(n)
			}
			vv.Set(val)
		}
	}
	return nil
}

// InitProps stores initial properties for change tracking access.
func (b *Base) InitProps(props []datastore.Property) {
	b.initProps = props
}

func (b *Base) IsNew() bool {
	return len(b.initProps) == 0
}

func loadModel(e any, ps []datastore.Property) error {
	if m, ok := e.(Model); ok {
		m.InitProps(ps)
	}
	fields := make(map[string]*structtag.StructField)
	for _, field := range structtag.GetFieldsByTag(e, "marshal") {
		fields[field.Tag] = field
	}
	var props []datastore.Property
	t := reflect.TypeOf(e)
	v := reflect.ValueOf(e)
	if t.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	for _, p := range ps {
		if field, ok := fields[p.Name]; ok {
			vv := v.Field(field.Index)
			vt := vv.Type()
			if vt.Kind() == reflect.Ptr {
				vt = vt.Elem()
			}
			i := reflect.New(vt).Interface()
			val := p.Value
			if sKey, ok := field.Value("encrypt"); ok {
				// decrypt value first
				if enc, ok := val.(string); ok {
					var (
						err    error
						secret []byte
					)
					if sKey == "" {
						secretEnv := os.Getenv("DATASTORE_ENCRYPTION_KEY")
						if secretEnv == "" {
							return fmt.Errorf("datastore.loadModel: encryption key missing")
						}
						secret = []byte(secretEnv)
					} else {
						secret, err = base64.StdEncoding.DecodeString(sKey)
						if err != nil {
							return fmt.Errorf("datastore.loadModel: base64 decode error %v", err)
						}
					}
					val, err = encryption.Decrypt(secret, enc)
					if err != nil {
						return err
					}
				}
			}
			if err := json.Unmarshal([]byte(val.(string)), &i); err != nil {
				return err
			}
			nv := reflect.ValueOf(i)
			if nv.Kind() == reflect.Ptr && vv.Kind() != reflect.Ptr {
				nv = nv.Elem()
			}
			vv.Set(nv)
		} else {
			props = append(props, p)
		}
	}

	return datastore.LoadStruct(e, props)
}

func saveModel(e any) ([]datastore.Property, error) {
	t := reflect.TypeOf(e)
	v := reflect.ValueOf(e)
	if t.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	now := time.Now().UTC()
	m, ok := e.(Model)
	for _, field := range structtag.GetFieldsByTag(e, "model") {
		tag := field.Tag
		switch tag {
		case "created":
			if ok && m.IsNew() {
				vv := v.Field(field.Index)
				vv.Set(reflect.ValueOf(now))
			}
		case "modified":
			if ok {
				vv := v.Field(field.Index)
				vv.Set(reflect.ValueOf(now))
			}
		}
	}
	props, err := datastore.SaveStruct(e)
	if err != nil {
		return nil, err
	}
	for _, field := range structtag.GetFieldsByTag(e, "marshal") {
		tag := field.Tag
		vv := v.Field(field.Index)
		if vv.IsZero() {
			continue
		}
		b, err := json.Marshal(vv.Interface())
		if err != nil {
			return nil, err
		}
		prop := datastore.Property{
			Name:    tag,
			NoIndex: true,
		}
		val := string(b)
		if sKey, ok := field.Value("encrypt"); ok {
			var (
				err    error
				secret []byte
			)
			if sKey == "" {
				secretEnv := os.Getenv("DATASTORE_ENCRYPTION_KEY")
				if secretEnv == "" {
					return nil, fmt.Errorf("datastore.saveModel: encryption key missing")
				}
				secret = []byte(secretEnv)
			} else {
				secret, err = base64.StdEncoding.DecodeString(sKey)
				if err != nil {
					return nil, fmt.Errorf("datastore.saveModel: base64 decode error %v", err)
				}
			}
			enc, err := encryption.Encrypt(secret, val)
			if err != nil {
				return nil, err
			}
			val = enc
		}
		prop.Value = val
		props = append(props, prop)
	}
	return props, nil
}

// Client wraps datastore and cache operations.
type Client struct {
	client    *ds.Client
	rawClient *datastore.Client
}

type options struct {
	projectID       string
	cache           ds.Cache
	datastoreClient *datastore.Client
}

type Option func(*options)

func WithProjectID(id string) Option {
	return func(o *options) {
		o.projectID = id
	}
}

func WithCache(c ds.Cache) Option {
	return func(o *options) {
		o.cache = c
	}
}

func WithDatastoreClient(c *datastore.Client) Option {
	return func(o *options) {
		o.datastoreClient = c
	}
}

// New creates a new Client with options value.
// It auto-detects caching backend: App Engine (Memcache), Redis (env REDIS_ADDR), or Memory.
func New(ctx context.Context, opts ...Option) (*Client, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	var dsClient *datastore.Client
	var err error

	if o.datastoreClient != nil {
		dsClient = o.datastoreClient
	} else {
		if o.projectID == "" {
			o.projectID = os.Getenv("DATASTORE_PROJECT_ID")
		}
		if o.projectID == "" {
			o.projectID = os.Getenv("GOOGLE_CLOUD_PROJECT")
		}

		dsClient, err = datastore.NewClient(ctx, o.projectID)
		if err != nil {
			return nil, err
		}
	}

	if o.cache == nil {
		if appengine.IsAppEngine() || appengine.IsDevAppServer() {
			o.cache = memcache.NewCache()
		} else if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
			pool, err := redis.NewPool(redisAddr, "", 0)
			if err != nil {
				return nil, err
			}
			o.cache, err = redis.NewCache(ctx, pool)
			if err != nil {
				return nil, err
			}
		} else {
			o.cache = memory.NewCache()
		}
	}

	dsoClient, err := ds.NewClient(ctx, o.cache, ds.WithDatastoreClient(dsClient), ds.WithCachePrefix(o.projectID+":"))
	if err != nil {
		return nil, err
	}

	return &Client{
		rawClient: dsClient,
		client:    dsoClient,
	}, nil
}

// Keys returns a slice of keys for the given slice of entities.
func (db *Client) Keys(val interface{}) []*datastore.Key {
	t := reflect.TypeOf(val)
	var keys []*datastore.Key
	if t.Kind() == reflect.Slice {
		v := reflect.ValueOf(val)
		for i := 0; i < v.Len(); i++ {
			keys = append(keys, db.Key(v.Index(i).Interface()))
		}
	}
	return keys
}

// Key returns the datastore key for the given entity.
func (db *Client) Key(val interface{}) *datastore.Key {
	t := reflect.TypeOf(val)
	v := reflect.ValueOf(val)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	key := &datastore.Key{Kind: t.Name()}
	for _, field := range structtag.GetFieldsByTag(val, "model") {
		tag := field.Tag
		vv := v.Field(field.Index)
		switch tag {
		case "id":
			switch vv.Kind() {
			case reflect.String:
				key.Name = vv.String()
			case reflect.Int64:
				key.ID = vv.Int()
			}
		case "parent":
			if p, ok := vv.Interface().(*datastore.Key); ok {
				key.Parent = p
				if p != nil && p.Namespace != "" {
					key.Namespace = p.Namespace
				}
			}
		case "ns":
			switch vv.Kind() {
			case reflect.String:
				key.Namespace = vv.String()
			case reflect.Int64:
				v := vv.Int()
				if v > 0 {
					key.Namespace = strconv.FormatInt(v, 10)
				}
			}
		}
	}
	return key
}

// Get loads the entity for the given key/struct into the struct.
func (db *Client) Get(ctx context.Context, val interface{}) error {
	if m, ok := val.(Model); ok {
		m.Init(ctx, val)
	}
	key := db.Key(val)
	if err := db.client.Get(ctx, key, val); err != nil {
		return err
	}
	if v, ok := val.(datastore.KeyLoader); ok {
		v.LoadKey(key)
	}
	return nil
}

// GetMulti loads multiple entities.
func (db *Client) GetMulti(ctx context.Context, keys []*datastore.Key, vals interface{}) error {
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("datastore.DB.GetMulti: must be slice type not '%v'", v.Kind())
	}
	for i, k := range keys {
		if k != nil {
			vv := v.Index(i)
			vv.Set(reflect.New(v.Index(i).Type().Elem()))
			e := vv.Interface()
			if m, ok := e.(Model); ok {
				m.Init(ctx, e)
			}
		}
	}
	err := db.client.GetMulti(ctx, keys, vals)
	if err != nil {
		if mErr, ok := err.(datastore.MultiError); ok {
			for i, e := range mErr {
				if e == datastore.ErrNoSuchEntity {
					v.Index(i).Set(reflect.Zero(v.Index(i).Type()))
				} else if e != nil {
					return err
				}
			}
		} else {
			return err
		}
	}
	loadKeys(v, keys)
	return nil
}

// Put saves an entity.
func (db *Client) Put(ctx context.Context, val interface{}) error {
	if m, ok := val.(Model); ok {
		m.Init(ctx, val)
	}
	k := db.Key(val)
	k, err := db.client.Put(ctx, k, val)
	if err != nil {
		return err
	}
	if v, ok := val.(datastore.KeyLoader); ok {
		v.LoadKey(k)
	}
	if as, ok := val.(AfterSave); ok {
		return as.AfterSave(ctx)
	}
	return nil
}

// PutMulti saves multiple entities.
func (db *Client) PutMulti(ctx context.Context, vals interface{}) error {
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("datastore.DB.PutMulti: must be slice type not '%v'", v.Kind())
	}
	var as []AfterSave
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i).Interface()
		if m, ok := e.(Model); ok {
			m.Init(ctx, e)
		}
		if a, ok := e.(AfterSave); ok {
			as = append(as, a)
		}
	}
	keys := db.Keys(vals)
	keys, err := db.client.PutMulti(ctx, keys, vals)
	if err != nil {
		return err
	}
	loadKeys(v, keys)

	if len(as) > 0 {
		return util.Task(20, as, func(e AfterSave) error {
			return e.AfterSave(ctx)
		})
	}

	return nil
}

func (db *Client) Delete(ctx context.Context, val interface{}) error {
	return db.client.Delete(ctx, db.Key(val))
}

func (db *Client) DeleteMulti(ctx context.Context, vals interface{}) error {
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("datastore.db.DeleteMulti: must be slice type not '%v'", v.Kind())
	}
	var keys []*datastore.Key
	if vKeys, ok := vals.([]*datastore.Key); ok {
		keys = vKeys
	} else {
		keys = db.Keys(vals)
	}
	return db.client.DeleteMulti(ctx, keys)
}

func (db *Client) QueryKeys(ctx context.Context, q *datastore.Query, cursor string) ([]*datastore.Key, string, error) {
	var keys []*datastore.Key
	if cursor != "" {
		c, err := datastore.DecodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		q = q.Start(c)
	}
	it := db.client.Run(ctx, q)
	for {
		k, err := it.Next(nil)
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, "", err
		}
		keys = append(keys, k)
	}
	next, err := it.Cursor()
	if err != nil {
		return nil, "", err
	}
	return keys, next.String(), nil
}

// Transaction wraps ds.Transaction to provide dsorm functionality (ID mapping, lifecycle hooks).
type Transaction struct {
	tx     *ds.Transaction
	client *Client
	ctx    context.Context
}

// Get loads entity to val within the transaction.
func (t *Transaction) Get(val interface{}) error {
	if m, ok := val.(Model); ok {
		m.Init(t.ctx, val)
	}
	key := t.client.Key(val)
	if err := t.tx.Get(key, val); err != nil {
		return err
	}
	if v, ok := val.(datastore.KeyLoader); ok {
		v.LoadKey(key)
	}
	return nil
}

// GetMulti loads multiple entities within the transaction.
func (t *Transaction) GetMulti(keys []*datastore.Key, vals interface{}) error {
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("dsorm.Transaction.GetMulti: must be slice type not '%v'", v.Kind())
	}
	for i, k := range keys {
		if k != nil {
			vv := v.Index(i)
			vv.Set(reflect.New(v.Index(i).Type().Elem()))
			e := vv.Interface()
			if m, ok := e.(Model); ok {
				m.Init(t.ctx, e)
			}
		}
	}
	err := t.tx.GetMulti(keys, vals)
	if err != nil {
		if mErr, ok := err.(datastore.MultiError); ok {
			for i, e := range mErr {
				if e == datastore.ErrNoSuchEntity {
					v.Index(i).Set(reflect.Zero(v.Index(i).Type()))
				} else if e != nil {
					return err
				}
			}
		} else {
			return err
		}
	}
	loadKeys(v, keys)
	return nil
}

// Put saves an entity within the transaction.
func (t *Transaction) Put(val interface{}) error {
	if m, ok := val.(Model); ok {
		m.Init(t.ctx, val)
	}
	k := t.client.Key(val)
	if _, err := t.tx.Put(k, val); err != nil {
		return err
	}
	// Note: tx.Put returns a PendingKey, but dsorm usually relies on the struct being updated or key known.
	// Since we are inside a tx, ID allocation for incomplete keys happens at commit.
	// However, datastore.Transaction.Put returns (*PendingKey, error).
	// We don't really have a way to propagate the pending key to the struct until commit?
	// Actually typical DS behavior with incomplete keys in TX is you get them after commit.
	if v, ok := val.(datastore.KeyLoader); ok {
		v.LoadKey(k)
	}
	if as, ok := val.(AfterSave); ok {
		return as.AfterSave(t.ctx)
	}
	return nil
}

// PutMulti saves multiple entities within the transaction.
func (t *Transaction) PutMulti(vals interface{}) error {
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("dsorm.Transaction.PutMulti: must be slice type not '%v'", v.Kind())
	}
	var as []AfterSave
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i).Interface()
		if m, ok := e.(Model); ok {
			m.Init(t.ctx, e)
		}
		if a, ok := e.(AfterSave); ok {
			as = append(as, a)
		}
	}
	keys := t.client.Keys(vals)
	if _, err := t.tx.PutMulti(keys, vals); err != nil {
		return err
	}
	loadKeys(v, keys)

	if len(as) > 0 {
		return util.Task(20, as, func(e AfterSave) error {
			return e.AfterSave(t.ctx)
		})
	}
	return nil
}

// Delete deletes an entity within the transaction.
func (t *Transaction) Delete(val interface{}) error {
	return t.tx.Delete(t.client.Key(val))
}

// DeleteMulti deletes multiple entities within the transaction.
func (t *Transaction) DeleteMulti(vals interface{}) error {
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("dsorm.Transaction.DeleteMulti: must be slice type not '%v'", v.Kind())
	}
	var keys []*datastore.Key
	if vKeys, ok := vals.([]*datastore.Key); ok {
		keys = vKeys
	} else {
		keys = t.client.Keys(vals)
	}
	return t.tx.DeleteMulti(keys)
}

// Transact runs a function in a transaction.
func (db *Client) Transact(ctx context.Context, f func(tx *Transaction) error) (*datastore.Commit, error) {
	return db.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		dsormTx := &Transaction{
			tx:     tx,
			client: db,
			ctx:    ctx,
		}
		return f(dsormTx)
	})
}

func (db *Client) Client() *ds.Client {
	return db.client
}

func (db *Client) RawClient() *datastore.Client {
	return db.rawClient
}

func loadKeys(v reflect.Value, keys []*datastore.Key) {
	for i, k := range keys {
		if k != nil {
			vv := v.Index(i)
			if vv.IsNil() {
				continue
			}
			if v, ok := vv.Interface().(datastore.KeyLoader); ok {
				v.LoadKey(k)
			}
		}
	}
}

// Query executes a query and returns a slice of models.
func Query[T Model](ctx context.Context, db *Client, q *datastore.Query, cursor string, limit int) ([]T, string, error) {
	if limit == 0 || limit > 100 {
		limit = 100
	}
	q = q.KeysOnly().Limit(limit)
	keys, next, err := db.QueryKeys(ctx, q, cursor)
	if err != nil {
		return nil, "", err
	}
	dst := make([]T, len(keys))
	if err := db.GetMulti(ctx, keys, dst); err != nil {
		return nil, "", err
	}
	if len(dst) < limit {
		next = ""
	}
	return dst, next, nil
}
