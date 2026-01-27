package dsorm

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
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
		IsNew() bool
		Key() *datastore.Key
	}

	modeler interface {
		initModel(context.Context, any) error
		setInitProps([]datastore.Property)
		getInitProps() []datastore.Property
	}

	OnLoad interface {
		OnLoad(context.Context) error
	}

	BeforeSave interface {
		BeforeSave(context.Context, Model) error
	}

	BeforeDelete interface {
		BeforeDelete(context.Context) error
	}

	AfterDelete interface {
		AfterDelete(context.Context) error
	}

	AfterSave interface {
		AfterSave(context.Context, Model) error
	}

	// Base provides default implementation for Model interface and common fields.
	// Embed this in your struct to use dsorm.
	Base struct {
		key *datastore.Key `datastore:"-" json:"-"`

		initProps []datastore.Property `datastore:"-"`
		entity    any                  `datastore:"-"`
		ctx       context.Context      `datastore:"-"`
	}
)

func (b *Base) initModel(ctx context.Context, e any) error {
	b.ctx = ctx
	if b.entity == nil {
		b.entity = e
	}
	if b.key != nil {
		if kl, ok := e.(datastore.KeyLoader); ok {
			if err := kl.LoadKey(b.key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Base) Key() *datastore.Key {
	return b.key
}

// initHelpers
func initModel(ctx context.Context, v any) error {
	if m, ok := v.(modeler); ok {
		return m.initModel(ctx, v)
	}
	return nil
}

func reconstructOldModel(ctx context.Context, val any, k *datastore.Key) (Model, error) {
	if m, ok := val.(Model); ok && !m.IsNew() {
		if pg, ok := val.(modeler); ok {
			initProps := pg.getInitProps()
			if len(initProps) > 0 {
				oldVal := reflect.New(reflect.ValueOf(val).Elem().Type()).Interface()
				if oldM, ok := oldVal.(Model); ok {
					if err := loadModel(ctx, oldVal, initProps); err != nil {
						return nil, err
					}
					if om, ok := oldVal.(modeler); ok {
						if err := om.initModel(ctx, oldVal); err != nil {
							return nil, err
						}
					}
					// LoadKey as well to ensure full state
					if kl, ok := oldVal.(datastore.KeyLoader); ok {
						if err := kl.LoadKey(k); err != nil {
							return nil, err
						}
					}
					return oldM, nil
				}
			}
		}
	}
	return nil, nil
}

// Load implements datastore.PropertyLoadSaver.
func (b *Base) Load(ps []datastore.Property) error {
	err := loadModel(b.ctx, b.entity, ps)
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
		if err := initModel(ctx, val); err != nil {
			return nil, err
		}
		if len(b.initProps) > 0 {
			if err := loadModel(ctx, val, b.initProps); err != nil {
				return nil, err
			}
			if v, ok := val.(datastore.KeyLoader); ok {
				if err := v.LoadKey(b.key); err != nil {
					return nil, err
				}
			}
		}
		if err := bs.BeforeSave(ctx, val.(Model)); err != nil {
			return nil, err
		}
	}
	return saveModel(b.ctx, b.entity)
}

// LoadKey implements datastore.KeyLoader.
func (b *Base) LoadKey(k *datastore.Key) error {
	b.key = k
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
			} else if k.Parent != nil && vv.Kind() == reflect.Ptr && vv.Type().Elem().Kind() == reflect.Struct {
				if vv.IsNil() {
					vv.Set(reflect.New(vv.Type().Elem()))
				}
				if vv.IsNil() {
					vv.Set(reflect.New(vv.Type().Elem()))
				}
				initModel(b.ctx, vv.Interface())
				if kl, ok := vv.Interface().(datastore.KeyLoader); ok {
					kl.LoadKey(k.Parent)
				}
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

// setInitProps stores initial properties for change tracking access.
func (b *Base) setInitProps(props []datastore.Property) {
	b.initProps = props
}

func (b *Base) getInitProps() []datastore.Property {
	return b.initProps
}

func (b *Base) IsNew() bool {
	return len(b.initProps) == 0
}

func loadModel(ctx context.Context, e any, ps []datastore.Property) error {
	if m, ok := e.(modeler); ok {
		m.setInitProps(ps)
	}
	fields := make(map[string]*structtag.StructField)
	for _, field := range structtag.GetFieldsByTag(e, "marshal") {
		// handle legacy or multi-options? "name,encrypt"
		// We map by property name. Property name is first part of the tag.
		parts := strings.Split(field.Tag, ",")
		fields[parts[0]] = field
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
			// Check options in tag
			parts := strings.Split(field.Tag, ",")
			shouldDecrypt := false
			for _, opt := range parts[1:] {
				if opt == "encrypt" {
					shouldDecrypt = true
					break
				}
			}

			if shouldDecrypt {
				// decrypt value first
				if enc, ok := val.(string); ok {
					var (
						err    error
						secret []byte
					)

					// Prioritize Context
					if ctxKey, ok := ctx.Value(encryptionKeyKey).([]byte); ok {
						secret = ctxKey
					} else {
						secretEnv := os.Getenv("DATASTORE_ENCRYPTION_KEY")
						if secretEnv != "" {
							secret = []byte(secretEnv)
						}
					}

					if len(secret) == 0 {
						return fmt.Errorf("datastore.loadModel: encryption key missing")
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

func saveModel(ctx context.Context, e any) ([]datastore.Property, error) {
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
	// Handle Marshal tags
	for _, field := range structtag.GetFieldsByTag(e, "marshal") {
		// tag might be "name,encrypt"
		parts := strings.Split(field.Tag, ",")
		tag := parts[0]

		vv := v.Field(field.Index)
		if vv.IsZero() {
			continue
		}
		props, err = appendMarshaledProp(ctx, props, tag, field, vv, parts[1:])
		if err != nil {
			return nil, err
		}
	}
	return props, nil
}

func appendMarshaledProp(ctx context.Context, props []datastore.Property, tag string, field *structtag.StructField, vv reflect.Value, options []string) ([]datastore.Property, error) {
	b, err := json.Marshal(vv.Interface())
	if err != nil {
		return nil, err
	}
	prop := datastore.Property{
		Name:    tag,
		NoIndex: true,
	}
	val := string(b)

	// Check encryption
	shouldEncrypt := false

	for _, opt := range options {
		if opt == "encrypt" {
			shouldEncrypt = true
			break
		}
	}

	if shouldEncrypt {
		var (
			err    error
			secret []byte
		)

		// Prioritize Context
		if ctxKey, ok := ctx.Value(encryptionKeyKey).([]byte); ok {
			secret = ctxKey
		} else {
			secretEnv := os.Getenv("DATASTORE_ENCRYPTION_KEY")
			if secretEnv != "" {
				secret = []byte(secretEnv)
			}
		}

		if len(secret) == 0 {
			return nil, fmt.Errorf("datastore.saveModel: encryption key missing")
		}

		enc, err := encryption.Encrypt(secret, val)
		if err != nil {
			return nil, err
		}
		val = enc
	}
	prop.Value = val
	props = append(props, prop)
	return props, nil
}

// Client wraps datastore and cache operations.
type Client struct {
	client    *ds.Client
	rawClient *datastore.Client
	encKey    []byte
}

type options struct {
	projectID       string
	cache           ds.Cache
	datastoreClient *datastore.Client
	encryptionKey   []byte
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

func WithEncryptionKey(key []byte) Option {
	return func(o *options) {
		o.encryptionKey = key
	}
}

type contextKey string

var encryptionKeyKey contextKey = "dsorm_encryption_key"

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

	if o.encryptionKey != nil {
		ctx = context.WithValue(ctx, encryptionKeyKey, o.encryptionKey)
	}

	dsoClient, err := ds.NewClient(ctx, o.cache, ds.WithDatastoreClient(dsClient), ds.WithCachePrefix(o.projectID+":"))
	if err != nil {
		return nil, err
	}

	return &Client{
		rawClient: dsClient,
		client:    dsoClient,
		encKey:    o.encryptionKey,
	}, nil
}

func (db *Client) context(ctx context.Context) context.Context {
	if db.encKey != nil && ctx.Value(encryptionKeyKey) == nil {
		return context.WithValue(ctx, encryptionKeyKey, db.encKey)
	}
	return ctx
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
			vv := v.Field(field.Index)
			if p, ok := vv.Interface().(*datastore.Key); ok {
				key.Parent = p
				if p != nil && p.Namespace != "" {
					key.Namespace = p.Namespace
				}
			} else if vv.Kind() == reflect.Ptr && !vv.IsNil() && vv.Elem().Kind() == reflect.Struct {
				p := db.Key(vv.Interface())
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
	ctx = db.context(ctx)
	if err := initModel(ctx, val); err != nil {
		return err
	}
	key := db.Key(val)
	if err := db.client.Get(ctx, key, val); err != nil {
		return err
	}
	if v, ok := val.(datastore.KeyLoader); ok {
		if err := v.LoadKey(key); err != nil {
			return err
		}
	}
	return nil
}

// GetMulti loads multiple entities.
func (db *Client) GetMulti(ctx context.Context, vals interface{}) error {
	ctx = db.context(ctx)
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("datastore.DB.GetMulti: must be slice type not '%v'", v.Kind())
	}
	keys := db.Keys(vals)
	for i, k := range keys {
		if k != nil {
			vv := v.Index(i)
			vv.Set(reflect.New(v.Index(i).Type().Elem()))
			e := vv.Interface()
			if err := initModel(ctx, e); err != nil {
				return err
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
	if err := loadKeys(v, keys); err != nil {
		return err
	}
	return nil
}

// Put saves an entity.
func (db *Client) Put(ctx context.Context, val interface{}) error {
	ctx = db.context(ctx)
	if err := initModel(ctx, val); err != nil {
		return err
	}
	k := db.Key(val)
	k, err := db.client.Put(ctx, k, val)
	if err != nil {
		return err
	}
	if v, ok := val.(datastore.KeyLoader); ok {
		if err := v.LoadKey(k); err != nil {
			return err
		}
	}
	if as, ok := val.(AfterSave); ok {
		// Reconstruct old model using helper
		old, err := reconstructOldModel(ctx, val, k)
		if err != nil {
			return err
		}
		return as.AfterSave(ctx, old)
	}
	return nil
}

// PutMulti saves multiple entities.
func (db *Client) PutMulti(ctx context.Context, vals interface{}) error {
	ctx = db.context(ctx)
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("datastore.DB.PutMulti: must be slice type not '%v'", v.Kind())
	}
	var as []func() error
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i).Interface()
		if err := initModel(ctx, e); err != nil {
			return err
		}

		if a, ok := e.(AfterSave); ok {
			// capture closure for AfterSave execution later
			// db.Key(e) gives us current state key
			k := db.Key(e)
			old, err := reconstructOldModel(ctx, e, k)
			if err != nil {
				return err
			}

			as = append(as, func() error {
				return a.AfterSave(ctx, old)
			})
		}
	}
	keys := db.Keys(vals)
	keys, err := db.client.PutMulti(ctx, keys, vals)
	if err != nil {
		return err
	}
	if err := loadKeys(v, keys); err != nil {
		return err
	}

	if len(as) > 0 {
		return util.Task(20, as, func(f func() error) error {
			return f()
		})
	}

	return nil
}

func (db *Client) Delete(ctx context.Context, val interface{}) error {
	ctx = db.context(ctx)
	if bd, ok := val.(BeforeDelete); ok {
		if err := bd.BeforeDelete(ctx); err != nil {
			return err
		}
	}
	err := db.client.Delete(ctx, db.Key(val))
	if err != nil {
		return err
	}
	if ad, ok := val.(AfterDelete); ok {
		return ad.AfterDelete(ctx)
	}
	return nil
}

func (db *Client) DeleteMulti(ctx context.Context, vals interface{}) error {
	ctx = db.context(ctx)
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("datastore.db.DeleteMulti: must be slice type not '%v'", v.Kind())
	}

	// BeforeDelete Hooks
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i).Interface()
		if bd, ok := e.(BeforeDelete); ok {
			if err := bd.BeforeDelete(ctx); err != nil {
				return err
			}
		}
	}

	var keys []*datastore.Key
	if vKeys, ok := vals.([]*datastore.Key); ok {
		keys = vKeys
	} else {
		keys = db.Keys(vals)
	}
	if err := db.client.DeleteMulti(ctx, keys); err != nil {
		return err
	}

	// AfterDelete Hooks
	var ads []func() error
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i).Interface()
		if ad, ok := e.(AfterDelete); ok {
			ads = append(ads, func() error {
				return ad.AfterDelete(ctx)
			})
		}
	}
	if len(ads) > 0 {
		return util.Task(20, ads, func(f func() error) error {
			return f()
		})
	}
	return nil
}

func (db *Client) Query(ctx context.Context, q *datastore.Query, cursor string, vals interface{}) ([]*datastore.Key, string, error) {
	ctx = db.context(ctx)
	var keys []*datastore.Key
	var v reflect.Value
	if vals != nil {
		v = reflect.ValueOf(vals)
		if v.Kind() != reflect.Ptr {
			return nil, "", fmt.Errorf("datastore.DB.Query: must be pointer type not '%v'", v.Kind())
		}
		if v.Elem().Kind() != reflect.Slice {
			return nil, "", fmt.Errorf("datastore.DB.Query: must be slice type not '%v'", v.Elem().Kind())
		}
	}

	if cursor != "" {
		c, err := datastore.DecodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		q = q.Start(c)
	}
	q = q.KeysOnly()
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
	if vals != nil {
		v.Elem().Set(reflect.MakeSlice(v.Elem().Type(), len(keys), len(keys)))
		if len(keys) > 0 {
			vSlice := v.Elem()
			for i := 0; i < vSlice.Len(); i++ {
				if vSlice.Index(i).Kind() == reflect.Ptr && vSlice.Index(i).IsNil() {
					newElem := reflect.New(vSlice.Index(i).Type().Elem())

					vSlice.Index(i).Set(newElem)
					if err := initModel(ctx, newElem.Interface()); err != nil {
						return nil, "", err
					}
				}
			}
			if err := loadKeys(vSlice, keys); err != nil {
				return nil, "", err
			}
			if err := db.GetMulti(ctx, vSlice.Interface()); err != nil {
				return nil, "", err
			}
		}
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
	if err := initModel(t.ctx, val); err != nil {
		return err
	}
	key := t.client.Key(val)
	if err := t.tx.Get(key, val); err != nil {
		return err
	}
	if v, ok := val.(datastore.KeyLoader); ok {
		if err := v.LoadKey(key); err != nil {
			return err
		}
	}
	return nil
}

// GetMulti loads multiple entities within the transaction.
func (t *Transaction) GetMulti(vals interface{}) error {
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("dsorm.Transaction.GetMulti: must be slice type not '%v'", v.Kind())
	}
	keys := t.client.Keys(vals)
	for i, k := range keys {
		if k != nil {
			vv := v.Index(i)
			vv.Set(reflect.New(v.Index(i).Type().Elem()))
			e := vv.Interface()
			if err := initModel(t.ctx, e); err != nil {
				return err
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
	if err := loadKeys(v, keys); err != nil {
		return err
	}
	return nil
}

// Put saves an entity within the transaction.
func (t *Transaction) Put(val interface{}) error {
	if err := initModel(t.ctx, val); err != nil {
		return err
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
		if err := v.LoadKey(k); err != nil {
			return err
		}
	}
	if as, ok := val.(AfterSave); ok {
		old, err := reconstructOldModel(t.ctx, val, k)
		if err != nil {
			return err
		}
		return as.AfterSave(t.ctx, old)
	}
	return nil
}

// PutMulti saves multiple entities within the transaction.
func (t *Transaction) PutMulti(vals interface{}) error {
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("dsorm.Transaction.PutMulti: must be slice type not '%v'", v.Kind())
	}
	var as []func() error
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i).Interface()
		if err := initModel(t.ctx, e); err != nil {
			return err
		}

		if a, ok := e.(AfterSave); ok {
			k := t.client.Key(e)
			old, err := reconstructOldModel(t.ctx, e, k)
			if err != nil {
				return err
			}

			as = append(as, func() error {
				return a.AfterSave(t.ctx, old)
			})
		}
	}
	keys := t.client.Keys(vals)
	if _, err := t.tx.PutMulti(keys, vals); err != nil {
		return err
	}
	if err := loadKeys(v, keys); err != nil {
		return err
	}

	if len(as) > 0 {
		return util.Task(20, as, func(f func() error) error {
			return f()
		})
	}
	return nil
}

// Delete deletes an entity within the transaction.
func (t *Transaction) Delete(val interface{}) error {
	if bd, ok := val.(BeforeDelete); ok {
		if err := bd.BeforeDelete(t.ctx); err != nil {
			return err
		}
	}
	err := t.tx.Delete(t.client.Key(val))
	if err != nil {
		return err
	}
	if ad, ok := val.(AfterDelete); ok {
		return ad.AfterDelete(t.ctx)
	}
	return nil
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
	if err := t.tx.DeleteMulti(keys); err != nil {
		return err
	}
	// AfterDelete Hooks ? DeleteMulti in Tx with hooks is tricky if passing keys directly.
	// t.DeleteMulti accepts vals interface{}.
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i).Interface()
		if ad, ok := e.(AfterDelete); ok {
			if err := ad.AfterDelete(t.ctx); err != nil {
				return err
			}
		}
	}
	return nil
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

func loadKeys(v reflect.Value, keys []*datastore.Key) error {
	for i, k := range keys {
		if k != nil {
			vv := v.Index(i)
			if vv.IsNil() {
				continue
			}
			if v, ok := vv.Interface().(datastore.KeyLoader); ok {
				if err := v.LoadKey(k); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Query executes a query and returns a slice of models.
func Query[T Model](ctx context.Context, db *Client, q *datastore.Query, cursor string) ([]T, string, error) {
	var dst []T
	_, next, err := db.Query(ctx, q, cursor, &dst)
	if err != nil {
		return nil, "", err
	}
	return dst, next, nil
}

// GetMulti loads multiple entities by IDs (int64, string, *datastore.Key) or struct slice.
func GetMulti[T Model](ctx context.Context, db *Client, ids any) ([]T, error) {
	v := reflect.ValueOf(ids)
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("dsorm.GetMulti: ids must be a slice, got %v", v.Kind())
	}

	// 1. Determine Kind from T
	// T is usually *MyModel
	tType := reflect.TypeOf(new(T)).Elem()
	if tType.Kind() == reflect.Ptr {
		tType = tType.Elem()
	}
	kind := tType.Name()

	keys := make([]*datastore.Key, v.Len())
	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		val := item.Interface()
		switch k := val.(type) {
		case *datastore.Key:
			keys[i] = k
		case string:
			keys[i] = datastore.NameKey(kind, k, nil)
		case int64:
			keys[i] = datastore.IDKey(kind, k, nil)
		case int:
			keys[i] = datastore.IDKey(kind, int64(k), nil)
		default:
			// check if it's a struct or ptr to struct, assume it's a Model we can derive key from
			if item.Kind() == reflect.Struct || (item.Kind() == reflect.Ptr && item.Elem().Kind() == reflect.Struct) {
				keys[i] = db.Key(val)
			} else {
				return nil, fmt.Errorf("dsorm.GetMulti: unsupported ID type at index %d: %T", i, val)
			}
		}
	}

	dst := make([]T, len(keys))
	dstVal := reflect.ValueOf(dst)
	for i := 0; i < dstVal.Len(); i++ {
		if dstVal.Index(i).Kind() == reflect.Ptr && dstVal.Index(i).IsNil() {
			newElem := reflect.New(dstVal.Index(i).Type().Elem())
			dstVal.Index(i).Set(newElem)
			dstVal.Index(i).Set(newElem)
			if err := initModel(ctx, newElem.Interface()); err != nil {
				return nil, err
			}
		}
	}
	if err := loadKeys(dstVal, keys); err != nil {
		return nil, err
	}
	if err := db.GetMulti(ctx, dst); err != nil {
		return nil, err
	}
	return dst, nil
}
