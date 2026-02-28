package dsorm

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/altlimit/dsorm/cache"
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
	// Model is the interface that all dsorm-managed structs must implement.
	// Embed [Base] in your struct to get a default implementation.
	Model interface {
		// IsNew reports whether the entity has not yet been loaded from the datastore.
		IsNew() bool
		// Key returns the datastore key for this entity, or nil if not yet assigned.
		Key() *datastore.Key
	}

	modeler interface {
		initModel(context.Context, any) error
		setInitProps([]datastore.Property)
		getInitProps() []datastore.Property
	}

	// OnLoad is an optional interface that a model can implement to run
	// custom logic immediately after the entity is loaded from the datastore.
	// It is called after all properties have been deserialized and decrypted.
	OnLoad interface {
		OnLoad(context.Context) error
	}

	// BeforeSave is an optional interface that a model can implement to run
	// validation or transformation logic before the entity is written to the
	// datastore. The Model parameter contains the previously saved state of
	// the entity (nil for new entities), allowing comparison between old and
	// new values.
	BeforeSave interface {
		BeforeSave(context.Context, Model) error
	}

	// BeforeDelete is an optional interface that a model can implement to run
	// cleanup or validation logic before the entity is deleted from the datastore.
	BeforeDelete interface {
		BeforeDelete(context.Context) error
	}

	// AfterDelete is an optional interface that a model can implement to run
	// side-effect logic (e.g. cache invalidation, cascading deletes) after the
	// entity has been successfully deleted from the datastore.
	AfterDelete interface {
		AfterDelete(context.Context) error
	}

	// AfterSave is an optional interface that a model can implement to run
	// side-effect logic after the entity has been successfully written to the
	// datastore. The Model parameter contains the previously saved state
	// (nil for new entities), enabling change-detection workflows such as
	// audit logging or sending notifications.
	AfterSave interface {
		AfterSave(context.Context, Model) error
	}

	// Base provides a default implementation of the [Model] interface along
	// with datastore.PropertyLoadSaver and datastore.KeyLoader. Embed this
	// struct in your model to gain automatic key mapping, property
	// marshaling/encryption, lifecycle hooks, and change tracking.
	//
	// Fields are configured via the "model" struct tag:
	//
	//	model:"id"                  — maps the field as the entity's datastore key ID (string or int64)
	//	model:"parent"              — maps the field as the entity's parent key
	//	model:"ns"                  — maps the field as the entity's namespace
	//	model:"created"             — auto-set to UTC now on first save
	//	model:"modified"            — auto-set to UTC now on every save
	//	model:"<name>,marshal"      — JSON-marshal the field into a single datastore property
	//	model:"<name>,encrypt"      — JSON-marshal and AES-encrypt the field
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

// Key returns the datastore key for this entity. The key is populated
// automatically when the entity is loaded from the datastore or after
// a successful Put operation.
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
		t = t.Elem()
		v = v.Elem()
	}

	info := getTypeInfo(t)

	// ID
	if info.idField != nil {
		vv := v.Field(info.idField.Index)
		var val reflect.Value
		switch vv.Kind() {
		case reflect.String:
			val = reflect.ValueOf(k.Name)
		case reflect.Int64:
			val = reflect.ValueOf(k.ID)
		}
		vv.Set(val)
	}

	// Parent
	if info.parentField != nil {
		vv := v.Field(info.parentField.Index)
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
			if err := initModel(b.ctx, vv.Interface()); err != nil {
				return err
			}
			if kl, ok := vv.Interface().(datastore.KeyLoader); ok {
				if err := kl.LoadKey(k.Parent); err != nil {
					return err
				}
			}
		}
	}

	// NS
	if info.nsField != nil {
		if k.Namespace != "" {
			vv := v.Field(info.nsField.Index)
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

// IsNew reports whether the entity has not yet been loaded from the datastore.
// It returns true for freshly constructed structs that have never been Get'd.
func (b *Base) IsNew() bool {
	return len(b.initProps) == 0
}

func loadModel(ctx context.Context, e any, ps []datastore.Property) error {
	if m, ok := e.(modeler); ok {
		m.setInitProps(ps)
	}
	t := reflect.TypeOf(e)
	v := reflect.ValueOf(e)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	info := getTypeInfo(t)
	fields := info.marshalFields
	var props []datastore.Property

	for _, p := range ps {
		if field, ok := fields[p.Name]; ok {
			vv := v.Field(field.Index)
			vt := vv.Type()
			if vt.Kind() == reflect.Ptr {
				vt = vt.Elem()
			}
			i := reflect.New(vt).Interface()
			val := p.Value
			// Decrypt if needed
			if field.Encrypt {
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
		t = t.Elem()
		v = v.Elem()
	}
	now := time.Now().UTC()
	m, ok := e.(Model)

	info := getTypeInfo(t)

	// Created/Modified
	if info.createdField != nil {
		if ok && m.IsNew() {
			vv := v.Field(info.createdField.Index)
			vv.Set(reflect.ValueOf(now))
		}
	}
	if info.modifiedField != nil {
		if ok {
			vv := v.Field(info.modifiedField.Index)
			vv.Set(reflect.ValueOf(now))
		}
	}

	props, err := datastore.SaveStruct(e)
	if err != nil {
		return nil, err
	}

	// Filter out excluded properties (id, ns, parent, marshal, encrypt fields).
	// Unsupported types (maps, custom structs) must have datastore:"-" so
	// SaveStruct skips them. Simple types (string, int64) still produce
	// zero-value properties that we filter here.
	if len(info.excludeProps) > 0 {
		filtered := props[:0]
		for _, p := range props {
			if !info.excludeProps[p.Name] {
				filtered = append(filtered, p)
			}
		}
		props = filtered
	}

	// Append marshaled/encrypted properties
	for _, fi := range info.marshalFieldsList {
		vv := v.Field(fi.Index)
		if vv.IsZero() {
			continue
		}
		props, err = appendMarshaledProp(ctx, props, fi.Name, vv, fi.Encrypt)
		if err != nil {
			return nil, err
		}
	}
	return props, nil
}

func appendMarshaledProp(ctx context.Context, props []datastore.Property, tag string, vv reflect.Value, shouldEncrypt bool) ([]datastore.Property, error) {
	b, err := json.Marshal(vv.Interface())
	if err != nil {
		return nil, err
	}
	prop := datastore.Property{
		Name:    tag,
		NoIndex: true,
	}
	val := string(b)

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
	client *ds.Client
	encKey []byte
}

// Cache returns an application-level cache.Cache for general-purpose
// caching operations (Get, Set, Delete, Load, Save, RateLimit, etc).
func (c *Client) Cache() cache.Cache {
	return cache.New(c.client.Cacher())
}

type options struct {
	projectID       string
	cache           ds.Cache
	datastoreClient *datastore.Client
	store           ds.Store
	encryptionKey   []byte
}

// Option configures a [Client] created via [New].
type Option func(*options)

// WithProjectID sets the Google Cloud project ID. If not specified, the
// DATASTORE_PROJECT_ID or GOOGLE_CLOUD_PROJECT environment variables are
// used as fallbacks.
func WithProjectID(id string) Option {
	return func(o *options) {
		o.projectID = id
	}
}

// WithCache overrides the auto-detected caching backend with the provided
// implementation. By default, [New] selects App Engine Memcache, Redis
// (via REDIS_ADDR env), or in-memory cache, in that order.
func WithCache(c ds.Cache) Option {
	return func(o *options) {
		o.cache = c
	}
}

// WithDatastoreClient provides an existing cloud.google.com/go/datastore
// client instead of creating one internally. Useful for environments that
// require custom client configuration (e.g. emulator endpoints).
func WithDatastoreClient(c *datastore.Client) Option {
	return func(o *options) {
		o.datastoreClient = c
	}
}

// WithStore replaces the default Google Cloud Datastore backend with a
// custom [ds.Store] implementation (e.g. the local in-memory store for
// testing).
func WithStore(s ds.Store) Option {
	return func(o *options) {
		o.store = s
	}
}

// WithEncryptionKey sets the AES encryption key used for fields tagged
// with model:"<name>,encrypt". If not set, the DATASTORE_ENCRYPTION_KEY
// environment variable is used as a fallback at load/save time.
func WithEncryptionKey(key []byte) Option {
	return func(o *options) {
		o.encryptionKey = key
	}
}

type contextKey string

var encryptionKeyKey contextKey = "dsorm_encryption_key"

// New creates a new [Client] with the given options.
//
// Caching backend is auto-detected in the following order unless overridden
// with [WithCache]:
//  1. App Engine Memcache (when running on App Engine)
//  2. Redis (when REDIS_ADDR environment variable is set)
//  3. In-memory cache (fallback)
func New(ctx context.Context, opts ...Option) (*Client, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	var dsClient *datastore.Client
	var err error

	if o.store == nil {
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
	}

	if o.cache == nil {
		if appengine.IsAppEngine() || appengine.IsDevAppServer() {
			o.cache = memcache.NewCache()
		} else if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
			o.cache, err = redis.NewCache(redisAddr)
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

	var dsOpts []ds.ClientOption
	if o.store != nil {
		dsOpts = append(dsOpts, ds.WithStore(o.store))
	} else {
		dsOpts = append(dsOpts, ds.WithDatastoreClient(dsClient))
	}
	dsOpts = append(dsOpts, ds.WithCachePrefix(o.projectID+":"))

	dsoClient, err := ds.NewClient(ctx, o.cache, dsOpts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: dsoClient,
		encKey: o.encryptionKey,
	}, nil
}

func (c *Client) context(ctx context.Context) context.Context {
	if c.encKey != nil && ctx.Value(encryptionKeyKey) == nil {
		return context.WithValue(ctx, encryptionKeyKey, c.encKey)
	}
	return ctx
}

// Keys returns a slice of datastore keys derived from a slice of model
// structs. Each entity's key is built from its model:"id", model:"parent",
// and model:"ns" struct tags.
func (c *Client) Keys(val interface{}) []*datastore.Key {
	t := reflect.TypeOf(val)
	var keys []*datastore.Key
	if t.Kind() == reflect.Slice {
		v := reflect.ValueOf(val)
		for i := 0; i < v.Len(); i++ {
			keys = append(keys, c.Key(v.Index(i).Interface()))
		}
	}
	return keys
}

// Key returns the datastore key for a single model struct. The key is
// constructed from the struct's model:"id" (string or int64), model:"parent"
// (datastore.Key or parent model pointer), and model:"ns" (namespace) tags.
func (c *Client) Key(val interface{}) *datastore.Key {
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
				p := c.Key(vv.Interface())
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

// Get loads a single entity from the datastore into val. The entity's key
// is derived from val's struct tags (see [Client.Key]). After loading, the
// entity's [Base.LoadKey] is called to map the key back into the struct
// fields.
func (c *Client) Get(ctx context.Context, val interface{}) error {
	ctx = c.context(ctx)
	if err := initModel(ctx, val); err != nil {
		return err
	}
	key := c.Key(val)
	if err := c.client.Get(ctx, key, val); err != nil {
		return err
	}
	if v, ok := val.(datastore.KeyLoader); ok {
		if err := v.LoadKey(key); err != nil {
			return err
		}
	}
	return nil
}

// GetMulti loads multiple entities from the datastore. vals must be a
// slice of pointer-to-struct (e.g. []*MyModel). Entities that do not
// exist in the datastore are set to nil in the slice rather than
// returning an error. Only non-ErrNoSuchEntity errors are returned.
func (c *Client) GetMulti(ctx context.Context, vals interface{}) error {
	ctx = c.context(ctx)
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("datastore.DB.GetMulti: must be slice type not '%v'", v.Kind())
	}
	keys := c.Keys(vals)
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
	err := c.client.GetMulti(ctx, keys, vals)
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

// Put saves a single entity to the datastore. For new entities with an
// auto-generated int64 ID, the ID is written back into the struct after
// a successful save. If val implements [BeforeSave], it is called before
// writing. If val implements [AfterSave], it is called after writing with
// the previous state of the entity.
func (c *Client) Put(ctx context.Context, val interface{}) error {
	ctx = c.context(ctx)
	if err := initModel(ctx, val); err != nil {
		return err
	}
	k := c.Key(val)
	k, err := c.client.Put(ctx, k, val)
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

// PutMulti saves multiple entities to the datastore. vals must be a slice
// of pointer-to-struct. Auto-generated IDs are written back into each
// struct. [BeforeSave] and [AfterSave] hooks are called for each entity
// that implements them. AfterSave hooks run concurrently.
func (c *Client) PutMulti(ctx context.Context, vals interface{}) error {
	ctx = c.context(ctx)
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
			// c.Key(e) gives us current state key
			k := c.Key(e)
			old, err := reconstructOldModel(ctx, e, k)
			if err != nil {
				return err
			}

			as = append(as, func() error {
				return a.AfterSave(ctx, old)
			})
		}
	}
	keys := c.Keys(vals)
	keys, err := c.client.PutMulti(ctx, keys, vals)
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

// Delete removes a single entity from the datastore. If val implements
// [BeforeDelete], it is called before deletion. If val implements
// [AfterDelete], it is called after successful deletion.
func (c *Client) Delete(ctx context.Context, val interface{}) error {
	ctx = c.context(ctx)
	if bd, ok := val.(BeforeDelete); ok {
		if err := bd.BeforeDelete(ctx); err != nil {
			return err
		}
	}
	err := c.client.Delete(ctx, c.Key(val))
	if err != nil {
		return err
	}
	if ad, ok := val.(AfterDelete); ok {
		return ad.AfterDelete(ctx)
	}
	return nil
}

// DeleteMulti removes multiple entities from the datastore. vals can be
// a slice of model structs or a []*datastore.Key. [BeforeDelete] and
// [AfterDelete] hooks are called for each entity that implements them.
func (c *Client) DeleteMulti(ctx context.Context, vals interface{}) error {
	ctx = c.context(ctx)
	v := reflect.ValueOf(vals)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("datastore.c.DeleteMulti: must be slice type not '%v'", v.Kind())
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
		keys = c.Keys(vals)
	}
	if err := c.client.DeleteMulti(ctx, keys); err != nil {
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

// Query executes a query using a keys-only strategy: it first fetches all
// matching keys, then hydrates the entities via [Client.GetMulti]. If vals
// is non-nil, it must be a pointer to a slice (e.g. *[]*MyModel) and will
// be populated with the loaded entities. The returned string is the cursor
// for pagination; pass it back as the cursor argument to resume.
func (c *Client) Query(ctx context.Context, q *QueryBuilder, cursor string, vals interface{}) ([]*datastore.Key, string, error) {
	ctx = c.context(ctx)
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
		q = q.Start(cursor)
	}
	q = q.KeysOnly()
	it := c.client.Run(ctx, q)
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
			if err := c.GetMulti(ctx, vSlice.Interface()); err != nil {
				return nil, "", err
			}
		}
	}
	return keys, next, nil
}

// Transaction wraps a datastore transaction with dsorm functionality
// including automatic key mapping, lifecycle hooks, and pending key
// resolution for auto-ID entities.
type Transaction struct {
	tx      *ds.Transaction
	client  *Client
	ctx     context.Context
	pending []pendingItem
}

type pendingItem struct {
	pk   *datastore.PendingKey
	item interface{}
}

// Get loads a single entity within the transaction. Behaves like
// [Client.Get] but operates within the transaction's isolation.
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

// GetMulti loads multiple entities within the transaction. Behaves like
// [Client.GetMulti]: entities that do not exist are set to nil in the
// slice.
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

// Put saves a single entity within the transaction. For new entities with
// auto-generated IDs, the ID is resolved after [Client.Transact] commits
// successfully.
func (t *Transaction) Put(val interface{}) error {
	if err := initModel(t.ctx, val); err != nil {
		return err
	}
	k := t.client.Key(val)
	pk, err := t.tx.Put(k, val)
	if err != nil {
		return err
	}
	if pk != nil {
		t.pending = append(t.pending, pendingItem{pk, val})
	}
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

// PutMulti saves multiple entities within the transaction. Auto-generated
// IDs are resolved after [Client.Transact] commits.
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
	pks, err := t.tx.PutMulti(keys, vals)
	if err != nil {
		return err
	}
	if len(pks) == v.Len() {
		for i, pk := range pks {
			if pk != nil {
				t.pending = append(t.pending, pendingItem{pk, v.Index(i).Interface()})
			}
		}
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

// Delete removes a single entity within the transaction.
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

// DeleteMulti removes multiple entities within the transaction.
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

// Transact runs f inside a datastore transaction. If f returns nil, the
// transaction is committed and any pending auto-generated IDs are resolved
// back into their corresponding structs via [Base.LoadKey]. If f returns
// an error, the transaction is rolled back.
func (c *Client) Transact(ctx context.Context, f func(tx *Transaction) error) (*datastore.Commit, error) {
	var pending []pendingItem

	cmt, err := c.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		dsormTx := &Transaction{
			tx:     tx,
			client: c,
			ctx:    ctx,
		}
		if err := f(dsormTx); err != nil {
			return err
		}
		pending = dsormTx.pending
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(pending) > 0 && cmt != nil {
		// Resolve pending keys
		for _, p := range pending {
			k := cmt.Key(p.pk)
			if k != nil {
				if v, ok := p.item.(datastore.KeyLoader); ok {
					if err := v.LoadKey(k); err != nil {
						// Should we error here? The TX is already committed.
						// Logging or returning error is tricky.
						return cmt, err
					}
				}
			}
		}
	}
	return cmt, nil

}

// Close closes the underlying datastore and cache connections.
func (c *Client) Close() error {
	return c.client.Close()
}

// Store returns the underlying [ds.Store]. To access backend-specific
// clients, type-assert to [ds.CloudAccess] or [ds.LocalAccess]:
//
//	if ca, ok := c.Store().(ds.CloudAccess); ok {
//	    raw := ca.DatastoreClient()
//	}
//	if la, ok := c.Store().(ds.LocalAccess); ok {
//	    sqlDB, err := la.DB("")
//	}
func (c *Client) Store() ds.Store {
	return c.client.Store
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

// Query is a generic convenience wrapper around [Client.Query] that returns
// a typed slice. It handles allocation and type assertion internally.
func Query[T Model](ctx context.Context, c *Client, q *QueryBuilder, cursor string) ([]T, string, error) {
	var dst []T
	_, next, err := c.Query(ctx, q, cursor, &dst)
	if err != nil {
		return nil, "", err
	}
	return dst, next, nil
}

// GetMulti is a generic convenience wrapper around [Client.GetMulti] that
// accepts a slice of IDs (int, int64, string, or *datastore.Key) or model
// structs and returns a typed slice. Entities that do not exist in the
// datastore are set to nil in the returned slice.
func GetMulti[T Model](ctx context.Context, c *Client, ids any) ([]T, error) {
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
				keys[i] = c.Key(val)
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
			if err := initModel(ctx, newElem.Interface()); err != nil {
				return nil, err
			}
		}
	}
	if err := loadKeys(dstVal, keys); err != nil {
		return nil, err
	}
	if err := c.GetMulti(ctx, dst); err != nil {
		return nil, err
	}
	return dst, nil
}

// ------------------------------------------------------------------
// Caching for Performance
// ------------------------------------------------------------------

var typeCache sync.Map

type typeInfo struct {
	idField       *structtag.StructField
	parentField   *structtag.StructField
	nsField       *structtag.StructField
	createdField  *structtag.StructField
	modifiedField *structtag.StructField

	marshalFields     map[string]*fieldInfo // for Load (lookup by prop name)
	marshalFieldsList []*fieldInfo          // for Save (ordered iteration)

	// excludeProps lists property names that should be auto-excluded from
	// datastore.SaveStruct output (model built-in fields + marshal/encrypt fields).
	excludeProps map[string]bool
}

type fieldInfo struct {
	Index   int
	Name    string
	Encrypt bool
}

// propNameForField returns the datastore property name for a struct field.
// It checks the "datastore" tag first; if absent, uses the Go field name.
func propNameForField(field *structtag.StructField) string {
	if dsTag, ok := field.Value("datastore"); ok {
		parts := strings.Split(dsTag, ",")
		if parts[0] != "" && parts[0] != "-" {
			return parts[0]
		}
	}
	return field.FieldName
}

func getTypeInfo(t reflect.Type) *typeInfo {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if v, ok := typeCache.Load(t); ok {
		return v.(*typeInfo)
	}

	info := &typeInfo{
		marshalFields: make(map[string]*fieldInfo),
		excludeProps:  make(map[string]bool),
	}

	// Model tags — parse all options from model:"value,opt1,opt2"
	dummy := reflect.New(t).Interface()

	for _, field := range structtag.GetFieldsByTag(dummy, "model") {
		parts := strings.Split(field.Tag, ",")
		primary := parts[0]

		// Parse options
		hasStore := false
		hasMarshal := false
		hasEncrypt := false
		for _, opt := range parts[1:] {
			switch opt {
			case "store":
				hasStore = true
			case "marshal":
				hasMarshal = true
			case "encrypt":
				hasEncrypt = true
			}
		}

		switch primary {
		case "id":
			info.idField = field
			if !hasStore {
				info.excludeProps[propNameForField(field)] = true
			}
		case "parent":
			info.parentField = field
			if !hasStore {
				info.excludeProps[propNameForField(field)] = true
			}
		case "ns":
			info.nsField = field
			if !hasStore {
				info.excludeProps[propNameForField(field)] = true
			}
		case "created":
			info.createdField = field
		case "modified":
			info.modifiedField = field
		default:
			// model:"name,marshal" or model:"name,encrypt"
			if hasMarshal || hasEncrypt {
				fi := &fieldInfo{
					Index:   field.Index,
					Name:    primary,
					Encrypt: hasEncrypt,
				}
				info.marshalFields[primary] = fi
				info.marshalFieldsList = append(info.marshalFieldsList, fi)
				// Auto-exclude from datastore.SaveStruct output
				info.excludeProps[propNameForField(field)] = true
			}
		}
	}

	typeCache.Store(t, info)
	return info
}
