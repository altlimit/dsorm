package redis

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

	dsorm "github.com/altlimit/dsorm/ds"
)

const (
	// Datastore max size is 1,048,572 bytes (1 MiB - 4 bytes)
	// + 4 bytes for uint32 flags
	maxCacheSize = (1 << 20)

	casScript = `local exp = tonumber(ARGV[3])
	local orig = redis.call("get", KEYS[1])
	if not orig then
		return nil
	end
	if orig == ARGV[1]
	then
		if exp >= 0
		then
			return redis.call("SET", KEYS[1], ARGV[2], "PX", exp)
		else
			return redis.call("SET", KEYS[1], ARGV[2])
		end
	else
		return redis.error_reply("cas conflict")
	end`

	incrScript = `local val = redis.call("INCRBY", KEYS[1], ARGV[1])
	if tonumber(ARGV[2]) > 0 then
		redis.call("PEXPIRE", KEYS[1], ARGV[2])
	end
	return val`
)

// Option configures a redis cache backend.
type Option func(*backend)

// WithPassword sets the password for the redis connection.
func WithPassword(password string) Option {
	return func(b *backend) {
		b.password = password
	}
}

// WithDB selects the redis database index.
func WithDB(db int) Option {
	return func(b *backend) {
		b.db = db
	}
}

// NewCache creates a new dsorm.Cache backed by Redis/Valkey.
// addr is the address of the Redis/Valkey server (e.g. "localhost:6379").
func NewCache(addr string, opts ...Option) (dsorm.Cache, error) {
	b := &backend{}
	for _, opt := range opts {
		opt(b)
	}

	clientOpts := valkey.ClientOption{
		InitAddress: []string{addr},
	}
	if b.password != "" {
		clientOpts.Password = b.password
	}
	if b.db != 0 {
		clientOpts.SelectDB = b.db
	}

	client, err := valkey.NewClient(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("redis: failed to create client: %w", err)
	}

	b.client = client
	b.ownsClient = true

	// Validate connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis: ping failed: %w", err)
	}

	return b, nil
}

// NewCacheFromClient creates a dsorm.Cache from an existing valkey.Client.
// This is useful for advanced configurations (cluster, sentinel, etc).
// The caller retains ownership of the client and must close it themselves.
func NewCacheFromClient(client valkey.Client) dsorm.Cache {
	return &backend{client: client}
}

type backend struct {
	client     valkey.Client
	ownsClient bool
	password   string
	db         int
}

// Close releases the underlying client if it was created by NewCache.
func (b *backend) Close() {
	if b.ownsClient && b.client != nil {
		b.client.Close()
	}
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (b *backend) AddMulti(ctx context.Context, items []*dsorm.Item) error {
	if len(items) == 0 {
		return nil
	}

	cmds := make(valkey.Commands, 0, len(items))
	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		if buf.Cap() <= maxCacheSize {
			bufPool.Put(buf)
		}
	}()

	for _, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		buf.Reset()
		buf.Grow(4 + len(item.Value))
		_ = binary.Write(buf, binary.LittleEndian, item.Flags)
		_, _ = buf.Write(item.Value)

		// Copy bytes since buf is reused across iterations
		data := append([]byte(nil), buf.Bytes()...)
		cmd := b.client.B().Set().Key(item.Key).Value(valkey.BinaryString(data)).Nx()
		if item.Expiration != 0 {
			cmds = append(cmds, cmd.Px(item.Expiration).Build())
		} else {
			cmds = append(cmds, cmd.Build())
		}
	}

	resps := b.client.DoMulti(ctx, cmds...)
	me := make(dsorm.MultiError, len(items))
	hasErr := false
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			if valkey.IsValkeyNil(err) {
				me[i] = dsorm.ErrNotStored
			} else {
				me[i] = err
			}
			hasErr = true
		}
	}
	if hasErr {
		return me
	}
	return nil
}

func (b *backend) SetMulti(ctx context.Context, items []*dsorm.Item) error {
	if len(items) == 0 {
		return nil
	}

	cmds := make(valkey.Commands, 0, len(items))
	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		if buf.Cap() <= maxCacheSize {
			bufPool.Put(buf)
		}
	}()

	for _, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		buf.Reset()
		buf.Grow(4 + len(item.Value))
		_ = binary.Write(buf, binary.LittleEndian, item.Flags)
		_, _ = buf.Write(item.Value)

		// Copy bytes since buf is reused across iterations
		data := append([]byte(nil), buf.Bytes()...)
		if item.Expiration != 0 {
			cmds = append(cmds, b.client.B().Set().Key(item.Key).Value(valkey.BinaryString(data)).Px(item.Expiration).Build())
		} else {
			cmds = append(cmds, b.client.B().Set().Key(item.Key).Value(valkey.BinaryString(data)).Build())
		}
	}

	resps := b.client.DoMulti(ctx, cmds...)
	me := make(dsorm.MultiError, len(items))
	hasErr := false
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			me[i] = err
			hasErr = true
		}
	}
	if hasErr {
		return me
	}
	return nil
}

func (b *backend) CompareAndSwapMulti(ctx context.Context, items []*dsorm.Item) error {
	if len(items) == 0 {
		return nil
	}

	me := make(dsorm.MultiError, len(items))
	hasErr := false

	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		if buf.Cap() <= maxCacheSize {
			bufPool.Put(buf)
		}
	}()

	for i, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		cas, ok := item.GetCASInfo().([]byte)
		if !ok || cas == nil {
			me[i] = dsorm.ErrNotStored
			hasErr = true
			continue
		}

		buf.Reset()
		buf.Grow(4 + len(item.Value))
		_ = binary.Write(buf, binary.LittleEndian, item.Flags)
		_, _ = buf.Write(item.Value)

		// Copy bytes since buf is reused across iterations
		data := append([]byte(nil), buf.Bytes()...)

		expire := int64(item.Expiration / time.Millisecond)
		if item.Expiration == 0 {
			expire = -1
		}

		resp := b.client.Do(ctx, b.client.B().Eval().Script(casScript).Numkeys(1).Key(item.Key).Arg(
			valkey.BinaryString(cas),
			valkey.BinaryString(data),
			fmt.Sprintf("%d", expire),
		).Build())

		if err := resp.Error(); err != nil {
			if valkey.IsValkeyNil(err) {
				me[i] = dsorm.ErrNotStored
			} else if err.Error() == "cas conflict" {
				me[i] = dsorm.ErrCASConflict
			} else {
				me[i] = err
			}
			hasErr = true
		}
	}

	if hasErr {
		return me
	}
	return nil
}

func (b *backend) DeleteMulti(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	resp := b.client.Do(ctx, b.client.B().Del().Key(keys...).Build())
	num, err := resp.AsInt64()
	if err != nil {
		return err
	}
	if num != int64(len(keys)) {
		return fmt.Errorf("redis: expected to remove %d keys, but only removed %d", len(keys), num)
	}
	return nil
}

func (b *backend) GetMulti(ctx context.Context, keys []string) (map[string]*dsorm.Item, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	resp := b.client.Do(ctx, b.client.B().Mget().Key(keys...).Build())
	results, err := resp.ToArray()
	if err != nil {
		return nil, err
	}

	if len(results) != len(keys) {
		return nil, fmt.Errorf("redis: len(results) != len(keys) (%d != %d)", len(results), len(keys))
	}

	result := make(map[string]*dsorm.Item)
	me := make(dsorm.MultiError, len(keys))
	hasErr := false

	for i, key := range keys {
		cacheItem, err := results[i].AsBytes()
		if err != nil {
			if valkey.IsValkeyNil(err) {
				continue // cache miss, skip
			}
			me[i] = err
			hasErr = true
			continue
		}

		if got := len(cacheItem); got < 4 {
			me[i] = fmt.Errorf("redis: cached item should be atleast 4 bytes, got %d", got)
			hasErr = true
			continue
		}

		buf := bytes.NewBuffer(cacheItem)
		var flags uint32
		if err = binary.Read(buf, binary.LittleEndian, &flags); err != nil {
			me[i] = err
			hasErr = true
			continue
		}

		dsoItem := &dsorm.Item{
			Key:   key,
			Flags: flags,
			Value: buf.Bytes(),
		}

		// Keep a copy of the original value data for any future CAS operations
		dsoItem.SetCASInfo(append([]byte(nil), cacheItem...))
		result[key] = dsoItem
	}

	if hasErr {
		return result, me
	}
	return result, nil
}

func (b *backend) Increment(ctx context.Context, key string, delta int64, expiration time.Duration) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	expMs := int64(0)
	if expiration > 0 {
		expMs = int64(expiration / time.Millisecond)
	}

	resp := b.client.Do(ctx, b.client.B().Eval().Script(incrScript).Numkeys(1).Key(key).Arg(
		fmt.Sprintf("%d", delta),
		fmt.Sprintf("%d", expMs),
	).Build())

	val, err := resp.AsInt64()
	if err != nil {
		return 0, err
	}
	return val, nil
}
