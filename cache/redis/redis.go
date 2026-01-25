package redis

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/opencensus-integrations/redigo/redis"

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
)

// NewPool creates a new redis pool
func NewPool(address, password string, db int) (*redis.Pool, error) {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", address)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if db != 0 {
				if _, err := c.Do("SELECT", db); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}, nil
}

// NewCache will return a dsorm.Cache backed by
// the provided redis pool. It will try and load a script
// into the redis script cache and return an error if it is
// unable to. Anytime the redis script cache is flushed, a new
// redis dsorm.Cache must be initialized to reload the script.
func NewCache(ctx context.Context, pool *redis.Pool) (n dsorm.Cache, err error) {
	conn := pool.GetWithContext(ctx).(redis.ConnWithContext)

	defer func() {
		if cerr := conn.CloseContext(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}()

	b := backend{store: pool}

	if b.casSha, err = redis.String(conn.DoContext(ctx, "SCRIPT", "LOAD", casScript)); err != nil {
		return
	}

	n = &b

	return
}

type backend struct {
	store  *redis.Pool
	casSha string
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (b *backend) AddMulti(ctx context.Context, items []*dsorm.Item) (err error) {
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)

	defer func() {
		if cerr := redisConn.CloseContext(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}()

	err = set(ctx, redisConn, true, items)

	return
}

func set(ctx context.Context, conn redis.ConnWithContext, nx bool, items []*dsorm.Item) error {
	me := make(dsorm.MultiError, len(items))
	meChan := make(chan error, len(items))

	hasErr := false
	var flushErr error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(meChan)

		buf := bufPool.Get().(*bytes.Buffer)
	Loop:
		for _, item := range items {
			select {
			case <-ctx.Done():
				break Loop
			default:
			}
			buf.Reset()
			buf.Grow(4 + len(item.Value))
			_ = binary.Write(buf, binary.LittleEndian, item.Flags) // Always returns nil since we're using bytes.Buffer
			_, _ = buf.Write(item.Value)

			args := []interface{}{item.Key, buf.Bytes()}
			if nx {
				args = append(args, "NX")
			}

			if item.Expiration != 0 {
				expire := item.Expiration.Truncate(time.Millisecond) / time.Millisecond
				args = append(args, "PX", int64(expire))
			}

			if err := conn.SendContext(ctx, "SET", args...); err != nil {
				meChan <- err
			}
		}
		flushErr = conn.FlushContext(ctx)
		if buf.Cap() <= maxCacheSize {
			bufPool.Put(buf)
		}
	}()

	go func() {
		defer wg.Done()
	Loop2:
		for i := 0; i < len(items); i++ {
			select {
			case <-ctx.Done():
				break Loop2
			case me[i] = <-meChan:
			default:
			}

			if me[i] != nil {
				// We couldn't queue the command so don't expect it's response
				hasErr = true
				continue
			}
			if _, err := redis.String(conn.ReceiveContext(ctx)); err != nil {
				if nx && err == redis.ErrNil {
					me[i] = dsorm.ErrNotStored
				} else {
					me[i] = err
				}
				hasErr = true
			}
		}
	}()

	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if flushErr != nil {
		return flushErr
	}

	if hasErr {
		return me
	}
	return nil
}

func (b *backend) CompareAndSwapMulti(ctx context.Context, items []*dsorm.Item) (err error) {
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)
	defer func() {
		if cerr := redisConn.CloseContext(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}()

	me := make(dsorm.MultiError, len(items))
	meChan := make(chan error, len(items))

	hasErr := false
	var flushErr error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(meChan)

		buf := bufPool.Get().(*bytes.Buffer)
	Loop:
		for _, item := range items {
			select {
			case <-ctx.Done():
				break Loop
			default:
			}
			if cas, ok := item.GetCASInfo().([]byte); ok && cas != nil {
				buf.Reset()
				buf.Grow(4 + len(item.Value))
				_ = binary.Write(buf, binary.LittleEndian, item.Flags) // Always returns nil since we're using bytes.Buffer
				_, _ = buf.Write(item.Value)
				expire := int64(item.Expiration.Truncate(time.Millisecond) / time.Millisecond)
				if item.Expiration == 0 {
					expire = -1
				}
				if rerr := redisConn.SendContext(ctx, "EVALSHA", b.casSha, "1", item.Key, cas, buf.Bytes(), expire); rerr != nil {
					meChan <- rerr
				}
			} else {
				meChan <- dsorm.ErrNotStored
			}
		}
		flushErr = redisConn.FlushContext(ctx)
		if buf.Cap() <= maxCacheSize {
			bufPool.Put(buf)
		}
	}()

	go func() {
		defer wg.Done()
	Loop2:
		for i := 0; i < len(items); i++ {
			select {
			case <-ctx.Done():
				break Loop2
			case me[i] = <-meChan:
			default:
			}

			if me[i] != nil {
				// We couldn't queue the command so don't expect it's response
				hasErr = true
				continue
			}
			if _, err := redis.String(redisConn.ReceiveContext(ctx)); err != nil {
				if err == redis.ErrNil {
					me[i] = dsorm.ErrNotStored
				} else if err.Error() == "cas conflict" {
					me[i] = dsorm.ErrCASConflict
				} else {
					me[i] = err
				}
				hasErr = true
			}
		}
	}()

	wg.Wait()

	err = ctx.Err()

	if err != nil {
		return
	}

	if err = flushErr; err != nil {
		return
	}

	if hasErr {
		err = me
		return
	}

	return
}

func (b *backend) DeleteMulti(ctx context.Context, keys []string) (err error) {
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)
	defer func() {
		if cerr := redisConn.CloseContext(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if len(keys) == 0 {
		return
	}

	args := make([]interface{}, len(keys))
	for i, key := range keys {
		args[i] = key
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
	}

	if num, nerr := redis.Int64(redisConn.DoContext(ctx, "DEL", args...)); nerr != nil {
		err = nerr
		return err
	} else if num != int64(len(keys)) {
		err = fmt.Errorf("redis: expected to remove %d keys, but only removed %d", len(keys), num)
		return
	}

	return
}

func (b *backend) GetMulti(ctx context.Context, keys []string) (result map[string]*dsorm.Item, err error) {
	if len(keys) == 0 {
		return
	}
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)
	defer func() {
		if cerr := redisConn.CloseContext(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}()

	args := make([]interface{}, len(keys))
	for i, key := range keys {
		args[i] = key
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
	}

	cachedItems, rerr := redis.ByteSlices(redisConn.DoContext(ctx, "MGET", args...))
	if rerr != nil {
		err = rerr
		return
	}

	result = make(map[string]*dsorm.Item)
	me := make(dsorm.MultiError, len(keys))
	hasErr := false
	if len(cachedItems) != len(keys) {
		return nil, fmt.Errorf("redis: len(cachedItems) != len(keys) (%d != %d)", len(cachedItems), len(keys))
	}
	for i, key := range keys {
		if cacheItem := cachedItems[i]; cacheItem != nil {
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
	}
	if hasErr {
		err = me
		return
	}

	return
}

func (b *backend) SetMulti(ctx context.Context, items []*dsorm.Item) (err error) {
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)
	defer func() {
		if cerr := redisConn.CloseContext(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}()

	err = set(ctx, redisConn, false, items)

	return
}
