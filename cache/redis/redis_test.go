package redis_test

import (
	"context"
	"os"
	"testing"
	"time"

	dso "github.com/altlimit/dsorm/ds"
	"github.com/altlimit/dsorm/cache/redis"
	redigo "github.com/opencensus-integrations/redigo/redis"
)

var (
	redisPool  *redigo.Pool
	redisAddr  = os.Getenv("REDIS_ADDR")
	goodClient dso.Cache
)

func TestRedisCache(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping redis tests...")
		return
	}

	// Setup Redis Connection
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisPool = &redigo.Pool{
		Dial: func() (redigo.Conn, error) {
			return redigo.Dial("tcp", redisAddr, redigo.DialReadTimeout(time.Second))
		},
	}

	client, err := redis.NewCache(context.Background(), redisPool)
	if err != nil {
		t.Fatalf("cannot test redis, error connecting to pool: %v", err)
	}
	goodClient = client

	t.Run("TestNewCache", NewCacheTest())
}

func NewCacheTest() func(t *testing.T) {
	badPool := &redigo.Pool{
		Dial: func() (redigo.Conn, error) {
			return redigo.Dial("tcp", "badaddress:999", redigo.DialReadTimeout(time.Second))
		},
	}

	closingPool := &redigo.Pool{
		Wait:      true,
		MaxActive: 1,
		Dial: func() (redigo.Conn, error) {
			conn, err := redigo.Dial("tcp", redisAddr, redigo.DialReadTimeout(time.Second))
			if err == nil {
				err = conn.Close()
			}
			return conn, err
		},
	}

	ctx := context.Background()
	type args struct {
		ctx  context.Context
		pool *redigo.Pool
	}
	var tests = []struct {
		name      string
		in        args
		expectErr bool
	}{
		{
			"Good Client",
			args{
				ctx:  ctx,
				pool: redisPool,
			},
			false,
		},
		{
			"Bad Pool",
			args{
				ctx:  ctx,
				pool: badPool,
			},
			true,
		},
		{
			"Closing Pool",
			args{
				ctx:  ctx,
				pool: closingPool,
			},
			true,
		},
	}
	return func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if _, err := redis.NewCache(tt.in.ctx, tt.in.pool); (err != nil) != tt.expectErr {
					t.Errorf("expectErr = %v, err = %v", tt.expectErr, err)
				}
			})
		}
	}
}
