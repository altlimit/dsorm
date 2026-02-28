package ds

import (
	"context"
	"log"

	"cloud.google.com/go/datastore"
)

type OnErrorFunc func(ctx context.Context, err error)

type Client struct {
	cacher      Cache
	onErrorFn   OnErrorFunc
	cachePrefix string

	Store
}

// ClientOption option for dsorm Client.
type ClientOption func(*Client)

func WithDatastoreClient(ds *datastore.Client) ClientOption {
	return func(c *Client) {
		c.Store = NewCloudStore(ds)
	}
}

func WithStore(s Store) ClientOption {
	return func(c *Client) {
		c.Store = s
	}
}

func WithCachePrefix(prefix string) ClientOption {
	return func(c *Client) {
		c.cachePrefix = prefix
	}
}

// WithOnErrorFunc sets an error handler for internal non-fatal errors.
func WithOnErrorFunc(f OnErrorFunc) ClientOption {
	return func(c *Client) {
		c.onErrorFn = f
	}
}

// NewClient returns a dsorm.Client using the provided cache.
func NewClient(ctx context.Context, cacher Cache, opts ...ClientOption) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	client := &Client{
		cacher:      cacher,
		cachePrefix: "ds:", // Default prefix
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.Store == nil {
		// Default datastore.Client
		if ds, err := datastore.NewClient(ctx, ""); err != nil {
			return nil, err
		} else {
			client.Store = NewCloudStore(ds)
		}
	}

	return client, nil
}

func (c *Client) onError(ctx context.Context, err error) {
	if c.onErrorFn != nil {
		c.onErrorFn(ctx, err)
		return
	}
	log.Println(err)
}

// Close closes the underlying store.
func (c *Client) Close() error {
	return c.Store.Close()
}

// Cacher returns the underlying Cache implementation used by the client.
func (c *Client) Cacher() Cache {
	return c.cacher
}
