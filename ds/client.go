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

	// TODO: Client is exported since we embedded datastore.Client - fix this
	*datastore.Client
}

// ClientOption option for dsorm Client.
type ClientOption func(*Client)

func WithDatastoreClient(ds *datastore.Client) ClientOption {
	return func(c *Client) {
		c.Client = ds
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

	if client.Client == nil {
		// Default datastore.Client
		if ds, err := datastore.NewClient(ctx, ""); err != nil {
			return nil, err
		} else {
			client.Client = ds
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
