// Package cloudflare implements a dsorm cache backend backed by a Cloudflare
// Durable Object.
//
// The backend speaks a small JSON batch protocol (see protocol.go) over HTTP to
// a fixed base URL. In production, dsorm runs inside a Cloudflare Container and
// that URL is an internal virtual hostname intercepted by the container's
// outbound handler, which routes each request to the per-tenant Durable Object
// (idFromName(tenant)) — no public Worker and no public network hop. The same
// protocol is served by a standalone test Worker (see worker/) and by the
// in-memory mock in this package (see NewMockHandler).
//
// Tenancy is resolved entirely from context via ds.WithTenant /
// orm.WithTenantContext. Every cache call targets exactly one tenant (the
// context tenant, or the configured default), which maps to one single-threaded
// Durable Object — making AddMulti, CompareAndSwapMulti and Increment atomic.
package cloudflare

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	dsorm "github.com/altlimit/dsorm/ds"
)

// DefaultTenant is the tenant used when the context carries none.
const DefaultTenant = "default"

// maxBatch bounds keys/items per HTTP request to stay within the Durable
// Object's 128-key storage batch limit.
const maxBatch = 128

// Option configures a Cloudflare cache backend.
type Option func(*backend)

// WithHTTPClient sets the HTTP client used for requests. Defaults to a client
// with a 30s timeout. Provide a shared client with keep-alives for best
// throughput.
func WithHTTPClient(c *http.Client) Option {
	return func(b *backend) { b.http = c }
}

// WithTimeout sets the request timeout on the default HTTP client. Ignored when
// WithHTTPClient is supplied.
func WithTimeout(d time.Duration) Option {
	return func(b *backend) { b.timeout = d }
}

// WithDefaultTenant overrides the tenant used when the context carries none.
func WithDefaultTenant(tenant string) Option {
	return func(b *backend) { b.defaultTenant = tenant }
}

// WithMaxBatch overrides the maximum number of keys/items per HTTP request.
func WithMaxBatch(n int) Option {
	return func(b *backend) {
		if n > 0 {
			b.maxBatch = n
		}
	}
}

type backend struct {
	baseURL       string
	http          *http.Client
	timeout       time.Duration
	defaultTenant string
	maxBatch      int
}

// NewCache creates a dsorm.Cache backed by a Cloudflare Durable Object reachable
// at baseURL (e.g. "http://cache.do" inside a container, or a wrangler dev URL).
func NewCache(baseURL string, opts ...Option) (dsorm.Cache, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("cloudflare: invalid base URL %q: %w", baseURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("cloudflare: base URL %q must include scheme and host", baseURL)
	}

	b := &backend{
		baseURL:       strings.TrimRight(baseURL, "/"),
		timeout:       30 * time.Second,
		defaultTenant: DefaultTenant,
		maxBatch:      maxBatch,
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.http == nil {
		b.http = &http.Client{Timeout: b.timeout}
	}
	return b, nil
}

func (b *backend) tenant(ctx context.Context) string {
	if t := dsorm.TenantFromContext(ctx); t != "" {
		return t
	}
	return b.defaultTenant
}

func ttlMs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(d / time.Millisecond)
}

// codeErr maps a per-item wire result code to its dsorm error.
func codeErr(code string) error {
	switch code {
	case codeOK:
		return nil
	case codeNotStored:
		return dsorm.ErrNotStored
	case codeConflict:
		return dsorm.ErrCASConflict
	default:
		return fmt.Errorf("cloudflare: %s", code)
	}
}

// do performs one batch request against {baseURL}/v1/{tenant}/{op}.
func (b *backend) do(ctx context.Context, tenant, op string, req *batchRequest) (*batchResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	endpoint := b.baseURL + "/v1/" + url.PathEscape(tenant) + "/" + op
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := b.http.Do(httpReq)
	if err != nil {
		// Surface the exact context error when the request was cancelled.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("cloudflare: %s: %w", op, err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(httpResp.Body, 512))
		return nil, fmt.Errorf("cloudflare: %s tenant=%s: status %d: %s", op, tenant, httpResp.StatusCode, bytes.TrimSpace(msg))
	}

	var resp batchResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("cloudflare: %s: decode response: %w", op, err)
	}
	return &resp, nil
}

func (b *backend) AddMulti(ctx context.Context, items []*dsorm.Item) error {
	return b.writeItems(ctx, opAdd, items)
}

func (b *backend) SetMulti(ctx context.Context, items []*dsorm.Item) error {
	return b.writeItems(ctx, opSet, items)
}

// writeItems implements the shared add/set request flow, chunked and with
// per-item error codes assembled into a MultiError.
func (b *backend) writeItems(ctx context.Context, op string, items []*dsorm.Item) error {
	if len(items) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tenant := b.tenant(ctx)
	me := make(dsorm.MultiError, len(items))
	hasErr := false

	for start := 0; start < len(items); start += b.maxBatch {
		end := min(start+b.maxBatch, len(items))
		chunk := items[start:end]
		req := &batchRequest{Items: make([]wireItem, len(chunk))}
		for i, it := range chunk {
			req.Items[i] = wireItem{
				Key:   it.Key,
				Blob:  encodeBlob(it.Flags, it.Value),
				TTLMs: ttlMs(it.Expiration),
			}
		}
		resp, err := b.do(ctx, tenant, op, req)
		if err != nil {
			return err
		}
		for i, code := range resp.Codes {
			if e := codeErr(code); e != nil {
				me[start+i] = e
				hasErr = true
			}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	tenant := b.tenant(ctx)
	me := make(dsorm.MultiError, len(items))
	hasErr := false

	// CAS items must each carry the original blob captured by GetMulti. Items
	// without it cannot have been read and so were never stored.
	pending := make([]int, 0, len(items)) // indices we actually send
	req := &batchRequest{Items: make([]wireItem, 0, len(items))}
	for i, it := range items {
		cas, ok := it.GetCASInfo().([]byte)
		if !ok || cas == nil {
			me[i] = dsorm.ErrNotStored
			hasErr = true
			continue
		}
		pending = append(pending, i)
		req.Items = append(req.Items, wireItem{
			Key:   it.Key,
			Blob:  encodeBlob(it.Flags, it.Value),
			CAS:   cas,
			TTLMs: ttlMs(it.Expiration),
		})
	}

	for off := 0; off < len(req.Items); off += b.maxBatch {
		end := min(off+b.maxBatch, len(req.Items))
		chunk := &batchRequest{Items: req.Items[off:end]}
		resp, err := b.do(ctx, tenant, opCAS, chunk)
		if err != nil {
			return err
		}
		for j, code := range resp.Codes {
			if e := codeErr(code); e != nil {
				me[pending[off+j]] = e
				hasErr = true
			}
		}
	}
	if hasErr {
		return me
	}
	return nil
}

func (b *backend) GetMulti(ctx context.Context, keys []string) (map[string]*dsorm.Item, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tenant := b.tenant(ctx)
	result := make(map[string]*dsorm.Item)

	for start := 0; start < len(keys); start += b.maxBatch {
		end := min(start+b.maxBatch, len(keys))
		resp, err := b.do(ctx, tenant, opGet, &batchRequest{Keys: keys[start:end]})
		if err != nil {
			return result, err
		}
		for _, it := range resp.Items {
			flags, value, ok := decodeBlob(it.Blob)
			if !ok {
				return result, fmt.Errorf("cloudflare: malformed blob for key %q", it.Key)
			}
			item := &dsorm.Item{Key: it.Key, Flags: flags, Value: value}
			// Retain the original blob so a later CAS can detect changes.
			item.SetCASInfo(append([]byte(nil), it.Blob...))
			result[it.Key] = item
		}
	}
	return result, nil
}

func (b *backend) DeleteMulti(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tenant := b.tenant(ctx)
	removed := 0

	for start := 0; start < len(keys); start += b.maxBatch {
		end := min(start+b.maxBatch, len(keys))
		resp, err := b.do(ctx, tenant, opDelete, &batchRequest{Keys: keys[start:end]})
		if err != nil {
			return err
		}
		removed += resp.Count
	}
	if removed != len(keys) {
		return fmt.Errorf("cloudflare: expected to remove %d keys, but only removed %d", len(keys), removed)
	}
	return nil
}

func (b *backend) Increment(ctx context.Context, key string, delta int64, expiration time.Duration) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	resp, err := b.do(ctx, b.tenant(ctx), opIncrement, &batchRequest{
		Key:   key,
		Delta: delta,
		TTLMs: ttlMs(expiration),
	})
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}
