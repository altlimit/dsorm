# dsorm Cloudflare cache — Durable Object + test Worker

This directory holds the Cloudflare side of the dsorm Cloudflare cache backend:

- `src/cache_do.ts` — **`CacheDO`**, the per-tenant SQLite-backed Durable Object.
  This is the real cache and is used in both production and tests.
- `src/worker.ts` — a standalone **test Worker** that fronts `CacheDO` over HTTP
  for `wrangler dev` and integration tests. Not deployed in production.
- `src/outbound.example.ts` — **production reference**: the container outbound
  handler that routes the in-container HTTP requests to `CacheDO` with no public
  Worker and no public network hop. Wire this onto your dsorm container class.
- `test/cache.test.ts` — vitest tests that run the real DO in-process via
  `@cloudflare/vitest-pool-workers` (workerd).

## HTTP contract

```
POST /v1/{tenant}/{op}        op in: get | set | add | cas | delete | incr
```

Bodies are JSON; binary blobs are base64 strings (Go encodes `[]byte` as base64).
A blob is `[4-byte little-endian flags][value]`, opaque to the DO except `incr`,
which treats the value as an 8-byte little-endian int64. See `protocol.go` in the
parent directory for the authoritative field definitions.

Each request targets exactly one tenant → one `CacheDO` instance
(`idFromName(tenant)`), so add/cas/incr are atomic by the DO's single-threaded
execution.

## Develop & test

```sh
npm install
npm run typecheck     # tsc --noEmit
npm test              # vitest: runs CacheDO in-process (workerd)
npm run dev           # wrangler dev: serve the test Worker locally
```

## Run the Go integration test against a live Worker

`npm run dev` prints a local URL (e.g. `http://localhost:8787`). Point the Go
integration test at it:

```sh
DSORM_CF_CACHE_URL=http://localhost:8787 go test ./cache/cloudflare/ -run Integration
```

Without `DSORM_CF_CACHE_URL`, the Go tests use the in-process mock
(`cloudflare.NewMockHandler`) and need nothing from Cloudflare.

## Production wiring (summary)

1. Bundle `CacheDO` into the Worker that defines your dsorm container.
2. Add the `outboundByHost` handler from `outbound.example.ts` so the container's
   requests to `http://cache.do/...` reach `CacheDO`.
3. In Go: `cloudflare.NewCache("http://cache.do")`, or set
   `DSORM_CF_CACHE_URL=http://cache.do` for auto-detection.
4. Resolve tenants per request with `orm.WithTenantContext(ctx, tenant)`; calls
   without it use the configured default tenant.
