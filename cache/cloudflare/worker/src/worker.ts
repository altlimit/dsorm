import { CacheDO } from "./cache_do";

export { CacheDO };

export interface Env {
  CACHE: DurableObjectNamespace<CacheDO>;
}

/**
 * Test Worker: a standalone HTTP front for the cache used to exercise the real
 * Durable Object (e.g. `wrangler dev`) against the Go client and its
 * integration tests.
 *
 * It implements the dsorm Cloudflare cache contract by routing each request to
 * the per-tenant Durable Object and forwarding it unchanged:
 *
 *   POST /v1/{tenant}/{op}  ->  idFromName(tenant) -> CacheDO.fetch
 *
 * In PRODUCTION this front does not exist as a deployed Worker; the equivalent
 * routing happens inside the container's outbound handler (see
 * outbound.example.ts), so the Go process never makes a public network hop.
 */
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/v1/healthz") {
      return new Response("ok");
    }

    // Expect /v1/{tenant}/{op}
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts.length !== 3 || parts[0] !== "v1") {
      return new Response("not found", { status: 404 });
    }
    const tenant = decodeURIComponent(parts[1]);

    const stub = env.CACHE.get(env.CACHE.idFromName(tenant));
    return stub.fetch(request);
  },
} satisfies ExportedHandler<Env>;
