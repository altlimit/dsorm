// PRODUCTION REFERENCE — not used by the test Worker.
//
// In production, dsorm runs inside a Cloudflare Container and reaches the cache
// by making a plain HTTP request to an internal virtual hostname (the Go
// client's baseURL, e.g. "http://cache.do"). The request never leaves the
// runtime: the container's *outbound handler* intercepts it and routes it to
// the per-tenant Durable Object. There is no separate public Worker and no
// public network hop.
//
// Requirements:
//   - @cloudflare/containers >= 0.2.0
//   - the CacheDO class bound in the same Worker (see wrangler.jsonc)
//
// Wire this onto the Container subclass that manages your dsorm container.

import { Container } from "@cloudflare/containers";
import { CacheDO } from "./cache_do";

export { CacheDO };

interface Env {
  CACHE: DurableObjectNamespace<CacheDO>;
}

export class DsormContainer extends Container<Env> {
  defaultPort = 8080; // the port your dsorm app listens on

  // Intercept requests the container makes to http://cache.do/... and forward
  // them to the per-tenant Durable Object. Tenant is the first path segment
  // after /v1/, exactly as the Go client emits it.
  static outboundByHost = {
    "cache.do": async (request: Request, env: Env): Promise<Response> => {
      const url = new URL(request.url);
      const parts = url.pathname.split("/").filter(Boolean); // ["v1", tenant, op]
      if (parts.length !== 3 || parts[0] !== "v1") {
        return new Response("not found", { status: 404 });
      }
      const tenant = decodeURIComponent(parts[1]);
      const stub = env.CACHE.get(env.CACHE.idFromName(tenant));
      return stub.fetch(request);
    },
  };
}
