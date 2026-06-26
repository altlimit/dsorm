import { DurableObject } from "cloudflare:workers";

/**
 * CacheDO is the per-tenant cache backend. One Durable Object instance is
 * created per tenant (via idFromName(tenant)), so its SQLite storage is
 * isolated to that tenant and its single-threaded execution makes add / cas /
 * increment atomic without explicit locking.
 *
 * It implements the dsorm Cloudflare cache HTTP contract:
 *
 *   POST /v1/{tenant}/{op}   op in: get | set | add | cas | delete | incr
 *
 * Stored values ("blobs") are opaque to the DO (a 4-byte LE flags header plus
 * the value bytes, produced by the Go client) except for `incr`, which treats
 * the value as an 8-byte little-endian int64. Blobs travel as base64 strings
 * because the Go client encodes []byte as base64 JSON.
 */
export class CacheDO extends DurableObject {
  private sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS cache(
         key TEXT PRIMARY KEY,
         blob TEXT NOT NULL,
         expires_at INTEGER
       )`,
    );
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const op = url.pathname.split("/").filter(Boolean).pop() ?? "";
    const body = (await request.json()) as BatchRequest;
    const now = Date.now();

    switch (op) {
      case "get":
        return json(this.get(body.keys ?? [], now));
      case "set":
        return json(this.write(body.items ?? [], now, false));
      case "add":
        return json(this.write(body.items ?? [], now, true));
      case "cas":
        return json(this.cas(body.items ?? [], now));
      case "delete":
        return json(this.delete(body.keys ?? [], now));
      case "incr":
        return json(this.increment(body, now));
      default:
        return new Response("unknown op", { status: 404 });
    }
  }

  private get(keys: string[], now: number): BatchResponse {
    const items: WireItem[] = [];
    for (const key of keys) {
      const row = this.sql
        .exec(
          "SELECT blob, expires_at FROM cache WHERE key = ?",
          key,
        )
        .toArray()[0] as Row | undefined;
      if (!row) continue;
      if (isExpired(row.expires_at, now)) {
        this.sql.exec("DELETE FROM cache WHERE key = ?", key);
        continue;
      }
      items.push({ k: key, b: row.blob });
    }
    return { items };
  }

  private write(items: WireItem[], now: number, addOnly: boolean): BatchResponse {
    const codes: string[] = [];
    for (const it of items) {
      if (addOnly) {
        const row = this.sql
          .exec("SELECT expires_at FROM cache WHERE key = ?", it.k)
          .toArray()[0] as Pick<Row, "expires_at"> | undefined;
        if (row && !isExpired(row.expires_at, now)) {
          codes.push("notstored");
          continue;
        }
      }
      this.sql.exec(
        "INSERT INTO cache(key, blob, expires_at) VALUES(?, ?, ?) " +
          "ON CONFLICT(key) DO UPDATE SET blob = excluded.blob, expires_at = excluded.expires_at",
        it.k,
        it.b ?? "",
        expiryFrom(now, it.t),
      );
      codes.push("");
    }
    return { codes };
  }

  private cas(items: WireItem[], now: number): BatchResponse {
    const codes: string[] = [];
    for (const it of items) {
      const row = this.sql
        .exec("SELECT blob, expires_at FROM cache WHERE key = ?", it.k)
        .toArray()[0] as Row | undefined;
      if (!row || isExpired(row.expires_at, now)) {
        this.sql.exec("DELETE FROM cache WHERE key = ?", it.k);
        codes.push("notstored");
        continue;
      }
      if (row.blob !== (it.c ?? "")) {
        codes.push("conflict");
        continue;
      }
      this.sql.exec(
        "UPDATE cache SET blob = ?, expires_at = ? WHERE key = ?",
        it.b ?? "",
        expiryFrom(now, it.t),
        it.k,
      );
      codes.push("");
    }
    return { codes };
  }

  private delete(keys: string[], now: number): BatchResponse {
    let count = 0;
    for (const key of keys) {
      const row = this.sql
        .exec("SELECT expires_at FROM cache WHERE key = ?", key)
        .toArray()[0] as Pick<Row, "expires_at"> | undefined;
      if (!row) continue;
      this.sql.exec("DELETE FROM cache WHERE key = ?", key);
      if (!isExpired(row.expires_at, now)) count++;
    }
    return { count };
  }

  private increment(body: BatchRequest, now: number): BatchResponse {
    const key = body.key ?? "";
    const row = this.sql
      .exec("SELECT blob, expires_at FROM cache WHERE key = ?", key)
      .toArray()[0] as Row | undefined;

    let n = 0n;
    let expiresAt: number | null = null;
    if (row && !isExpired(row.expires_at, now)) {
      n = readLE64(row.blob);
      expiresAt = row.expires_at; // preserve existing expiry by default
    }
    n += BigInt(body.delta ?? 0);
    if (body.ttl && body.ttl > 0) {
      expiresAt = now + body.ttl;
    }
    this.sql.exec(
      "INSERT INTO cache(key, blob, expires_at) VALUES(?, ?, ?) " +
        "ON CONFLICT(key) DO UPDATE SET blob = excluded.blob, expires_at = excluded.expires_at",
      key,
      writeLE64(n),
      expiresAt,
    );
    return { value: Number(n) };
  }
}

// ---- wire types (mirror protocol.go) ----
interface WireItem {
  k: string;
  b?: string; // base64 blob
  c?: string; // base64 original blob (cas)
  t?: number; // ttl ms
}
interface BatchRequest {
  items?: WireItem[];
  keys?: string[];
  key?: string;
  delta?: number;
  ttl?: number;
}
interface BatchResponse {
  items?: WireItem[];
  codes?: string[];
  count?: number;
  value?: number;
}
interface Row {
  blob: string;
  expires_at: number | null;
}

// ---- helpers ----
function json(body: BatchResponse): Response {
  return new Response(JSON.stringify(body), {
    headers: { "Content-Type": "application/json" },
  });
}

function isExpired(expiresAt: number | null, now: number): boolean {
  return expiresAt != null && now > expiresAt;
}

function expiryFrom(now: number, ttlMs?: number): number | null {
  return ttlMs && ttlMs > 0 ? now + ttlMs : null;
}

/** Decode a base64 blob as an 8-byte little-endian int64; 0 if not 8 bytes. */
function readLE64(b64: string): bigint {
  const bytes = base64ToBytes(b64);
  if (bytes.length !== 8) return 0n;
  let n = 0n;
  for (let i = 7; i >= 0; i--) n = (n << 8n) | BigInt(bytes[i]);
  return BigInt.asIntN(64, n);
}

/** Encode an int64 as a base64 8-byte little-endian blob. */
function writeLE64(n: bigint): string {
  const u = BigInt.asUintN(64, n);
  const bytes = new Uint8Array(8);
  let v = u;
  for (let i = 0; i < 8; i++) {
    bytes[i] = Number(v & 0xffn);
    v >>= 8n;
  }
  return bytesToBase64(bytes);
}

function base64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function bytesToBase64(bytes: Uint8Array): string {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}
