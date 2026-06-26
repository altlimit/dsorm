import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";

// Exercises the real CacheDO (SQLite-backed) through the test Worker, verifying
// the on-the-wire contract the Go client depends on. Blobs are base64 strings
// of [4-byte LE flags][value]; here we use arbitrary base64 payloads.

const b64 = (s: string) => btoa(s);

async function op(tenant: string, name: string, body: unknown) {
  const res = await SELF.fetch(`http://cache.do/v1/${tenant}/${name}`, {
    method: "POST",
    body: JSON.stringify(body),
  });
  expect(res.status).toBe(200);
  return res.json() as Promise<any>;
}

describe("CacheDO contract", () => {
  it("set then get round-trips", async () => {
    await op("t1", "set", { items: [{ k: "a", b: b64("hello") }] });
    const got = await op("t1", "get", { keys: ["a"] });
    expect(got.items).toEqual([{ k: "a", b: b64("hello") }]);
  });

  it("add is set-if-absent, including duplicates within a batch", async () => {
    const res = await op("t2", "add", {
      items: [
        { k: "dup", b: b64("first") },
        { k: "dup", b: b64("second") },
      ],
    });
    expect(res.codes).toEqual(["", "notstored"]);
    const got = await op("t2", "get", { keys: ["dup"] });
    expect(got.items[0].b).toBe(b64("first"));
  });

  it("cas succeeds on match, conflicts on change, notstored when missing", async () => {
    await op("t3", "set", { items: [{ k: "x", b: b64("v1") }] });
    const ok = await op("t3", "cas", {
      items: [{ k: "x", b: b64("v2"), c: b64("v1") }],
    });
    expect(ok.codes).toEqual([""]);

    const conflict = await op("t3", "cas", {
      items: [{ k: "x", b: b64("v3"), c: b64("stale") }],
    });
    expect(conflict.codes).toEqual(["conflict"]);

    const missing = await op("t3", "cas", {
      items: [{ k: "ghost", b: b64("v"), c: b64("v") }],
    });
    expect(missing.codes).toEqual(["notstored"]);
  });

  it("delete counts only live rows", async () => {
    await op("t4", "set", { items: [{ k: "d", b: b64("d") }] });
    const res = await op("t4", "delete", { keys: ["d", "missing"] });
    expect(res.count).toBe(1);
  });

  it("increment is atomic and resets after expiry", async () => {
    expect((await op("t5", "incr", { key: "c", delta: 5 })).value).toBe(5);
    expect((await op("t5", "incr", { key: "c", delta: 3 })).value).toBe(8);
    expect((await op("t5", "incr", { key: "c", delta: -10 })).value).toBe(-2);
  });

  it("tenants are isolated", async () => {
    await op("ta", "set", { items: [{ k: "shared", b: b64("a") }] });
    await op("tb", "set", { items: [{ k: "shared", b: b64("b") }] });
    expect((await op("ta", "get", { keys: ["shared"] })).items[0].b).toBe(b64("a"));
    expect((await op("tb", "get", { keys: ["shared"] })).items[0].b).toBe(b64("b"));
  });
});
