// @vitest-environment node
//
// The rungs fetch, exercised the way a server component runs it.
//
// `getRungs` called `fetch("/api-proxy/rungs")`. That works in a browser and
// cannot work in Node: a relative URL has nothing to be relative to, so it
// throws "Failed to parse URL" before a request is made. The front page is a
// server component, so its rung fetch had never once succeeded.
//
// Nothing showed it, because the only server-side caller wraps the fetch in a
// catch that drops the frame on failure. That catch was written for an
// occasional network error. The failure was total, so a permanent absence was
// indistinguishable from an intermittent one, and a caption simply never
// appeared.
//
// This file runs under the node environment on purpose. Under jsdom `window`
// exists, `baseUrl()` returns the public path, and the bug is invisible: the
// test would pass on the broken code.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { getRungs } from "@/lib/rungs";

const ORIGINAL = globalThis.fetch;

function captureUrl(): string[] {
  const seen: string[] = [];
  const stub: typeof globalThis.fetch = async (input) => {
    seen.push(String(input));
    return new Response(JSON.stringify({ rungs: [], metric: "f_micro_w" }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };
  globalThis.fetch = vi.fn(stub);
  return seen;
}

beforeEach(() => {
  process.env.NEXT_PUBLIC_API_URL = "/api-proxy";
});

afterEach(() => {
  globalThis.fetch = ORIGINAL;
  vi.restoreAllMocks();
});

describe("getRungs on the server", () => {
  it("requests an absolute URL, because Node cannot resolve a relative one", () => {
    expect(typeof window).toBe("undefined");
    const seen = captureUrl();
    return getRungs().then(() => {
      expect(seen).toHaveLength(1);
      expect(seen[0]).toMatch(/^https?:\/\//);
    });
  });

  it("uses the internal base rather than the public path", () => {
    process.env.PROTEA_INTERNAL_API_URL = "http://api.internal:9999";
    const seen = captureUrl();
    return getRungs().then(() => {
      expect(seen[0]).toBe("http://api.internal:9999/rungs");
      delete process.env.PROTEA_INTERNAL_API_URL;
    });
  });

  it("still reaches the rungs route and not some other path", () => {
    const seen = captureUrl();
    return getRungs().then(() => {
      expect(seen[0].endsWith("/rungs")).toBe(true);
    });
  });
});

describe("getRungs failure mode", () => {
  it("rejects rather than throwing synchronously when the base is unset", async () => {
    // The caller wraps this in `.catch()`. A synchronous throw escapes before
    // the promise exists, so the catch never attaches and the exception takes
    // down the server render instead of dropping one caption. That is how the
    // fix for the relative-URL bug broke the front page's H1: the bug it
    // replaced failed asynchronously and was therefore survivable.
    delete process.env.NEXT_PUBLIC_API_URL;
    let threw = false;
    try {
      const p = getRungs();
      expect(p).toBeInstanceOf(Promise);
      await p;
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  });

  it("survives the caller's catch, which is what the caller relies on", async () => {
    delete process.env.NEXT_PUBLIC_API_URL;
    const out = await getRungs()
      .then((r) => r.rungs)
      .catch(() => []);
    expect(out).toEqual([]);
  });
});

describe("a 200 with the wrong shape", () => {
  function replyWith(body: unknown) {
    const stub: typeof globalThis.fetch = async () =>
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    globalThis.fetch = vi.fn(stub);
  }

  it("rejects rather than resolving to something with no rungs", async () => {
    // What the e2e mock actually returns for an unknown route. Resolving
    // this gave the caller `undefined`, which threw one line later, outside
    // the promise chain, where its catch could not see it.
    replyWith([]);
    await expect(getRungs()).rejects.toThrow(/no rungs array/);
  });

  it("rejects an object whose rungs is not an array", async () => {
    replyWith({ rungs: "soon" });
    await expect(getRungs()).rejects.toThrow(/no rungs array/);
  });

  it("accepts the real shape", async () => {
    replyWith({ rungs: [], metric: "f_micro_w" });
    await expect(getRungs()).resolves.toEqual({ rungs: [], metric: "f_micro_w" });
  });

  it("leaves the caller's catch able to do its job", async () => {
    // The property the front page depends on: any failure, of any kind,
    // ends as an empty list rather than as a blank page.
    replyWith([]);
    await expect(
      getRungs()
        .then((r) => r.rungs)
        .catch(() => []),
    ).resolves.toEqual([]);
  });
});
