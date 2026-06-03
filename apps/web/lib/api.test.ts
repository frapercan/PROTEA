// LOGIN-PERSIST-DEBUG (2026-05-29). Regression guard for the
// ``apps/web/lib/api.ts::withAuth`` helper. The bug shape this file
// pins down: the previous helper only attached ``Authorization:
// Bearer <jwt>`` for mutating methods (POST/PUT/PATCH/DELETE) and
// dropped it for GETs. Role-gated GET endpoints
// (``GET /v1/admin/dlq/summary``, ``GET /v1/admin/users`` after the
// admin gating sweep in PR #569) therefore returned 401 to
// authenticated users, the ``http`` 401-swallow path turned that into
// an empty list, and the user perceived the chrome as "login is not
// persistent" because every admin page silently rendered empty.
//
// These tests exercise ``http`` through a stubbed ``fetch`` so we can
// assert exactly which headers leave the browser for GETs vs writes,
// both with and without the ``protea_session`` cookie. The chrome
// must:
//
//   * attach the Bearer header on GET when the cookie is present
//     (otherwise role-gated GETs 401 and the page goes blank);
//   * attach the Bearer header on POST/PATCH/DELETE when the cookie
//     is present (already covered before this regression but kept
//     here so the contract is exhaustive);
//   * leave the request alone when no cookie is present so anonymous
//     dashboards keep being anonymous.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { SESSION_COOKIE_NAME } from "@/lib/auth";

const FAKE_JWT_PAYLOAD = {
  sub: "user-uuid",
  role: "admin",
  exp: 9_999_999_999,
};

function mintFakeJwt(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  return `${header}.${body}.signature-not-verified-client-side`;
}

function setSessionCookie(token: string | null): void {
  if (token === null) {
    document.cookie = `${SESSION_COOKIE_NAME}=; Path=/; Max-Age=0`;
    return;
  }
  document.cookie = `${SESSION_COOKIE_NAME}=${encodeURIComponent(token)}; Path=/`;
}

function makeJsonResponse(body: unknown, init: ResponseInit = { status: 200 }): Response {
  return new Response(JSON.stringify(body), {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
}

describe("lib/api withAuth + http", () => {
  let fetchSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.stubEnv("NEXT_PUBLIC_API_URL", "https://protea.example.test");
    fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    setSessionCookie(null);
  });

  afterEach(() => {
    setSessionCookie(null);
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
    vi.resetModules();
  });

  it("attaches Authorization: Bearer on GET when the session cookie is present (LOGIN-PERSIST-DEBUG regression)", async () => {
    const token = mintFakeJwt(FAKE_JWT_PAYLOAD);
    setSessionCookie(token);
    fetchSpy.mockResolvedValueOnce(makeJsonResponse({ ok: true }));

    const api = await import("@/lib/api");
    await api.getDlqSummary(100);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("https://protea.example.test/v1/admin/dlq/summary?max_peek=100");
    const headers = new Headers(init?.headers);
    expect(headers.get("Authorization")).toBe(`Bearer ${token}`);
  });

  it("attaches Authorization: Bearer on POST when the session cookie is present", async () => {
    const token = mintFakeJwt(FAKE_JWT_PAYLOAD);
    setSessionCookie(token);
    fetchSpy.mockResolvedValueOnce(makeJsonResponse({ id: "j-1", status: "queued" }));

    const api = await import("@/lib/api");
    await api.createJob({ operation: "ping", queue_name: "protea.ping" });

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    const headers = new Headers(init?.headers);
    expect(headers.get("Authorization")).toBe(`Bearer ${token}`);
  });

  it("does not attach Authorization on GET when no session cookie is present", async () => {
    setSessionCookie(null);
    fetchSpy.mockResolvedValueOnce(makeJsonResponse([]));

    const api = await import("@/lib/api");
    await api.listJobs();

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    const headers = new Headers(init?.headers);
    expect(headers.get("Authorization")).toBeNull();
  });

  it("swallows 401 on GET and resolves to an empty list (public-viewer policy preserved)", async () => {
    setSessionCookie(null);
    fetchSpy.mockResolvedValueOnce(new Response("", { status: 401 }));

    const api = await import("@/lib/api");
    const rows = await api.listJobs();

    expect(rows).toEqual([]);
  });

  it("re-raises 401 on POST so the caller surfaces a sign-in CTA", async () => {
    setSessionCookie(null);
    fetchSpy.mockResolvedValueOnce(new Response("bad token", { status: 401 }));

    const api = await import("@/lib/api");
    await expect(
      api.createJob({ operation: "ping", queue_name: "protea.ping" }),
    ).rejects.toThrow(/bad token|HTTP 401/);
  });
});
