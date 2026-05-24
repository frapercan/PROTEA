// FEAT-AUTH (WAVE-2 2026-05-23) — unit tests for the client-side
// session helpers. Pure functions: no React hooks, no DOM, no fetch.

import { describe, expect, it } from "vitest";
import {
  authHeaders,
  decodeJwtPayload,
  getSessionTokenFromCookieString,
  hasRole,
  roleFromToken,
  SESSION_COOKIE_NAME,
} from "@/lib/auth";

function mintFakeJwt(payload: Record<string, unknown>): string {
  // header.payload.signature — only payload is read by decodeJwtPayload.
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  return `${header}.${body}.signature-not-verified-client-side`;
}

describe("getSessionTokenFromCookieString", () => {
  it("returns the value when the cookie is present", () => {
    const t = mintFakeJwt({ sub: "x", role: "operator", exp: 9_999_999_999 });
    const cookies = `other=foo; ${SESSION_COOKIE_NAME}=${encodeURIComponent(t)}; bar=baz`;
    expect(getSessionTokenFromCookieString(cookies)).toBe(t);
  });

  it("returns null when the cookie is absent or empty", () => {
    expect(getSessionTokenFromCookieString("")).toBeNull();
    expect(getSessionTokenFromCookieString(null)).toBeNull();
    expect(getSessionTokenFromCookieString("foo=bar")).toBeNull();
    expect(getSessionTokenFromCookieString(`${SESSION_COOKIE_NAME}=`)).toBeNull();
  });
});

describe("decodeJwtPayload", () => {
  it("decodes a well-formed payload", () => {
    const t = mintFakeJwt({ sub: "u-1", role: "admin", exp: 10 });
    const claims = decodeJwtPayload(t);
    expect(claims?.sub).toBe("u-1");
    expect(claims?.role).toBe("admin");
    expect(claims?.exp).toBe(10);
  });

  it("returns null on garbage input", () => {
    expect(decodeJwtPayload("")).toBeNull();
    expect(decodeJwtPayload(null)).toBeNull();
    expect(decodeJwtPayload("only-one-segment")).toBeNull();
    // Malformed second segment that isn't valid base64-encoded JSON
    // collapses to null too (the decoder swallows the SyntaxError).
    expect(decodeJwtPayload("not.@@@.jwt")).toBeNull();
  });
});

describe("roleFromToken", () => {
  it("returns viewer when no token", () => {
    expect(roleFromToken(null)).toBe("viewer");
  });

  it("returns the claimed role when valid + not expired", () => {
    const t = mintFakeJwt({ role: "operator", exp: 9_999_999_999, sub: "x" });
    expect(roleFromToken(t)).toBe("operator");
  });

  it("collapses unknown role values to viewer", () => {
    const t = mintFakeJwt({ role: "superuser", exp: 9_999_999_999, sub: "x" });
    expect(roleFromToken(t)).toBe("viewer");
  });

  it("returns viewer when the token is expired", () => {
    const t = mintFakeJwt({ role: "admin", exp: 1, sub: "x" });
    expect(roleFromToken(t, 9_999_999_999)).toBe("viewer");
  });
});

describe("hasRole", () => {
  it("admin satisfies every floor", () => {
    expect(hasRole("admin", "viewer")).toBe(true);
    expect(hasRole("admin", "operator")).toBe(true);
    expect(hasRole("admin", "admin")).toBe(true);
  });

  it("viewer fails operator+ floors", () => {
    expect(hasRole("viewer", "operator")).toBe(false);
    expect(hasRole("viewer", "admin")).toBe(false);
    expect(hasRole("viewer", "viewer")).toBe(true);
  });

  it("operator satisfies operator + viewer but not admin", () => {
    expect(hasRole("operator", "operator")).toBe(true);
    expect(hasRole("operator", "viewer")).toBe(true);
    expect(hasRole("operator", "admin")).toBe(false);
  });
});

describe("authHeaders", () => {
  it("returns empty object when no document.cookie is set", () => {
    // happy-dom resets the document between tests; ensure cookie is empty.
    if (typeof document !== "undefined") {
      document.cookie = `${SESSION_COOKIE_NAME}=; Path=/; Max-Age=0`;
    }
    expect(authHeaders()).toEqual({});
  });

  it("attaches Authorization: Bearer when the cookie is present", () => {
    const t = mintFakeJwt({ sub: "x", role: "operator", exp: 9_999_999_999 });
    document.cookie = `${SESSION_COOKIE_NAME}=${encodeURIComponent(t)}; Path=/`;
    const headers = authHeaders();
    expect(headers["Authorization"]).toBe(`Bearer ${t}`);
    // Cleanup so this test doesn't leak into siblings.
    document.cookie = `${SESSION_COOKIE_NAME}=; Path=/; Max-Age=0`;
  });
});
