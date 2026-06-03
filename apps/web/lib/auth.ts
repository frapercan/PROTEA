// FEAT-AUTH (WAVE-2 2026-05-23) — client-side session helpers.
//
// The backend mints a Bearer JWT at POST /v1/auth/login that carries
// sub+exp+iat+role. We park it in a non-HttpOnly cookie called
// ``protea_session`` so middleware.ts (Edge runtime) AND the browser
// (lib/api.ts mutations + AuthChip) can both read it without a round
// trip. The token is short-lived (1h default); a refresh flow lives
// in a separate slice.
//
// Decoding rule: the payload claim is base64url-decoded client-side
// for read-only chrome (AuthChip, useRole). We do NOT verify the
// signature here -- that is the backend's job on every protected
// request. The cookie value is opaque to the browser as far as
// authorisation is concerned; what we render from it is purely
// cosmetic. If a user tampers with the cookie they only fool the UI;
// the backend rejects the forged token at the gate.

export type Role = "viewer" | "researcher" | "operator" | "admin";

const COOKIE_NAME = "protea_session";

// ``viewer`` is the synthesized anonymous / API-key floor, kept for
// pre-FARM-AUTH.3 sessions; ``researcher`` is the default User role
// minted by signup (between viewer and operator in the ranking).
const KNOWN_ROLES: ReadonlyArray<Role> = ["viewer", "researcher", "operator", "admin"];

const RANK: Record<Role, number> = { viewer: 0, researcher: 1, operator: 2, admin: 3 };

export type SessionClaims = {
  sub?: string;
  exp?: number;
  iat?: number;
  role?: Role;
  [key: string]: unknown;
};

/**
 * Read the raw JWT from ``document.cookie``. Returns ``null`` outside
 * the browser (SSR) or when the cookie is absent / empty.
 *
 * Exported so middleware-adjacent code (which has its own request-
 * cookies API) and tests can swap the source.
 */
export function getSessionTokenFromCookieString(cookieStr: string | undefined | null): string | null {
  if (!cookieStr) return null;
  const parts = cookieStr.split(/;\s*/);
  for (const p of parts) {
    const eq = p.indexOf("=");
    if (eq <= 0) continue;
    const name = p.slice(0, eq);
    if (name !== COOKIE_NAME) continue;
    const value = decodeURIComponent(p.slice(eq + 1));
    return value || null;
  }
  return null;
}

export function getSessionToken(): string | null {
  if (typeof document === "undefined") return null;
  return getSessionTokenFromCookieString(document.cookie);
}

/**
 * Base64url-decode the JWT payload. Returns null on any malformed
 * input; never throws so the calling chrome code can fall through
 * to ``anonymous`` rendering.
 */
export function decodeJwtPayload(token: string | null | undefined): SessionClaims | null {
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length < 2) return null;
  const payload = parts[1];
  try {
    const padded = payload + "=".repeat((4 - (payload.length % 4)) % 4);
    const b64 = padded.replace(/-/g, "+").replace(/_/g, "/");
    const raw = typeof atob === "function"
      ? atob(b64)
      : Buffer.from(b64, "base64").toString("binary");
    const json = decodeURIComponent(
      Array.prototype.map
        .call(raw, (c: string) => "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2))
        .join(""),
    );
    return JSON.parse(json) as SessionClaims;
  } catch {
    return null;
  }
}

function normaliseRole(raw: unknown): Role {
  if (typeof raw !== "string") return "viewer";
  const candidate = raw.trim().toLowerCase();
  return (KNOWN_ROLES as ReadonlyArray<string>).includes(candidate) ? (candidate as Role) : "viewer";
}

/**
 * Resolve the current session's role. Anonymous (no cookie) and
 * expired sessions both surface as ``viewer`` so operator gates fail
 * closed by default. ``nowSec`` is injectable for tests.
 */
export function roleFromToken(token: string | null | undefined, nowSec: number = Math.floor(Date.now() / 1000)): Role {
  const claims = decodeJwtPayload(token);
  if (!claims) return "viewer";
  if (typeof claims.exp === "number" && claims.exp < nowSec) return "viewer";
  return normaliseRole(claims.role);
}

/**
 * Build a ``{Authorization: Bearer <token>}`` headers fragment when
 * the session cookie is present, else an empty object. Used by every
 * mutation in ``lib/api.ts``. GETs are explicitly unauthenticated so
 * public dashboards keep working without a session.
 */
export function authHeaders(): Record<string, string> {
  const token = getSessionToken();
  if (!token) return {};
  return { Authorization: `Bearer ${token}` };
}

export function hasRole(current: Role, required: Role): boolean {
  return RANK[current] >= RANK[required];
}

export { COOKIE_NAME as SESSION_COOKIE_NAME };
