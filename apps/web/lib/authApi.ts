// FARM-AUTH.10 (2026-05-24) — client helpers for the User-based auth
// flow shipped in FARM-AUTH.3 (``/auth/signup`` + ``/auth/login`` +
// ``/auth/logout`` + ``GET /auth/me``).
//
// Why a separate module from ``lib/api.ts``? The User flow lives
// exclusively behind the same-origin ``/api-proxy`` rewrite so the
// browser will actually send the ``protea_session`` cookie back on
// subsequent requests. ``lib/api.ts`` builds full ``baseUrl()`` URLs
// which point at the API origin directly; that bypasses the rewrite
// and trips ``SameSite=strict`` (the cookie is rejected because the
// page that holds the form lives at the Next.js origin). Keeping the
// auth flow on the proxy path avoids the cross-origin landmine while
// also letting ``HttpOnly; Secure; SameSite=strict`` stay on, which
// is the only same-origin CSRF surface we have (ADR D37).
//
// The shape of every response below mirrors the FastAPI routers in
// ``protea/api/routers/auth_user.py``. ``UserMe`` is what
// ``GET /auth/me`` returns; ``SignupResponse`` is the 201 body from
// ``POST /auth/signup``; ``LoginResponse`` is the 200 body from
// ``POST /auth/login`` (the session cookie is set as a side effect).
//
// All call sites that depend on the cookie session SHOULD route their
// mutations through these helpers so the cookie + Bearer header stay
// consistent. GETs go via ``GET /auth/me`` which reads the cookie
// directly on the server.

export type UserRole = "viewer" | "researcher" | "operator" | "admin";

export type UserStatus = "pending" | "active" | "deactivated";

export type UserMe = {
  id: string;
  email: string;
  username: string;
  display_name: string | null;
  role: UserRole;
  status: UserStatus;
};

export type SignupResponse = {
  id: string;
  email: string;
  username: string;
  status: UserStatus;
};

export type SignupRequest = {
  email: string;
  username: string;
  password: string;
  display_name?: string | null;
  intended_use?: string | null;
};

export type LoginRequest = {
  email: string;
  password: string;
};

export type LoginResponse = UserMe;

/** Wraps fetch with the same JSON-or-throw contract used elsewhere in
 *  the app. The path argument is the API path AFTER ``/v1`` (e.g.
 *  ``/auth/login``); we always go through ``/api-proxy/v1`` so the
 *  cookie domain matches the Next.js origin. */
async function postJson<T>(path: string, body: unknown, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api-proxy/v1${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    credentials: "include",
    cache: "no-store",
    ...init,
  });
  return parseOrThrow<T>(res);
}

async function getJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api-proxy/v1${path}`, {
    credentials: "include",
    cache: "no-store",
    ...init,
  });
  return parseOrThrow<T>(res);
}

/** Shared {ok, status, detail} extractor. Surfaces the FastAPI
 *  ``HTTPException(detail=...)`` slug so callers can switch on it
 *  (``invalid_credentials``, ``account_pending_approval``, …). */
async function parseOrThrow<T>(res: Response): Promise<T> {
  if (res.status === 204) return undefined as unknown as T;
  const text = await res.text();
  let parsed: unknown = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    // Non-JSON (e.g. HTML 502 from a misconfigured proxy) — bubble the raw text.
    if (!res.ok) {
      throw new AuthError(res.status, text || `HTTP ${res.status}`);
    }
    return undefined as unknown as T;
  }
  if (!res.ok) {
    const detail =
      parsed && typeof parsed === "object" && parsed !== null && "detail" in parsed
        ? String((parsed as { detail?: unknown }).detail ?? `HTTP ${res.status}`)
        : `HTTP ${res.status}`;
    throw new AuthError(res.status, detail);
  }
  return parsed as T;
}

/** Stable error shape so pages can `instanceof` switch on status. */
export class AuthError extends Error {
  status: number;
  detail: string;
  constructor(status: number, detail: string) {
    super(detail);
    this.status = status;
    this.detail = detail;
    this.name = "AuthError";
  }
}

export function signup(body: SignupRequest) {
  return postJson<SignupResponse>("/auth/signup", body);
}

export function login(body: LoginRequest) {
  return postJson<LoginResponse>("/auth/login", body);
}

export async function logout(): Promise<void> {
  await postJson<void>("/auth/logout", {});
}

export function me() {
  return getJson<UserMe>("/auth/me");
}

// ---------------------------------------------------------------------------
// Admin user-management — FORWARD-COMPATIBLE WIRING
// ---------------------------------------------------------------------------
//
// FARM-AUTH.4 / .5 will land ``GET /admin/users``, ``POST /admin/users/{id}/approve``,
// ``POST /admin/users/{id}/deactivate`` and ``POST /admin/users/{id}/role``.
// The admin page below tries to hit those endpoints, and gracefully
// shows a "backend endpoint not yet available" panel on 404 so the
// UI ships ahead of (or alongside) the API and starts working the
// instant the FastAPI routers merge. We deliberately avoid stubbing
// fake data here per the FARM-AUTH.10 plan rule.

export type AdminUserRow = {
  id: string;
  email: string;
  username: string;
  display_name: string | null;
  role: UserRole;
  status: UserStatus;
  intended_use?: string | null;
  created_at: string | null;
  last_login_at: string | null;
};

export async function listAdminUsers(): Promise<AdminUserRow[]> {
  return getJson<AdminUserRow[]>("/admin/users");
}

export function approveAdminUser(id: string) {
  return postJson<AdminUserRow>(`/admin/users/${encodeURIComponent(id)}/approve`, {});
}

export function deactivateAdminUser(id: string) {
  return postJson<AdminUserRow>(`/admin/users/${encodeURIComponent(id)}/deactivate`, {});
}

export function setAdminUserRole(id: string, role: UserRole) {
  return postJson<AdminUserRow>(`/admin/users/${encodeURIComponent(id)}/role`, { role });
}
