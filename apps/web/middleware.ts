import { NextResponse, type NextRequest } from "next/server";
import createMiddleware from "next-intl/middleware";
import { routing } from "./i18n/routing";
import { SESSION_COOKIE_NAME, roleFromToken, hasRole, type Role } from "./lib/auth";

// FEAT-AUTH (WAVE-2 2026-05-23) — session/cookie gate.
//
// The previous middleware was a thin next-intl wrapper. We now layer
// a session gate on top so operator/admin pages return 403 to viewers
// (or anonymous visitors). The gate is intentionally permissive by
// default: any path NOT listed in PROTECTED stays open and rides the
// next-intl pipeline as before. This keeps public dashboards
// (showcase, benchmark, proteins, stack) anonymous-by-default per
// the WAVE-2 spec: "Public pages stay open when no cookie present
// (read-only viewer); when cookie present, role propagates."

const intl = createMiddleware(routing);

// Each entry is a (regex, min role) pair. The regex is matched against
// the path AFTER the optional ``/<locale>/`` prefix is stripped, so the
// rule survives ``/en/maintenance`` and ``/es/admin/...`` alike.
const PROTECTED: ReadonlyArray<[RegExp, Role]> = [
  // /maintenance/* — vacuums and other ops-driven workflows.
  [/^\/maintenance(\/|$)/, "operator"],
  // /admin/* — destructive controls (reset-db, key-management UI to come).
  [/^\/admin(\/|$)/, "admin"],
];

function stripLocale(pathname: string): string {
  // routing.locales lives on the runtime config; replicate the prefix
  // strip manually so this stays edge-runtime compatible (we cannot
  // import the full routing helpers inside the middleware bundle).
  const parts = pathname.split("/");
  if (parts.length >= 2 && parts[1].length === 2) {
    return "/" + parts.slice(2).join("/");
  }
  return pathname;
}

function findGate(pathname: string): Role | null {
  for (const [pattern, role] of PROTECTED) {
    if (pattern.test(pathname)) return role;
  }
  return null;
}

export default function middleware(request: NextRequest) {
  const stripped = stripLocale(request.nextUrl.pathname);
  const required = findGate(stripped);
  if (required !== null) {
    const token = request.cookies.get(SESSION_COOKIE_NAME)?.value ?? null;
    const role = roleFromToken(token);
    if (!hasRole(role, required)) {
      // 403 with a small body. Pages that get a 403 here are not
      // expected to be deep-linked by viewers; the chrome's AuthChip
      // still nudges them to /docs#/auth for the sign-in dance.
      return new NextResponse(
        `Forbidden: this page requires role '${required}' (current role: '${role}').`,
        { status: 403, headers: { "Content-Type": "text/plain; charset=utf-8" } },
      );
    }
  }
  return intl(request);
}

export const config = {
  matcher: [
    "/((?!api|_next|_vercel|sphinx|docs|static|api-proxy|.*\\..*).*)",
  ],
};
