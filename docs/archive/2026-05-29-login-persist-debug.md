# Login persistence symptom on role-gated GET pages - 2026-05-29

## Context

User report (recurring): "el login no es persistente". After logging
in at `/es/login/` and clicking into a role-gated admin page
(`/es/admin/dlq/`, `/es/admin/users/`, etc.), the page renders with
the AuthChip still showing the signed-in pill, the sidebar admin
group still visible, but the page body is empty (no DLQ rows, no
user rows, etc.). The user perceives the empty body as proof that
"the login was lost".

Slice: `LOGIN-PERSIST-DEBUG` (Phase F3).

## Prior fixes that are NOT the cause

* PR #504 (`FIX-AUTH-ROLE-SNAPSHOT`, merged 2026-05-24) restored
  `role` propagation in `_lookup_and_validate` for API keys. Already
  deployed.
* PR #535 (`fix(auth): preserve login across navigation`, merged
  2026-05-25) flipped the `protea_session` cookie from `HttpOnly` to
  readable JS so `lib/auth.ts::getSessionToken` works. Already
  deployed.
* PR #540 (`fix(web): unblock /v1/*`) added the missing rewrite and
  middleware-matcher exclusion so direct `/v1/*` calls survive the
  trailing-slash redirect plus next-intl locale prepend. Already
  deployed.

After all three, the symptom RESURFACED on admin DLQ and admin users
pages, which is what this incident captures.

## Root cause

`apps/web/lib/api.ts::withAuth` only attached
`Authorization: Bearer <jwt>` to requests whose method belonged to
`MUTATING_METHODS = {POST, PUT, PATCH, DELETE}`. The comment in the
previous revision rationalised the carve-out as "GETs stay
unauthenticated so public dashboards keep serving anonymous
visitors", which is true for the viewer-policy paths
(`/v1/jobs`, `/v1/datasets`, `/v1/proteins`, ...) where the backend
accepts anonymous principals.

It is false for the role-gated GET surface introduced and tightened
by the admin gating sweep (PR #569, merged 2026-05-27):

* `GET /v1/admin/dlq/summary` (floor: operator)
* `GET /v1/admin/users` (floor: admin)
* `GET /v1/admin/api-keys` (floor: admin)

A logged-in admin clicking `/es/admin/dlq/` triggers
`apps/web/lib/api.ts::getDlqSummary()` which is a `GET`. Without the
Bearer header, the backend `require_role("operator")` gate finds no
principal in `Authorization` and no API-key in `X-Api-Key` and
returns 401 with `WWW-Authenticate: ApiKey, Bearer`. The same
`http` helper that issued the request then matches
`!MUTATING_METHODS.has("GET") && status === 401` and silently
resolves the promise with `[]` (per the public-viewer policy added
in PR #562 to keep anonymous list pages from crashing). The page
component receives an empty list and renders the empty-state copy.
The chrome (which reads `document.cookie` directly) remains
correctly logged in, so the AuthChip still says "Bearer", but the
data-driven body is empty - exactly the "login isn't persistent"
shape the user keeps reporting.

The same backend gate ALSO rejects an API-key principal whose role
does not meet the floor, so API-key holders trying to manage admin
surfaces see the same empty-body symptom for the same reason; the
cookie/JWT path is just the most common trigger.

## Live reproducer (no credentials required)

Anonymous: confirms the gate is live on the canonical admin GET.

```bash
curl -sL -D - "https://protea.ngrok.app/v1/admin/dlq/summary/" \
  | grep -i '^HTTP\|^www-authenticate\|^content-type'
# HTTP/2 401
# content-type: application/problem+json
# www-authenticate: ApiKey, Bearer  (sent by FastAPI auth dep)
```

The frontend's `http` helper at `apps/web/lib/api.ts` then catches
the 401 on a GET, logs a warning to the browser console
(`[api] anonymous GET ... returned 401; returning empty`), and
resolves the call with `[]`. With the session cookie present the
cookie is sent (path=/) but the Bearer header was deliberately
omitted by `withAuth`, so the backend still treats the request as
anonymous.

## Reproducer scoped to the front-end helper

`apps/web/lib/api.test.ts` ships a Vitest suite that stubs `fetch`,
sets the `protea_session` cookie to a base64-encoded fake JWT, and
asserts `Authorization: Bearer <token>` IS attached on GETs. The
pre-fix code path failed that assertion; the fix passes it.

Invoke from `apps/web/`: `npm run test`, optionally with the
`--run lib/api.test.ts` arg-pass via the Vitest CLI separator.

End-to-end variant (manual, requires admin credentials and the
running stack):

```bash
# 1. log in
curl -s -c /tmp/jar -X POST -H "Content-Type: application/json" \
  -d '{"email":"<admin>","password":"<pwd>"}' \
  https://protea.ngrok.app/api-proxy/v1/auth/login/

# 2. GET admin DLQ summary WITHOUT the Bearer header (browser default
#    before fix) -> 401
curl -s -b /tmp/jar -D - \
  https://protea.ngrok.app/v1/admin/dlq/summary/ \
  | grep -i '^HTTP'
# HTTP/2 401

# 3. GET admin DLQ summary WITH the Bearer header (post-fix behaviour)
#    -> 200
TOKEN=$(grep protea_session /tmp/jar | awk '{print $7}')
curl -s -H "Authorization: Bearer ${TOKEN}" -D - \
  https://protea.ngrok.app/v1/admin/dlq/summary/ \
  | grep -i '^HTTP'
# HTTP/2 200
```

## Fix

`apps/web/lib/api.ts::withAuth` now attaches the Bearer header on
every request when the session cookie is present, not only on
mutations. The 401-swallow path on GETs is preserved for the
anonymous-visitor case (no cookie -> no Bearer -> 401 surfaces
empty list).

## Regression guard

`apps/web/lib/api.test.ts` covers:

* `Authorization: Bearer <jwt>` IS attached on GET when the cookie is
  present (the bug condition);
* same for POST (was already correct, kept to make the contract
  exhaustive);
* no `Authorization` header attached when no cookie is present
  (anonymous viewer path stays intact);
* 401 on a GET still resolves to `[]` (public-viewer policy still
  honoured);
* 401 on a POST still throws so the caller can surface a sign-in CTA.

## Followups not addressed by this slice

* `apps/web/lib/auth.ts::RANK` lists four roles
  (`viewer < researcher < operator < admin`) but the backend
  `protea/api/roles.py::_RANK` only knows three
  (`viewer < operator < admin`); a `researcher` JWT will normalise
  to `viewer` server-side. This is a separate role-mismatch bug
  tracked elsewhere.
* `apps/web/lib/api.ts::http` swallows GET 401s globally; some
  routes might want to surface a more explicit "sign in" CTA
  instead of an empty list, but changing that behaviour belongs to
  a UX slice, not this hotfix.
