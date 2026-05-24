Authentication
==============

PROTEA's HTTP API guards its most sensitive POST endpoints with API
keys (T5.6a) and Bearer JWT (T5.6b), throttled per principal by
:mod:`slowapi`. oauth2-proxy in front of the admin UI (T5.6c) is
deferred to a post-defensa slice; this page covers the first two
iterations.

Auth gate overview
------------------

The authentication surface sits between the network edge and the
FastAPI application layer. Each incoming request passes through the
gate in order:

.. code-block:: text

                      HTTP request
                           │
   ┌───────────────────────▼───────────────────────────────────────────┐
   │  EDGE  (T5.6c, pending: oauth2-proxy + Authentik for browser UI)  │
   └───────────────────────┬───────────────────────────────────────────┘
                           │
   ┌───────────────────────▼───────────────────────────────────────────┐
   │  RATE LIMITER  (T5.6b, active: slowapi per-principal buckets)     │
   └───────────────────────┬───────────────────────────────────────────┘
                           │
   ┌───────────────────────▼───────────────────────────────────────────┐
   │  AUTH GATE  (T5.6a + T5.6b + FARM-AUTH.5, active)                 │
   │                                                                   │
   │  require_role(min_role) dependency on all mutable endpoints       │
   │  (wraps require_api_key_or_bearer; roles: viewer/operator/admin)  │
   │                                                                   │
   │  Authorization: ApiKey <raw_key>       (API-key form)             │
   │  X-Api-Key: <raw_key>                  (alternative carrier)      │
   │  Authorization: Bearer <jwt>           (JWT form, HS256)          │
   │                                                                   │
   │  key check: sha256(raw_key) vs ApiKey.key_hash                    │
   │  JWT check: HS256 via PROTEA_JWT_SECRET; sub+iat+exp required     │
   │  on failure: 401 + WWW-Authenticate: ApiKey, Bearer               │
   │                                                                   │
   │  dev override: PROTEA_AUTHN_REQUIRED=false skips gate             │
   └───────────────────────┬───────────────────────────────────────────┘
                           │
   ┌───────────────────────▼───────────────────────────────────────────┐
   │  FASTAPI ROUTER  (open GET / protected POST)                      │
   │                                                                   │
   │  Role tiers (FARM-AUTH.5):                                        │
   │    viewer   - POST /v1/annotate, /v1/query-sets, /v1/support     │
   │    operator - POST /v1/jobs, /v1/datasets, /v1/embeddings/*      │
   │               /v1/experiment-runs, /v1/maintenance/*             │
   │               /v1/scoring/configs*, DELETE /v1/scoring/*         │
   │               /v1/reranker-models/import*, /v1/annotations/*     │
   │    admin    - /v1/auth/api-keys (POST/GET/DELETE)                │
   │                                                                   │
   │  Open (no auth):  GET /v1/jobs, GET /v1/proteins,                │
   │                   GET /v1/scoring/*, GET /v1/stack, /v1/health   │
   └───────────────────────────────────────────────────────────────────┘

Role-based endpoint map (FARM-AUTH.5)
--------------------------------------

PROTEA recognises three roles ordered by privilege. Every mutable
endpoint (POST, PATCH, DELETE) now carries an explicit
``require_role(min_role)`` dependency that short-circuits with 403
when the principal's role is below the floor.

**viewer** (any authenticated user)
   ``POST /v1/annotate``, ``POST /v1/query-sets``,
   ``DELETE /v1/query-sets/{set_id}``, ``POST /v1/support``

**operator** (pipeline operators)
   ``POST /v1/jobs``, ``POST|DELETE /v1/datasets``,
   ``POST|DELETE /v1/embeddings/configs``,
   ``POST /v1/embeddings/predict``,
   ``DELETE /v1/embeddings/prediction-sets/{set_id}``,
   ``POST|PATCH|DELETE /v1/experiment-runs``,
   ``POST /v1/maintenance/vacuum-embeddings``, ``POST /v1/maintenance/vacuum-sequences``,
   ``POST|DELETE /v1/reranker-models/import*``,
   ``POST|DELETE /v1/scoring/configs*``,
   ``DELETE /v1/scoring/rerankers/{reranker_id}``,
   ``POST|DELETE /v1/annotations/sets/*``,
   ``PATCH /v1/annotations/snapshots/{snapshot_id}/ia-url``,
   ``POST /v1/annotations/snapshots/load``,
   ``POST|DELETE /v1/annotations/evaluation-sets/*``

**admin** (platform administrators)
   ``POST /v1/auth/api-keys``,
   ``GET  /v1/auth/api-keys``,
   ``DELETE /v1/auth/api-keys/{key_id}``

GET endpoints remain open (no authentication required) for researcher
UX. Full read-lockdown is deferred to T5.6c (oauth2-proxy).

Header format
-------------

Three equivalent header shapes are accepted:

.. code-block:: text

   Authorization: ApiKey <raw_key>

.. code-block:: text

   X-Api-Key: <raw_key>

.. code-block:: text

   Authorization: Bearer <jwt>

The first two are validated by :func:`protea.api.auth.require_api_key`
(T5.6a). The Bearer flow is handled by :mod:`protea.api.bearer` (HS256
HMAC, secret from ``PROTEA_JWT_SECRET``). Production routes use the
combined :func:`protea.api.auth.require_api_key_or_bearer` dependency,
which accepts either form. A missing, malformed, expired, or revoked
credential returns RFC 7807 problem details with status 401 and a
``WWW-Authenticate: ApiKey, Bearer`` header so generic HTTP clients
know which schemes to retry under.

Bearer JWT
----------

* Algorithm: ``HS256`` with the shared secret from
  ``PROTEA_JWT_SECRET``.
* Minimum payload: ``{sub, iat, exp}``. Tokens missing any of these
  claims are rejected.
* On startup, when ``PROTEA_AUTHN_REQUIRED=true`` and the secret is
  unset, the API process fails loudly via
  :func:`protea.api.bearer.assert_bearer_config`. This is deliberate:
  a misconfigured deployment must not silently 401 every authenticated
  request.
* PROTEA does not issue tokens in this slice; an external signer is
  expected to mint them with the agreed secret. Token issuance via
  OIDC and refresh tokens land in T5.6c.

Rate limits
-----------

Every protected POST is throttled per principal by :mod:`slowapi`. The
bucket key is the API-key prefix, the JWT ``sub``, or the remote IP if
neither auth header is present.

==============================  ===========  ====================================
Route                           Default      Env override
==============================  ===========  ====================================
``POST /v1/jobs``               10/minute    ``PROTEA_RATELIMIT_JOBS``
``POST /v1/datasets``           5/minute     ``PROTEA_RATELIMIT_DATASETS``
``POST /v1/auth/api-keys``      5/hour       ``PROTEA_RATELIMIT_API_KEYS``
==============================  ===========  ====================================

Exceeding the limit returns a 429 problem response with a
``Retry-After`` header. Overrides accept any slowapi syntax
(``"100/minute"``, ``"1000/hour;200/minute"``, ...).

Creating a key
--------------

Mint a new key with the operator endpoint:

.. code-block:: bash

   curl -X POST http://localhost:8000/v1/auth/api-keys \
     -H 'Content-Type: application/json' \
     -d '{"name": "lab-runner-2026-05"}'

Response (the only chance to copy the raw value):

.. code-block:: json

   {
     "id": "<uuid>",
     "prefix": "abc12345",
     "name": "lab-runner-2026-05",
     "key": "abc12345_the_rest_of_the_secret",
     "created_at": "2026-05-11T12:00:00+00:00",
     "revoked_at": null,
     "last_used_at": null
   }

PROTEA stores only the sha256 hash of the raw key and an 8-character
display prefix. Lost keys cannot be recovered, only revoked and
replaced.

Revoking a key
--------------

.. code-block:: bash

   curl -X DELETE http://localhost:8000/v1/auth/api-keys/<id>

The row is preserved with ``revoked_at`` set; subsequent uses of the
key are rejected by :func:`require_api_key`.

Dev override
------------

For local development without keys, set
``PROTEA_AUTHN_REQUIRED=false`` in the API process environment. The
dependency short-circuits and waves every request through. The
default is on, so production deployments stay safe by accident: an
operator must explicitly opt out.

Threat model and follow-ups
---------------------------

This iteration is a forward-defence layer, not a full identity story:

* No scopes or role-based access control. A credential either passes
  or does not; there are no permission tiers within the authenticated
  group. T5.6c will revisit RBAC together with the OIDC layer.
* No OIDC / human SSO. T5.6c (post-defensa) wires
  ``oauth2-proxy`` in front of the admin UI; until then the
  ``/v1/auth/api-keys`` endpoints now require ``role=admin``
  (FARM-AUTH.5); bootstrap the first admin key via a direct SQL
  insert or the ``PROTEA_ADMIN_TOKEN`` env var on a trusted host.
* Bearer signing is symmetric (HS256). A leaked secret invalidates
  every issued token; RS256 / asymmetric keys land in T5.6c.
