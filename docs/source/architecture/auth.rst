Authentication
==============

PROTEA's HTTP API guards its most sensitive POST endpoints with API
keys (T5.6a). Bearer JWT (T5.6b) and oauth2-proxy in front of the
admin UI (T5.6c) are deferred to follow-up slices; this page only
covers the first iteration.

Protected routes
----------------

The following endpoints reject unauthenticated requests with a 401:

* ``POST /v1/jobs``
* ``POST /v1/datasets``
* ``POST /v1/reranker-models/import``
* ``POST /v1/reranker-models/import-by-reference``

GET endpoints stay open for now (researcher UX). Full lockdown is
post T5.6b.

Header format
-------------

Two equivalent header shapes are accepted:

.. code-block:: http

   Authorization: ApiKey <raw_key>

.. code-block:: http

   X-Api-Key: <raw_key>

Both round-trip through :func:`protea.api.auth.require_api_key`. A
missing, malformed, or revoked key returns RFC 7807 problem details
with status 401 and a ``WWW-Authenticate: ApiKey`` header so generic
HTTP clients know which scheme to retry under.

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

* No scopes or role-based access control. A key either passes or
  does not; there are no permission tiers within the authenticated
  group. T5.6b will add Bearer JWT for richer claims.
* No rate limiting. T5.6b ships ``slowapi`` on top of the same
  dependency so misbehaving clients can be throttled.
* No OIDC / human SSO. T5.6c (post-defensa) wires
  ``oauth2-proxy`` in front of the admin UI; until then the
  ``/v1/auth/api-keys`` endpoints themselves are intentionally
  unauthenticated and should be reachable only from a trusted
  network.
