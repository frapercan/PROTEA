"""Password hashing primitives for FARM-AUTH (ADR D37 Wave 3).

This module is the single source of truth for hashing and verifying
``User.password_hash`` values. The User table that depends on these
helpers is added by the same FARM-AUTH.1 slice; subsequent slices
(login, signup, admin bootstrap) call into here exclusively.

Algorithm
---------

Argon2id via the :mod:`argon2-cffi` reference binding. Argon2id is the
winner of the Password Hashing Competition and the OWASP-recommended
default for new password storage as of 2026. We do not keep a bcrypt
fallback: every PROTEA deployment runs the same Python interpreter
with the same poetry lockfile, so there is no portability case for
two algorithms. A future migration to a stronger KDF would land as a
proper migration of every hash on first successful login (the
:class:`PasswordHasher` API exposes ``check_needs_rehash`` for that).

Parameters
----------

We rely on the upstream :class:`argon2.PasswordHasher` defaults rather
than pinning ``time_cost`` / ``memory_cost`` / ``parallelism`` here.
The defaults are tracked by argon2-cffi releases against OWASP
guidance, and pinning them in PROTEA would freeze a security number
that should follow industry consensus. The library exports
``DEFAULT_TIME_COST``, ``DEFAULT_MEMORY_COST``, etc. for inspection if
an operator wants to audit the live values.

Output shape
------------

:func:`hash_password` returns the standard PHC string format
(``$argon2id$v=19$m=...,t=...,p=...$<salt>$<hash>``) which encodes
the algorithm, parameters, salt and digest in a single value. The DB
column type is :class:`sqlalchemy.Text`; no separate salt column is
needed because the salt rides inside the encoded hash.

Constant-time guarantees
------------------------

:meth:`argon2.PasswordHasher.verify` raises :class:`VerifyMismatchError`
on mismatch and runs in time independent of *where* the mismatch
occurred, so the wrapper does not have to add its own constant-time
comparison. The wrapper catches the mismatch / invalid-hash exceptions
and converts them to a plain ``False`` return so callers can treat the
result as a boolean gate without an ``except`` clause of their own.
"""

from __future__ import annotations

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

# A single module-level hasher is fine: ``PasswordHasher`` is stateless
# beyond its tuning parameters, thread-safe, and re-instantiating it on
# every call would just throw away a tiny amount of constant setup.
_HASHER = PasswordHasher()


def hash_password(plaintext: str) -> str:
    """Hash ``plaintext`` with Argon2id and return the PHC string.

    The returned string is opaque to callers; pass it back to
    :func:`verify_password` to check a candidate. Two calls with the
    same input produce different outputs because the salt is generated
    fresh per call (``os.urandom`` under the hood).

    Raises
    ------
    TypeError
        If ``plaintext`` is not a string. Empty strings are accepted by
        the underlying hasher; rejection of empty / weak passwords is
        a policy decision that belongs higher up the call stack
        (signup endpoint validators).
    """
    if not isinstance(plaintext, str):
        raise TypeError(f"plaintext must be str, got {type(plaintext).__name__}")
    return _HASHER.hash(plaintext)


def verify_password(plaintext: str, stored_hash: str) -> bool:
    """Return ``True`` iff ``plaintext`` matches ``stored_hash``.

    Wraps :meth:`PasswordHasher.verify` to translate the argon2-cffi
    exception model into a plain boolean. Both a mismatch and a
    malformed ``stored_hash`` collapse to ``False`` so a corrupted DB
    row cannot escalate into a 500 response on a login attempt; the
    caller still needs to render a generic ``"invalid credentials"``
    message either way.

    Raises
    ------
    TypeError
        If either argument is not a string. The underlying library
        would raise its own ``TypeError`` eventually, but raising here
        keeps the failure inside this module's contract.
    """
    if not isinstance(plaintext, str):
        raise TypeError(f"plaintext must be str, got {type(plaintext).__name__}")
    if not isinstance(stored_hash, str):
        raise TypeError(f"stored_hash must be str, got {type(stored_hash).__name__}")
    try:
        return _HASHER.verify(stored_hash, plaintext)
    except (VerifyMismatchError, InvalidHashError):
        return False


__all__ = ["hash_password", "verify_password"]
