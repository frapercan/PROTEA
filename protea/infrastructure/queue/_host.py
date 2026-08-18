"""Which machine computed a batch, recorded on the event rather than inferred.

The compute is distributed: a coordinator partitions a run into batches, the
batches go to a shared queue, and whichever worker is free takes one. That is the
arrangement the thesis describes, and until now nothing written down said which
machine did which piece. Every field on a batch event described the WORK
(queries, references, rows, elapsed_seconds) and none described the WORKER.

So a run that in fact executed across two machines was indistinguishable from one
that executed on either, and a result that differed between them would have had no
attributable cause. That is a provenance gap in the claim itself, not only an
inconvenience for debugging.

The name is configurable because a hostname is an accident of installation.
``xaxi-PC`` says nothing to a reader in two years; ``desktop-gpu`` says which role
in the topology produced the row. Where nothing is configured the hostname is
still better than silence.
"""

from __future__ import annotations

import os
import socket
from functools import lru_cache


@lru_cache(maxsize=1)
def compute_host() -> str:
    """This machine's name for the event log.

    Cached because it cannot change within a process and this runs on every
    emitted event, of which a single batch produces many.

    ``PROTEA_COMPUTE_HOST`` wins so a machine can declare its role in the
    topology rather than its installation name. An empty or whitespace-only
    setting is treated as unset, since an env var exported empty by a unit file
    would otherwise stamp every row with nothing and look like a bug in this
    function.
    """
    declared = os.environ.get("PROTEA_COMPUTE_HOST", "").strip()
    if declared:
        return declared
    try:
        return socket.gethostname() or "unknown"
    except OSError:
        # Recording an event must never fail because the host could not be named.
        return "unknown"
