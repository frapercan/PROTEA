"""Shout when a queue holds work that nothing is doing.

WHY THIS EXISTS. Twice in two days the campaign stopped dead and nothing said
so. On 2026-09-05 the broker was OOM-killed and eleven workers went on
reporting ``active`` for four hours while every queue sat empty behind them. On
2026-09-06 a pathological query plan blocked the batch consumers inside a
five-minute COUNT, their AMQP heartbeats stopped, the broker dropped them, and
216 messages waited three hours and twenty minutes for consumers that were no
longer there -- with nine jobs still marked RUNNING. Both times the researcher
noticed before any instrument did, by looking at the screen and saying the jobs
did not seem fluid.

Progress monitors would not have caught either: they watch things go up, and
the failure is that nothing moves. What both had in common is a queue holding
messages that nobody was draining, so that is what this watches.

THREE STATES, AND ONLY TWO ARE FAULTS.

    stalled     messages > 0 and consumers == 0
                Nobody is attached. Work is addressed to nothing.

    starved     messages > 0, consumers > 0, and no acknowledgements for
                longer than the grace period.
                Somebody is attached and nothing is moving through them --
                a consumer blocked inside an operation, which is what
                happened on the 6th.

    idle        messages == 0.
                Not a fault however long it lasts, and saying otherwise
                would train everyone to ignore this.

A queue with a slow consumer is not starved: one acknowledgement inside the
window clears it. The grace period therefore has to exceed the longest single
operation the queue serves, or a heavy batch reads as a stall. It defaults to
fifteen minutes and is worth raising rather than lowering.

Exit codes are the whole interface: 0 healthy, 1 faults found, 2 the broker
itself could not be reached -- which is its own fault and the one that hid the
OOM. Run it from a timer and let the failure of the unit be the alarm.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from base64 import b64encode

#: Where the "holding since" clock lives between runs. A fixed global path
#: would be shared by every invocation, so two checks with different scopes --
#: or two tests -- would overwrite each other's clock and mask a real stall.
DEFAULT_STATE = "/tmp/protea-queue-heartbeat.json"


def _fetch(api: str, user: str, password: str) -> list[dict]:
    req = urllib.request.Request(f"{api}/api/queues")
    req.add_header("Authorization", "Basic " + b64encode(f"{user}:{password}".encode()).decode())
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


def _load_seen(path: str) -> dict[str, float]:
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--api", default=os.environ.get("PROTEA_RABBIT_API", "http://localhost:15672"))
    ap.add_argument("--user", default=os.environ.get("PROTEA_RABBIT_USER", "guest"))
    ap.add_argument("--password", default=os.environ.get("PROTEA_RABBIT_PASS", "guest"))
    ap.add_argument("--prefix", default="protea.", help="only watch queues named like this")
    ap.add_argument(
        "--grace-seconds",
        type=int,
        default=900,
        help="how long a queue may hold messages without an ack before it counts as starved",
    )
    ap.add_argument(
        "--state",
        default=os.environ.get("PROTEA_HEARTBEAT_STATE", DEFAULT_STATE),
        help="file holding the per-queue 'holding since' clock between runs",
    )
    ap.add_argument("--now", type=float, default=None, help="clock override, for tests")
    a = ap.parse_args()

    now = a.now if a.now is not None else __import__("time").time()

    try:
        queues = _fetch(a.api, a.user, a.password)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        # The broker being unreachable is the failure that hid the OOM for four
        # hours, so it is reported as a fault rather than as "nothing to see".
        print(f"CRITICO: no se puede consultar el broker en {a.api}: {exc}", file=sys.stderr)
        return 2

    seen, faults, healthy = _load_seen(a.state), [], []
    fresh: dict[str, float] = {}

    for q in queues:
        name = q.get("name", "")
        if not name.startswith(a.prefix):
            continue
        msgs = q.get("messages", 0) or 0
        cons = q.get("consumers", 0) or 0
        acks = (q.get("message_stats", {}).get("ack_details", {}) or {}).get("rate", 0.0)

        if msgs == 0:
            continue  # idle is not a fault, however long it lasts
        if acks and acks > 0:
            healthy.append(f"{name}: {msgs} en cola, {cons} consumidor(es), {acks:.2f} ack/s")
            continue

        # Held work with nothing moving. Remember since when, so a single
        # quiet sample does not raise an alarm a busy batch would clear.
        since = seen.get(name, now)
        fresh[name] = since
        held = now - since
        if cons == 0:
            faults.append(
                f"ATASCADA {name}: {msgs} mensajes, CERO consumidores ({held / 60:.0f} min)"
            )
        elif held >= a.grace_seconds:
            faults.append(
                f"HAMBRIENTA {name}: {msgs} mensajes, {cons} consumidor(es), "
                f"sin acks desde hace {held / 60:.0f} min"
            )

    try:
        with open(a.state, "w") as fh:
            json.dump(fresh, fh)
    except OSError:
        pass  # losing the state costs one cycle of memory, not the check

    for line in healthy:
        print(f"ok        {line}")
    for line in faults:
        print(line, file=sys.stderr)
    if not faults:
        print("ok        ninguna cola retiene trabajo sin consumirlo")
    return 1 if faults else 0


if __name__ == "__main__":
    sys.exit(main())
