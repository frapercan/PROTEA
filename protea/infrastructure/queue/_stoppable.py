"""The deliberate-stop flag, shared by both consumers."""

from __future__ import annotations


class Stoppable:
    """Carries the stop flag a consumer's signal handler sets, and exposes it
    to whoever supervises that consumer.

    The supervisor needs it because ``run`` RETURNS on a deliberate stop
    rather than raising. With no way to tell "finished because it was told
    to" from "the connection dropped", a reconnect loop restarts the consumer
    and the process outlives its own SIGTERM, still holding the code it was
    started with. That is not a slow shutdown, it is a worker that cannot be
    restarted by any signal short of SIGKILL.
    """

    _stop: bool

    @property
    def stopped(self) -> bool:
        return self._stop
