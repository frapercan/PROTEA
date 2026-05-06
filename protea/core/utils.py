from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Sequence as Seq
from datetime import UTC, datetime
from typing import Any


def utcnow() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(UTC)


def chunks(seq: Seq[Any], n: int) -> Iterable[Seq[Any]]:
    """Yield successive n-sized chunks from seq."""
    for i in range(0, len(seq), n):
        yield seq[i : i + n]
