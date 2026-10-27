#!/usr/bin/env python3
"""Write the tokenisation reference that ``tests/golden/tokeniser_ids.json`` holds.

Run by hand, never imported. It is the reference, so rewriting it to make a
failing test pass is the one thing that empties the test of meaning: do it only
when a tokenisation change is intended and understood, and say so in the commit.

    poetry run python scripts/capture_tokeniser_golden.py

Refuses to write a file with holes. A reference missing entries turns the
verification into a rubber stamp for exactly the configs that could not be
loaded, which are the ones most likely to have broken.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import transformers  # noqa: E402

from tests.helpers.tokenisation import CONFIGS, SEQUENCE, tokenise  # noqa: E402

DEST = REPO / "tests" / "golden" / "tokeniser_ids.json"


def main() -> int:
    out: dict = {
        "_sequence": SEQUENCE,
        "_n_residues": len(SEQUENCE),
        "_captured_with": {"transformers": transformers.__version__},
        "configs": {},
    }
    failed: list[str] = []

    for key, backend, model_name, tokeniser_repo, mode in CONFIGS:
        try:
            klass, ids = tokenise(tokeniser_repo, mode)
        except Exception as exc:  # noqa: BLE001 - reported, not swallowed
            out["configs"][key] = {"backend": backend, "model_name": model_name,
                                   "ERROR": f"{type(exc).__name__}: {exc}"}
            failed.append(key)
            print(f"  ERR {key:11s} {backend:7s} {type(exc).__name__}: {str(exc)[:70]}")
            continue
        out["configs"][key] = {
            "backend": backend,
            "model_name": model_name,
            "tokeniser_repo": tokeniser_repo,
            "mode": mode,
            "class": klass,
            "n_tokens": len(ids),
            "sha256_ids": hashlib.sha256(json.dumps(ids).encode()).hexdigest(),
            "ids": ids,
        }
        print(f"  ok  {key:11s} {backend:7s} {klass:22s} -> {len(ids):4d} tokens")

    if failed:
        print(
            f"\n  NOT writing the reference: {len(failed)} config(s) could not be "
            f"captured ({', '.join(failed)})."
        )
        return 1

    DEST.parent.mkdir(parents=True, exist_ok=True)
    with DEST.open("w") as fh:
        json.dump(out, fh, indent=1)
        fh.write("\n")
    print(f"\n  reference written to {DEST.relative_to(REPO)} "
          f"with transformers {transformers.__version__}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
