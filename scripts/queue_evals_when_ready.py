#!/usr/bin/env python3
"""Wait for predict jobs to finish, then queue CAFA evaluations.

Usage:
    python scripts/queue_evals_when_ready.py
"""
import time

import requests

API = "http://localhost:8000"
EVAL_SET = "42b34e79-6fe9-4fa0-b718-02f43a1e3192"

# (predict_job_id, prediction_set_id, k)
PENDING = [
    ("20b56bb7-8f3c-4278-89e7-715a1792c3c4", "a4442444-a7c7-4568-8432-eb1efecf1e24", 20),
    ("5a21422b-c4ae-4bde-979b-e5a357c8cb80", "d41b8d05-e591-4153-85bb-04d22413d1e7", 50),
]

POLL = 30  # seconds


def main():
    remaining = list(PENDING)
    while remaining:
        still_waiting = []
        for job_id, ps_id, k in remaining:
            r = requests.get(f"{API}/jobs/{job_id}", timeout=10)
            status = r.json()["status"]
            progress = r.json().get("progress_current", "?")
            total = r.json().get("progress_total", "?")
            print(f"  k={k}  job={job_id[:8]}  status={status}  {progress}/{total}")

            if status == "succeeded":
                # Queue CAFA eval
                resp = requests.post(
                    f"{API}/annotations/evaluation-sets/{EVAL_SET}/run",
                    json={"prediction_set_id": ps_id},
                    timeout=10,
                )
                resp.raise_for_status()
                eval_id = resp.json()["id"]
                print(f"  → Queued CAFA eval for k={k}: {eval_id}")
            elif status in ("failed", "cancelled"):
                print(f"  → SKIP k={k}: job {status}")
            else:
                still_waiting.append((job_id, ps_id, k))

        remaining = still_waiting
        if remaining:
            print(f"\n  Waiting {POLL}s for {len(remaining)} job(s)...\n")
            time.sleep(POLL)

    print("\nDone — all CAFA evals queued.")


if __name__ == "__main__":
    main()
