"""The three states a queue can be in, and which two are faults.

Both stalls this campaign suffered were invisible to every progress monitor,
because a progress monitor watches things go up and the failure is that nothing
moves. These pin the distinction the heartbeat draws instead: held work with
nobody draining it is a fault, an empty queue never is, and a slow consumer is
not a stalled one.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "queue_heartbeat.py"


def _run(queues: list[dict], tmp_path: Path, *, now: float = 10_000.0, grace: int = 900):
    """Run the checker against a canned broker reply."""
    body = json.dumps(queues)
    stub = tmp_path / "stub.py"
    stub.write_text(
        "import json,sys\n"
        f"BODY={body!r}\n"
        "class _R:\n"
        "    def __enter__(self): return self\n"
        "    def __exit__(self,*a): return False\n"
        "    def read(self): return BODY.encode()\n"
        "import urllib.request\n"
        "urllib.request.urlopen = lambda *a, **k: _R()\n"
        f"sys.argv = ['hb','--now','{now}','--grace-seconds','{grace}',"
        f"'--state',{str(tmp_path / 'hb.json')!r}]\n"
        f"sys.path.insert(0, {str(SCRIPT.parent)!r})\n"
        "import runpy\n"
        f"runpy.run_path({str(SCRIPT)!r}, run_name='__main__')\n"
    )
    env = {"PATH": "/usr/bin:/bin", "TMPDIR": str(tmp_path), "HOME": str(tmp_path)}
    return subprocess.run(
        [sys.executable, str(stub)], capture_output=True, text=True, env=env, timeout=60
    )


def _q(name: str, msgs: int, consumers: int, ack_rate: float = 0.0) -> dict:
    return {
        "name": name,
        "messages": msgs,
        "consumers": consumers,
        "message_stats": {"ack_details": {"rate": ack_rate}},
    }


class TestWhatCountsAsAFault:
    def test_messages_with_no_consumer_is_a_fault(self, tmp_path):
        """The 2026-09-05 shape: work addressed to nothing."""
        r = _run([_q("protea.predictions.batch", 216, 0)], tmp_path)
        assert r.returncode == 1
        assert "ATASCADA" in r.stderr
        assert "CERO consumidores" in r.stderr

    def test_an_empty_queue_is_never_a_fault(self, tmp_path):
        """Idle is the normal state between runs. Alarming on it would train
        everyone to ignore the alarm."""
        r = _run([_q("protea.predictions.batch", 0, 0)], tmp_path)
        assert r.returncode == 0

    def test_a_moving_queue_is_not_a_fault_however_deep(self, tmp_path):
        r = _run([_q("protea.predictions.write", 2027, 1, ack_rate=0.6)], tmp_path)
        assert r.returncode == 0
        assert "0.60 ack/s" in r.stdout


class TestStarvation:
    def test_a_first_quiet_sample_does_not_alarm(self, tmp_path):
        """One silent reading is a busy batch, not a stall. The grace period
        has to exceed the longest single operation the queue serves."""
        r = _run([_q("protea.predictions.batch", 216, 2)], tmp_path)
        assert r.returncode == 0

    def test_quiet_past_the_grace_period_does_alarm(self, tmp_path):
        """The 2026-09-06 shape: consumers attached, nothing moving through
        them, because they are blocked inside an operation."""
        first = _run([_q("protea.predictions.batch", 216, 2)], tmp_path, now=1000.0)
        assert first.returncode == 0
        later = _run([_q("protea.predictions.batch", 216, 2)], tmp_path, now=1000.0 + 1200)
        assert later.returncode == 1
        assert "HAMBRIENTA" in later.stderr
        assert "20 min" in later.stderr

    def test_an_ack_clears_the_clock(self, tmp_path):
        _run([_q("protea.predictions.batch", 216, 2)], tmp_path, now=1000.0)
        moving = _run([_q("protea.predictions.batch", 216, 2, 0.4)], tmp_path, now=1000.0 + 1200)
        assert moving.returncode == 0


class TestTheBrokerItself:
    def test_an_unreachable_broker_is_its_own_fault(self, tmp_path):
        """This is what hid the OOM for four hours: the thing that would have
        reported the problem was the thing that was gone."""
        r = subprocess.run(
            [sys.executable, str(SCRIPT), "--api", "http://127.0.0.1:1"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert r.returncode == 2
        assert "CRITICO" in r.stderr


class TestScope:
    def test_queues_outside_the_prefix_are_ignored(self, tmp_path):
        r = _run([_q("otracosa", 500, 0)], tmp_path)
        assert r.returncode == 0


@pytest.mark.parametrize("state", ["ATASCADA", "HAMBRIENTA"])
def test_faults_go_to_stderr_so_a_timer_can_fail_on_them(state, tmp_path):
    """The exit code and stderr are the whole interface: a systemd timer unit
    failing IS the alarm, with no extra delivery path to maintain."""
    now = 1000.0
    if state == "ATASCADA":
        r = _run([_q("protea.x", 5, 0)], tmp_path, now=now)
    else:
        _run([_q("protea.x", 5, 1)], tmp_path, now=now)
        r = _run([_q("protea.x", 5, 1)], tmp_path, now=now + 1200)
    assert r.returncode == 1
    assert state in r.stderr and state not in r.stdout
