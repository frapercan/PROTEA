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
        r = _run([_q("protea.predictions", 216, 2)], tmp_path)
        assert r.returncode == 0

    def test_quiet_past_the_grace_period_does_alarm(self, tmp_path):
        """The 2026-09-06 shape: consumers attached, nothing moving through
        them, because they are blocked inside an operation.

        Uses the coordinator queue rather than the batch one: batch queues were
        later given a grace of their own, since a reference load precedes every
        score and two machines take turns on them.
        """
        first = _run([_q("protea.predictions", 216, 2)], tmp_path, now=1000.0)
        assert first.returncode == 0
        later = _run([_q("protea.predictions", 216, 2)], tmp_path, now=1000.0 + 2400)
        assert later.returncode == 1
        assert "HAMBRIENTA" in later.stderr
        assert "40 min" in later.stderr

    def test_an_ack_clears_the_clock(self, tmp_path):
        _run([_q("protea.predictions", 216, 2)], tmp_path, now=1000.0)
        moving = _run([_q("protea.predictions", 216, 2, 0.4)], tmp_path, now=1000.0 + 2400)
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


class TestTheGraceIsPerQueue:
    def test_a_batch_queue_gets_a_longer_rope_than_the_default(self, tmp_path):
        """It cried wolf on a healthy queue within a day of being written.
        predict_go_terms_batch loads a reference pool before it scores, and two
        machines share the queue, so this consumer can legitimately go forty
        minutes without acking while the other one works. Sixteen batches
        completed in the ten minutes after the alarm fired."""
        _run([_q("protea.predictions.batch", 213, 2)], tmp_path, now=1000.0)
        r = _run([_q("protea.predictions.batch", 213, 2)], tmp_path, now=1000.0 + 2400)
        assert r.returncode == 0, "40 min on a batch queue is work, not a stall"

    def test_the_same_wait_on_an_ordinary_queue_does_alarm(self, tmp_path):
        """The longer rope is given by name, so it cannot quietly cover a
        queue whose operations are short."""
        _run([_q("protea.predictions", 213, 2)], tmp_path, now=1000.0)
        r = _run([_q("protea.predictions", 213, 2)], tmp_path, now=1000.0 + 2400)
        assert r.returncode == 1
        assert "HAMBRIENTA" in r.stderr

    def test_a_batch_queue_still_alarms_once_past_its_own_grace(self, tmp_path):
        """Generous is not infinite: a real stall trips any threshold in the end."""
        _run([_q("protea.predictions.batch", 213, 2)], tmp_path, now=1000.0)
        r = _run([_q("protea.predictions.batch", 213, 2)], tmp_path, now=1000.0 + 6000)
        assert r.returncode == 1
        assert "margen 90 min" in r.stderr

    def test_zero_consumers_needs_no_grace_at_all(self, tmp_path):
        """Nobody attached is not slowness, so the longer rope does not apply."""
        r = _run([_q("protea.predictions.batch", 213, 0)], tmp_path)
        assert r.returncode == 1
        assert "ATASCADA" in r.stderr


class TestQueuesWithNoConsumerByDesign:
    def test_the_dead_letter_queue_is_not_a_stall(self, tmp_path):
        """It has no consumer on purpose, so "messages and nobody attached" is
        its normal state. Alarming on it would fire forever, which is exactly
        how an alarm gets ignored -- the failure this whole check exists to
        avoid, reintroduced by the check itself."""
        r = _run([_q("protea.dead-letter", 5203, 0)], tmp_path)
        assert r.returncode == 0
        assert "sin consumidor por diseño" in r.stdout
        assert "ATASCADA" not in r.stderr

    def test_its_depth_is_still_reported(self, tmp_path):
        """Not a fault is not the same as not worth saying: five thousand
        dead letters is a fact somebody should see."""
        r = _run([_q("protea.dead-letter", 5203, 0)], tmp_path)
        assert "5203" in r.stdout

    def test_an_ordinary_queue_with_the_same_shape_still_alarms(self, tmp_path):
        """The exemption is by name, not by shape, so it cannot silently
        swallow a real stall on a working queue."""
        r = _run([_q("protea.predictions.batch", 5203, 0)], tmp_path)
        assert r.returncode == 1
        assert "ATASCADA" in r.stderr


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
