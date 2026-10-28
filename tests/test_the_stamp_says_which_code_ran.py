"""The per-operation stamp has to name the code, not only the library builds.

``stamp_library_provenance`` emitted the library versions alone, on the stated
grounds that "the revision guard records WHICH CODE". The guard does -- to the
journal, when a worker STARTS. Nothing of it reaches the job. So a worker
replaced between two batches of one job writes rows under the same job id, with
the same library versions, from different code, and the record cannot tell them
apart.

Measured on this deployment 2026-10-28: the compute node syncs to the declared
revision on a 3-minute timer whose in-flight guard only defers while a batch is
mid-operation. Between batches the gap is 0,16 s of a 3,2 s cycle, so a tick
lands in it with probability around 5% and therefore within about an hour.
"""

from __future__ import annotations

from protea.core.code_revision import code_revision
from protea.core.provenance import _LIBRARIES, stamp_library_provenance


def _spy():
    seen: list[tuple] = []
    return seen, lambda kind, _m, fields, level: seen.append((kind, fields, level))


class TestWhatItEmits:
    def test_the_event_carries_the_code_revision(self):
        seen, emit = _spy()
        stamp_library_provenance(emit)
        (kind, fields, level), = seen
        assert kind == "provenance.libraries"
        assert level == "info"
        assert fields["code_revision"] == code_revision()

    def test_it_still_carries_every_library(self):
        """An absent key reads later as a field nobody captured, which is the
        opposite of what a provenance record is for."""
        seen, emit = _spy()
        stamp_library_provenance(emit)
        fields = seen[0][1]
        for name in _LIBRARIES:
            assert name in fields

    def test_the_revision_is_a_sha_or_a_stated_non_identity(self):
        """``code_revision`` returns a bare sha only when that sha identifies
        the running code; anything else is a string that says why not, and
        either is better than the silence this replaces."""
        seen, emit = _spy()
        stamp_library_provenance(emit)
        value = seen[0][1]["code_revision"]
        assert isinstance(value, str) and value


class TestWhatItNoLongerCosts:
    def test_no_git_subprocess_per_stamp(self):
        """``capture_provenance`` shells out to ``git rev-parse`` and the stamp
        used to call it just to take ``["libraries"]``, so the subprocess was
        paid once per batch and thrown away: 10.689 times on job cbaba8bb
        alone. The revision now comes from the import-time constant instead."""
        import subprocess

        calls = []
        original = subprocess.check_output

        def spy(*args, **kwargs):
            calls.append(args[0] if args else kwargs.get("args"))
            return original(*args, **kwargs)

        subprocess.check_output = spy
        try:
            _seen, emit = _spy()
            stamp_library_provenance(emit)
        finally:
            subprocess.check_output = original
        assert calls == [], f"the stamp shelled out: {calls}"

    def test_it_does_not_go_through_capture_provenance(self):
        """Read from the source, because calling it again would be correct and
        would quietly restore the subprocess."""
        import inspect

        from protea.core import provenance as mod

        src = inspect.getsource(mod.stamp_library_provenance)
        assert "capture_provenance(" not in src
        assert "_library_versions()" in src


class TestTheRecordCanNowTellThemApart:
    def test_two_revisions_are_distinguishable_in_one_job(self):
        """The shape of the defect: same job, same libraries, different code.
        Before this change both events were byte-identical."""
        import protea.core.provenance as mod

        seen, emit = _spy()
        original = mod.code_revision
        try:
            mod.code_revision = lambda: "a" * 40
            stamp_library_provenance(emit)
            mod.code_revision = lambda: "b" * 40
            stamp_library_provenance(emit)
        finally:
            mod.code_revision = original
        first, second = seen[0][1], seen[1][1]
        assert first["code_revision"] != second["code_revision"]
        # and nothing else moved, which is exactly why the revision was needed
        assert {k: v for k, v in first.items() if k != "code_revision"} == {
            k: v for k, v in second.items() if k != "code_revision"
        }
