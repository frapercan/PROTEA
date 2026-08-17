"""The entrypoint's own tests, which run the shell script.

The sibling module tests the driver's Python and never executes a line of
``lafa_entrypoint.sh``. That gap shipped a container that aborted on its own
first line: a merge placed a writability check above the branch that assigns
the directory it reads, and under ``set -u`` every invocation died with
``OUT_DIR: parameter not set`` before any work began. Both the guide's calling
style and the bind-mount layout were affected, ruff and mypy were clean, and
all twenty-four Python tests passed.

So these run the real script with a stub interpreter on ``PATH`` in place of
``python``. Nothing here needs torch, the backbone or the bank; the stub
records the arguments the script would have handed the driver and exits, which
is exactly the contract the entrypoint is responsible for.

One thing the harness cannot fabricate: a bind mount. The entrypoint refuses an
output directory that is not one, because an unmounted directory inside the
image swallows the results, and pytest's ``tmp_path`` is never a mount. Tests
that need a run to succeed write to ``/dev/shm``, which is a real tmpfs on every
Linux this container targets; tests of the refusal itself use ``tmp_path``,
where the condition holds for free.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

ENTRYPOINT = Path(__file__).resolve().parents[1] / "apps" / "lafa_sparse_knn" / "lafa_entrypoint.sh"


@dataclass(frozen=True)
class Layout:
    """A mount layout the script accepts, plus a stub for the driver."""

    tmp: Path
    bundle: Path
    inp: Path
    out: Path
    mounted_out: Path
    hf: Path
    environ: dict[str, str]

    def run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["sh", str(ENTRYPOINT), *args],
            env=self.environ,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def handoff(self) -> list[str]:
        """The argv the stub interpreter received, empty if it never ran."""
        path = self.tmp / "handoff.txt"
        return path.read_text().splitlines() if path.exists() else []

    def out_arg(self) -> str:
        """An output path whose directory is NOT a mount, so exit 69 applies."""
        return str(self.out / "pred.tsv")

    def mounted_arg(self) -> str:
        """An output path whose directory IS a mount, so a run can succeed."""
        return str(self.mounted_out / "pred.tsv")


@pytest.fixture()
def layout(tmp_path: Path) -> Layout:
    """Every path is real, so a test that wants a particular failure removes
    or chmods one of them rather than mocking."""
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "BANK.json").write_text("{}")

    inp = tmp_path / "input"
    inp.mkdir()
    (inp / "queries.fasta").write_text(">P1\nMKT\n")
    (inp / "go-basic.obo").write_text("[Term]\nid: GO:0008150\n")

    out = tmp_path / "output"
    out.mkdir()

    hf = tmp_path / "hf-cache"
    (hf / "hub").mkdir(parents=True)

    # A real mount point. The entrypoint refuses an output directory that is not
    # one, because an unmounted directory inside the image swallows the results,
    # and pytest's tmp_path is never a mount. /dev/shm is a tmpfs listed in
    # /proc/mounts on every Linux this container targets, and writing a few
    # bytes there is harmless.
    mounted_out = Path("/dev/shm")

    # The stub interpreter. It answers to `python -m <module> <args...>` and
    # writes the arguments where a test can read them.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "python"
    stub.write_text(f'#!/bin/sh\nprintf "%s\\n" "$@" > "{tmp_path / "handoff.txt"}"\nexit 0\n')
    stub.chmod(0o755)

    return Layout(
        tmp=tmp_path,
        bundle=bundle,
        inp=inp,
        out=out,
        mounted_out=mounted_out,
        hf=hf,
        environ={
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "PROTEA_SPARSE_BUNDLE": str(bundle),
            "PROTEA_SPARSE_QUERY": str(inp / "queries.fasta"),
            "PROTEA_SPARSE_OBO": str(inp / "go-basic.obo"),
            "PROTEA_SPARSE_OUTPUT": str(out / "predictions.tsv"),
            "HF_HOME": str(hf),
        },
    )


# --- the two calling styles, which is the whole point ------------------


def test_the_guide_style_reaches_the_driver(layout: Layout) -> None:
    """Paths as arguments, which is what the container guide asks for."""
    result = layout.run(
        "--query_file",
        str(layout.inp / "queries.fasta"),
        "--graph",
        str(layout.inp / "go-basic.obo"),
        "--output_file",
        layout.mounted_arg(),
    )
    assert result.returncode == 0, result.stderr
    assert "--output_file" in layout.handoff()


def test_the_bind_mount_style_gets_as_far_as_the_bind_mount_check(layout: Layout) -> None:
    """No arguments at all, the layout this container shipped with.

    It cannot reach the driver here, and that is the script working: the
    temporary output directory is not a bind mount, so exit 69 fires by design.
    What this pins is that the run travels the bundle, query and ontology
    checks to get there, which is everything the default injection owns.
    """
    result = layout.run()
    assert result.returncode == 69, (result.returncode, result.stderr)
    assert "not a bind mount" in result.stderr


def test_neither_style_leaves_a_variable_unset(layout: Layout) -> None:
    """The regression itself: ``set -u`` must not abort either style.

    Asserted on stderr rather than on the exit code alone, because an
    unset-variable abort and a legitimate refusal are both non-zero, and only
    the message tells them apart.
    """
    for args in ([], ["--output_file", layout.mounted_arg()]):
        result = layout.run(*args)
        assert "parameter not set" not in result.stderr, result.stderr
        assert "unbound variable" not in result.stderr, result.stderr


def test_the_output_path_is_read_from_an_equals_form_too(layout: Layout) -> None:
    """``--output=path`` is one argv entry, and argparse accepts it."""
    result = layout.run(f"--output={layout.mounted_arg()}")
    assert result.returncode == 0, result.stderr


# --- the failure the writability check exists to prevent ---------------


def test_an_unwritable_output_is_refused_before_any_work(layout: Layout) -> None:
    """Exit 68 up front, not a PermissionError after the whole computation.

    Reachable on an unmounted directory because writability is checked before
    the mount test. That order is deliberate: an unmounted directory inside the
    image is always writable, so checking the mount first would answer 69 for a
    permissions problem and bury the specific remedy under the general one.
    """
    layout.out.chmod(0o555)
    try:
        result = layout.run("--output_file", layout.out_arg())
        assert result.returncode == 68, (result.returncode, result.stderr)
        assert "not writable" in result.stderr
        assert layout.handoff() == [], "the driver must not have been reached"
    finally:
        layout.out.chmod(0o755)


def test_a_missing_output_directory_is_refused(layout: Layout) -> None:
    result = layout.run("--output_file", str(layout.tmp / "nope" / "pred.tsv"))
    assert result.returncode == 66, (result.returncode, result.stderr)


def test_an_unmounted_output_is_refused_in_both_styles(layout: Layout) -> None:
    """The silent-discard hole, which was open through the documented interface.

    An output directory that is not a bind mount exists and is writable inside
    the image, so the run completes, logs how many predictions it wrote, and
    exits 0 with the file discarded when the container goes. A silent success
    that produces nothing is the worst outcome this script can have.

    It used to be caught only when the path came from the defaults. The
    caller-supplied branch was exempted on the reasoning that somebody naming a
    path has chosen where it goes, and the guide's own calling style is exactly
    where that reasoning fails. Both styles are checked here, and the harness
    can assert it because no path under the temporary directory is ever a mount.
    """
    for args in ([], ["--output_file", layout.out_arg()]):
        result = layout.run(*args)
        assert result.returncode == 69, (args, result.returncode, result.stderr)
        assert "not a bind mount" in result.stderr
        assert layout.handoff() == [], "the driver must not have been reached"


# --- the remaining documented exit codes -------------------------------


def test_a_missing_bundle_is_refused(layout: Layout) -> None:
    (layout.bundle / "BANK.json").unlink()
    assert layout.run().returncode == 64


def test_missing_queries_are_refused(layout: Layout) -> None:
    (layout.inp / "queries.fasta").unlink()
    assert layout.run().returncode == 65


def test_a_missing_ontology_is_refused(layout: Layout) -> None:
    (layout.inp / "go-basic.obo").unlink()
    assert layout.run().returncode == 67


def test_a_missing_backbone_cache_is_refused(layout: Layout) -> None:
    """Guide style, because the cache guard sits after the output block."""
    (layout.hf / "hub").rmdir()
    result = layout.run("--output_file", layout.mounted_arg())
    assert result.returncode == 70, (result.returncode, result.stderr)


def test_help_answers_with_nothing_mounted(layout: Layout) -> None:
    """Reading the usage must not require a single mount to exist."""
    (layout.bundle / "BANK.json").unlink()
    (layout.inp / "queries.fasta").unlink()
    (layout.hf / "hub").rmdir()
    result = layout.run("--help")
    assert result.returncode == 0, result.stderr
    assert layout.handoff()[-1] == "--help"
    assert "apps.lafa_sparse_knn.sparse_driver" in layout.handoff()
