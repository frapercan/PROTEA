"""The cluster scripts of ADR-D59: what they ship and what they refuse.

``deploy/cluster/send_builder_code.sh`` ships ``builder_files.txt`` and nothing else,
so the list must be exactly the import closure of the measure and build tools inside
``protea/``, and that closure must need nothing a compute node lacks (the standard
library and pyarrow). The download and job scripts refuse a cut after the mark and a
UniProt release at or after the ceiling before they touch a byte; those refusals are
run here, offline.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CLUSTER = ROOT / "deploy" / "cluster"
ENTRY_POINTS = ("protea.core.cut_photo.build", "protea.core.cut_photo.measure")
#: What a compute node is allowed to need beyond the standard library.
THIRD_PARTY = frozenset({"pyarrow"})


def _path_of(module: str) -> Path | None:
    for candidate in (
        ROOT / (module.replace(".", "/") + ".py"),
        ROOT / module.replace(".", "/") / "__init__.py",
    ):
        if candidate.is_file():
            return candidate
    return None


def _closure() -> tuple[set[str], set[str]]:
    """The files under ``protea/`` the entry points import, and every outside module."""
    todo, seen, outside = list(ENTRY_POINTS), set(), set()
    while todo:
        module = todo.pop()
        path = _path_of(module)
        if path is None or module in seen:
            continue
        seen.add(module)
        parts = module.split(".")
        todo.extend(".".join(parts[:i]) for i in range(1, len(parts)))
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                if node.module.startswith("protea"):
                    todo.append(node.module)
                    todo.extend(f"{node.module}.{alias.name}" for alias in node.names)
                else:
                    outside.add(node.module.split(".")[0])
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("protea"):
                        todo.append(alias.name)
                    else:
                        outside.add(alias.name.split(".")[0])
    files = {str(p.relative_to(ROOT)) for m in seen if (p := _path_of(m)) is not None}
    return files, outside


def _listed() -> set[str]:
    lines = (CLUSTER / "builder_files.txt").read_text(encoding="utf-8").splitlines()
    return {line.strip() for line in lines if line.strip() and not line.startswith("#")}


def test_the_shipped_list_is_the_import_closure() -> None:
    files, _ = _closure()
    assert _listed() == files


def test_the_closure_needs_only_the_standard_library_and_pyarrow() -> None:
    _, outside = _closure()
    unexpected = outside - set(sys.stdlib_module_names) - THIRD_PARTY - {"__future__"}
    assert not unexpected, f"a compute node would need {sorted(unexpected)}"


def _run(script: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(CLUSTER / script), *args],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://ftp.ebi.ac.uk/pub/databases/GO/goa/old/UNIPROT/goa_uniprot_all.gaf.228.gz",
        "https://ftp.uniprot.org/pub/databases/uniprot/previous_releases/release-2025_04/"
        "knowledgebase/uniprot_sprot-only2025_04.tar.gz",
    ],
)
def test_the_download_refuses_a_source_past_the_mark(url: str, tmp_path: Path) -> None:
    done = _run("fetch_source.sh", url, str(tmp_path / "file"), "10", "1")
    assert done.returncode == 2, done.stdout + done.stderr
    assert "REFUSED" in done.stdout
    assert not any(tmp_path.iterdir())


def test_the_build_job_refuses_a_cut_past_the_mark(tmp_path: Path) -> None:
    args = ["python", str(tmp_path), "228", "2025-11-01", "2025_03", "2025-06-18"]
    args += [str(tmp_path)] * 3 + ["rev", "url", "0" * 64, "url"]
    done = _run("cut_photo_build.sh", *args)
    assert done.returncode == 2
    assert "after the mark 227" in done.stdout


def test_the_measure_job_refuses_a_cut_past_the_mark(tmp_path: Path) -> None:
    args = ["python", str(tmp_path), "228", "2025-11-01", "2025_03", "2025-06-18"]
    args += [str(tmp_path / "sprot"), str(tmp_path / "uniref"), "0" * 32, str(tmp_path), "rev"]
    done = _run("cut_photo_measure.sh", *args)
    assert done.returncode == 2
    assert "after the mark 227" in done.stdout


def test_the_send_refuses_a_name_that_is_not_protea_code(tmp_path: Path) -> None:
    # The script reads the list beside itself, so a copy beside a list that names a
    # data file shows the refusal, which comes before git or ssh is touched.
    (tmp_path / "send_builder_code.sh").write_text(
        (CLUSTER / "send_builder_code.sh").read_text(encoding="utf-8"), encoding="utf-8"
    )
    (tmp_path / "builder_files.txt").write_text("protea/core/marks.py\ndata/photo.parquet\n")
    done = subprocess.run(
        ["bash", str(tmp_path / "send_builder_code.sh"), "HEAD", "nowhere", "/tmp/x"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert done.returncode == 2
    assert "REFUSED: data/photo.parquet is not a Python file under protea/" in done.stdout
