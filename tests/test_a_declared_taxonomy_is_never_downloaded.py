"""A declared taxonomy is read from its file and never downloaded.

ete3's ``NCBITaxa()`` downloads today's NCBI dump (about 100 MB, 2.9 million
nodes) whenever its default file under ``$HOME`` is absent or in a format it
does not read, and the workers on protea.jobs, protea.predictions.batch and
protea.training call it at startup. The taxonomy behind a feature is then
whatever NCBI published the day a worker first started, which a run pinned to
a cut cannot accept. ``PROTEA_TAXONOMY_DB_PATH`` declares the file instead: it
is loaded as it is, and a declared taxonomy that cannot be loaded is a clear
error, never a download. ``PROTEA_SKIP_TAXONOMY_WARMUP=1`` keeps the warm-up
out of startup.

Every test runs with sockets refused and ete3's downloader replaced by one
that records the call and fails, so a path that reached the network fails
here instead of downloading. The tests that open a real database need ete3,
which cannot be imported on Python 3.13 and later (its package import reaches
``import cgi``), so they skip there; CI runs 3.12.
"""

from __future__ import annotations

import logging
import socket
import sqlite3
import sys
import urllib.request
from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import protea.core.feature_engineering as fe
from protea.core.feature_engineering import TAXONOMY_DB_PATH_ENV, DeclaredTaxonomyUnavailable
from protea.infrastructure.settings import Settings

requires_ete3 = pytest.mark.skipif(
    not fe._ETE3_AVAILABLE, reason="ete3 cannot be imported on this interpreter"
)

# A five-node tree: root, cellular organisms, Eukaryota, human, mouse. ete3
# stores each lineage as the comma-separated path from the taxon to the root.
_TRACKS = {
    1: "1",
    131567: "131567,1",
    2759: "2759,131567,1",
    9606: "9606,2759,131567,1",
    10090: "10090,2759,131567,1",
}


def _ete3_db_version() -> int:
    from ete3.ncbi_taxonomy.ncbiquery import DB_VERSION

    return int(DB_VERSION)


def _write_taxonomy(path: Path, version: int | None = None) -> Path:
    """An ete3 taxonomy database in the schema ``NCBITaxa`` reads."""
    db = sqlite3.connect(path)
    db.executescript(
        """
        CREATE TABLE stats (version INT PRIMARY KEY);
        CREATE TABLE species (taxid INT PRIMARY KEY, parent INT, spname VARCHAR(50),
                              common VARCHAR(50), rank VARCHAR(50), track TEXT);
        CREATE TABLE synonym (taxid INT, spname VARCHAR(50), PRIMARY KEY (spname, taxid));
        CREATE TABLE merged (taxid_old INT, taxid_new INT);
        """
    )
    stamped = _ete3_db_version() if version is None else version
    db.execute("INSERT INTO stats (version) VALUES (?)", (stamped,))
    db.executemany(
        "INSERT INTO species (taxid, parent, spname, common, rank, track)"
        " VALUES (?, ?, ?, '', '', ?)",
        [
            (taxid, int(track.split(",")[1]) if "," in track else 1, str(taxid), track)
            for taxid, track in _TRACKS.items()
        ],
    )
    db.commit()
    db.close()
    return path


@pytest.fixture(autouse=True)
def _no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("a taxonomy test reached the network")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", _refuse)


@pytest.fixture
def downloads(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every attempt to fetch the NCBI dump, through ete3 or urllib."""
    calls: list[str] = []

    def _update_db(dbfile: str, *_args: object) -> None:
        calls.append(f"update_db({dbfile})")
        raise AssertionError("ete3 tried to download the NCBI dump")

    def _urlretrieve(url: str, *_args: object, **_kwargs: object) -> None:
        calls.append(f"urlretrieve({url})")
        raise AssertionError("urlretrieve was called")

    monkeypatch.setattr(urllib.request, "urlretrieve", _urlretrieve)
    if fe._ETE3_AVAILABLE:
        from ete3.ncbi_taxonomy import ncbiquery

        monkeypatch.setattr(ncbiquery, "update_db", _update_db)
    return calls


@pytest.fixture(autouse=True)
def _fresh_taxonomy(monkeypatch: pytest.MonkeyPatch):
    """No test sees the database another test opened."""
    monkeypatch.delenv(TAXONOMY_DB_PATH_ENV, raising=False)
    monkeypatch.setattr(fe, "_ncbi", None)
    fe._cached_lineage.cache_clear()
    yield
    fe._cached_lineage.cache_clear()
    opened = getattr(fe._ncbi, "db", None)
    if isinstance(opened, sqlite3.Connection):
        opened.close()


class TestUnsetKeepsTheDefault:
    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_ete3_default_is_opened_exactly_as_before(
        self, value: str | None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        if value is not None:
            monkeypatch.setenv(TAXONOMY_DB_PATH_ENV, value)
        default = MagicMock()
        monkeypatch.setattr(fe, "_ETE3_AVAILABLE", True)
        monkeypatch.setattr(fe, "NCBITaxa", default, raising=False)
        assert fe._get_ncbi() is default.return_value
        default.assert_called_once_with()

    def test_without_ete3_the_pair_is_still_unrelated_and_the_warmup_quiet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(fe, "_ETE3_AVAILABLE", False)
        fe.warmup_taxonomy_db()
        assert fe.compute_taxonomy(9606, 10090)["taxonomic_relation"] == "unrelated"


@requires_ete3
class TestADeclaredFileIsTheTaxonomy:
    def test_it_is_loaded_and_answers_without_a_download(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, downloads: list[str]
    ) -> None:
        path = _write_taxonomy(tmp_path / "taxa.sqlite")
        monkeypatch.setenv(TAXONOMY_DB_PATH_ENV, str(path))
        fe.warmup_taxonomy_db()
        assert fe._get_ncbi().dbfile == str(path)
        out = fe.compute_taxonomy(9606, 10090)
        assert (out["taxonomic_lca"], out["taxonomic_distance"]) == (2759, 4)
        assert out["taxonomic_relation"] == "intermediate"
        assert downloads == []

    def test_the_loaded_object_refuses_to_download_even_when_asked(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, downloads: list[str]
    ) -> None:
        monkeypatch.setenv(TAXONOMY_DB_PATH_ENV, str(_write_taxonomy(tmp_path / "taxa.sqlite")))
        with pytest.raises(DeclaredTaxonomyUnavailable, match="never downloaded"):
            fe._get_ncbi().update_taxonomy_database()
        assert downloads == []


class TestADeclaredTaxonomyThatCannotLoadIsAClearError:
    @pytest.fixture(autouse=True)
    def _ete3_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A missing file is refused before ete3 is touched, on any interpreter."""
        monkeypatch.setattr(fe, "_ETE3_AVAILABLE", True)

    def test_a_missing_file_raises_and_creates_nothing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, downloads: list[str]
    ) -> None:
        path = tmp_path / "not-yet" / "taxa.sqlite"
        monkeypatch.setenv(TAXONOMY_DB_PATH_ENV, str(path))
        with pytest.raises(DeclaredTaxonomyUnavailable) as raised:
            fe.warmup_taxonomy_db()
        assert f"{TAXONOMY_DB_PATH_ENV}={path} does not exist" in str(raised.value)
        assert downloads == []
        assert not path.parent.exists()

    def test_compute_taxonomy_lets_it_through_instead_of_nulling_the_pair(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, downloads: list[str]
    ) -> None:
        monkeypatch.setenv(TAXONOMY_DB_PATH_ENV, str(tmp_path / "absent.sqlite"))
        with pytest.raises(DeclaredTaxonomyUnavailable):
            fe.compute_taxonomy(9606, 10090)
        assert downloads == []

    @requires_ete3
    @pytest.mark.parametrize(
        "make",
        [
            lambda path: path.write_bytes(b""),
            lambda path: _write_taxonomy(path, version=_ete3_db_version() - 1),
        ],
        ids=["empty-file", "older-format"],
    )
    def test_a_file_ete3_would_rebuild_is_refused(
        self,
        make: Callable[[Path], object],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        downloads: list[str],
    ) -> None:
        path = tmp_path / "taxa.sqlite"
        make(path)
        monkeypatch.setenv(TAXONOMY_DB_PATH_ENV, str(path))
        with pytest.raises(DeclaredTaxonomyUnavailable, match="without rebuilding"):
            fe.warmup_taxonomy_db()
        assert downloads == []

    @requires_ete3
    def test_a_file_that_is_not_a_database_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, downloads: list[str]
    ) -> None:
        path = tmp_path / "taxa.sqlite"
        path.write_bytes(b"this is not an sqlite database, " * 64)
        monkeypatch.setenv(TAXONOMY_DB_PATH_ENV, str(path))
        with pytest.raises(DeclaredTaxonomyUnavailable, match="not an ete3 taxonomy database"):
            fe.warmup_taxonomy_db()
        assert downloads == []

    def test_a_declaration_ete3_cannot_serve_is_refused_not_nulled(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, downloads: list[str]
    ) -> None:
        monkeypatch.setattr(fe, "_ETE3_AVAILABLE", False)
        monkeypatch.setattr(fe, "_ETE3_IMPORT_ERROR", ImportError("No module named 'cgi'"))
        monkeypatch.setenv(TAXONOMY_DB_PATH_ENV, str(tmp_path / "taxa.sqlite"))
        with pytest.raises(DeclaredTaxonomyUnavailable, match="ete3 cannot be imported"):
            fe.warmup_taxonomy_db()
        with pytest.raises(DeclaredTaxonomyUnavailable, match="No module named 'cgi'"):
            fe.compute_taxonomy(9606, 10090)
        assert downloads == []


class _StoppedConsumer:
    """A consumer that returns at once, as if a signal had stopped it."""

    stopped = True

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def run(self) -> None:
        return None


@pytest.fixture
def start_worker(monkeypatch: pytest.MonkeyPatch):
    """Run ``scripts/worker.py`` through its warm-up with nothing behind it."""
    import scripts.worker as worker_script

    monkeypatch.setattr("protea.infrastructure.logging.configure_logging", lambda **_kw: None)
    monkeypatch.setattr("protea.infrastructure.telemetry.configure_telemetry", lambda **_kw: None)
    settings = Settings(
        db_url="postgresql+psycopg://protea:pw@127.0.0.1:1/protea",
        amqp_url="amqp://protea:pw@127.0.0.1:1/",
        artifacts_dir=Path("unused"),
    )
    monkeypatch.setattr(worker_script, "load_settings", lambda _root: settings)
    monkeypatch.setattr(worker_script, "build_session_factory", lambda _url: MagicMock())
    monkeypatch.setattr(worker_script, "build_operation_registry", MagicMock)
    monkeypatch.setattr(worker_script, "BaseWorker", MagicMock())
    monkeypatch.setattr(worker_script, "QueueConsumer", _StoppedConsumer)
    monkeypatch.setattr(worker_script, "OperationConsumer", _StoppedConsumer)
    pika_logger = logging.getLogger("pika")
    level = pika_logger.level

    def _start(queue: str) -> None:
        monkeypatch.setattr(sys, "argv", ["worker.py", "--queue", queue])
        worker_script.main()

    yield _start
    pika_logger.setLevel(level)


class TestTheWorkerWarmUp:
    @pytest.fixture
    def warmups(self, monkeypatch: pytest.MonkeyPatch) -> list[int]:
        calls: list[int] = []
        monkeypatch.setattr(fe, "warmup_taxonomy_db", lambda: calls.append(1))
        return calls

    @pytest.mark.parametrize("value", ["1", "true", "yes"])
    def test_skip_keeps_it_out_of_startup(
        self, value: str, start_worker, warmups: list[int], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROTEA_SKIP_TAXONOMY_WARMUP", value)
        start_worker("protea.jobs")
        assert warmups == []

    @pytest.mark.parametrize(("queue", "expected"), [("protea.jobs", [1]), ("protea.ping", [])])
    def test_without_skip_it_runs_on_the_queues_that_compute_features(
        self,
        queue: str,
        expected: list[int],
        start_worker,
        warmups: list[int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("PROTEA_SKIP_TAXONOMY_WARMUP", raising=False)
        start_worker(queue)
        assert warmups == expected

    def test_a_missing_declared_file_is_reported_and_never_downloaded(
        self,
        tmp_path: Path,
        start_worker,
        downloads: list[str],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        path = tmp_path / "taxa.sqlite"
        monkeypatch.setattr(fe, "_ETE3_AVAILABLE", True)
        monkeypatch.delenv("PROTEA_SKIP_TAXONOMY_WARMUP", raising=False)
        monkeypatch.setenv(TAXONOMY_DB_PATH_ENV, str(path))
        with caplog.at_level(logging.WARNING):
            start_worker("protea.jobs")
        assert f"{TAXONOMY_DB_PATH_ENV}={path} does not exist" in caplog.text
        assert downloads == []
        assert not path.exists()
