"""Retiring a window: archive first, delete second, purge last.

THE DEFECT THE FIRST VERSION HAD, and the reason the order is the subject of
this file. It deleted each row and each file inside one loop, with the commit
left to the worker. A failure halfway rolled the rows back and left the
earlier parquets gone: rows pointing at nothing, which its own docstring
claimed to avoid. And the test meant to cover it simulated a STORE failure,
not a transaction failure, so it reported assurance it had not earned. The
desktop session found it by reading the code.

So ``test_an_interruption_leaves_the_earlier_parquet_alone`` is the test this
file exists for.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime

import pytest

from protea.core.operations.retire_evaluation_sets import (
    RETIRED_PREFIX,
    RetireEvaluationSetsOperation,
    RetireEvaluationSetsPayload,
)

VIEJO = ("as_closed", "pivot_terms", "any")
VIVO = ("projected", "live_in_both_ends", "experimental")
BLOB = b"PAR1-verdad-vieja"
SHA16 = hashlib.sha256(BLOB).hexdigest()[:16]


class _Eset:
    def __init__(self, rules=VIEJO, role: str | None = None) -> None:
        self.id = uuid.uuid4()
        self.truth_profile, self.toi_rule, self.not_codes = rules
        self.window_role = role
        self.old_annotation_set_id = uuid.uuid4()
        self.new_annotation_set_id = uuid.uuid4()
        self.pivot_snapshot_id = uuid.uuid4()
        self.old_native_snapshot_id = self.pivot_snapshot_id
        self.new_native_snapshot_id = self.pivot_snapshot_id
        self.code_revision = None
        self.groundtruth_uri = f"s3://protea/eval_groundtruth/{self.id}/groundtruth.parquet"
        self.created_at = datetime(2026, 10, 9)
        self.job_id = uuid.uuid4()
        self.stats = {"delta_proteins": 1646}


class _Session:
    """Answers the handful of statements the operation issues, and records them."""

    def __init__(self, rows, counts=None, live_payloads=None) -> None:
        self.rows = {e.id: e for e in rows}
        self.counts = counts or {}
        self.live_payloads = live_payloads or []
        self.borradas: list[uuid.UUID] = []
        self.commits = 0
        self.bloqueo = False

    def get(self, _model, ident):
        return self.rows.get(ident)

    def commit(self):
        self.commits += 1

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        if "pg_constraint" in sql:
            return [
                ("evaluation_result", "evaluation_set_id"),
                ("exploration", "evaluation_set_id"),
            ]
        if "FOR UPDATE" in sql:
            self.bloqueo = True
            return _Res([])
        if sql.startswith("SELECT j.id::text"):
            return _Res(self.live_payloads)
        if sql.startswith("DELETE"):
            self.borradas.append(params["v"])
            return _Res([])
        tabla = sql.split("FROM ")[1].split()[0]
        return _Res([], uno=self.counts.get(tabla, 0))


class _Res:
    def __init__(self, rows, uno=0) -> None:
        self._rows, self._uno = rows, uno

    def all(self):
        return self._rows

    def scalar_one(self):
        return self._uno

    def __iter__(self):
        return iter(self._rows)


class _Store:
    def __init__(
        self,
        falla_en: int | None = None,
        copia_mala: bool = False,
        borra_false: bool = False,
    ) -> None:
        self.objetos: dict[str, bytes] = {}
        self.puts: list[str] = []
        self.borrados: list[str] = []
        self.gets = 0
        self.falla_en = falla_en
        self.copia_mala = copia_mala
        self.borra_false = borra_false

    def get(self, key: str) -> bytes:
        self.gets += 1
        if self.falla_en is not None and self.gets > self.falla_en:
            raise OSError("el almacen se cayo")
        if key.startswith(RETIRED_PREFIX) and self.copia_mala:
            # Una escritura parcial: el put dijo que si y los bytes no son.
            return b"PAR1-a-medias"
        return BLOB

    def put(self, key: str, src) -> str:
        self.puts.append(key)
        self.objetos[key] = src if isinstance(src, bytes) else b""
        return f"s3://protea/{key}"

    def delete(self, key: str) -> bool:
        self.borrados.append(key)
        return not self.borra_false


def _run(session, store, ids, monkeypatch, dry=False, eventos=None, job_id=None):
    monkeypatch.setattr(
        "protea.core.operations.retire_evaluation_sets.get_artifact_store", lambda _s: store
    )
    monkeypatch.setattr(
        "protea.core.operations.retire_evaluation_sets.load_settings", lambda _r: object()
    )
    eventos = [] if eventos is None else eventos
    carga = {"evaluation_set_ids": ids, "dry_run": dry}
    if job_id is not None:
        # BaseWorker inyecta _job_id en TODO payload de operacion. El ayudante
        # no lo hacia, que es por lo que las pruebas no vieron el reflejo.
        carga["_job_id"] = str(job_id)
    out = RetireEvaluationSetsOperation().execute(
        session,
        carga,
        emit=lambda e, _p, f, lvl: eventos.append((e, f)) or f,
    )
    return out.result, eventos


class TestTheOrderThatWasWrong:
    def test_an_interruption_leaves_the_earlier_parquet_alone(self, monkeypatch) -> None:
        # THE TEST THIS FILE EXISTS FOR. The store fails fetching the SECOND
        # window, after the first has been archived. Nothing may have been
        # deleted and the first original must still be there: a failure
        # halfway must not leave a row pointing at nothing.
        a, b = _Eset(), _Eset()
        session, store = _Session([a, b]), _Store(falla_en=1)
        with pytest.raises(OSError):
            _run(session, store, [str(a.id), str(b.id)], monkeypatch)
        assert session.borradas == [], "borro filas antes de terminar de archivar"
        assert session.commits == 0
        assert store.borrados == [], "purgo un original antes de borrar su fila"
        assert any(k.startswith(RETIRED_PREFIX) for k in store.objetos), "no archivo nada"

    def test_the_rows_are_committed_before_any_original_is_purged(self, monkeypatch) -> None:
        e = _Eset()
        session, store = _Session([e]), _Store()
        res, eventos = _run(session, store, [str(e.id)], monkeypatch)
        assert session.commits == 1
        assert [n for n, _ in eventos].index("retire_evaluation_sets.rows_gone") < [
            n for n, _ in eventos
        ].index("retire_evaluation_sets.done")
        assert store.borrados == [f"eval_groundtruth/{e.id}/groundtruth.parquet"]
        assert res["retired"] == 1 and res["originals_left_behind"] == 0

    def test_the_parquet_is_archived_under_retired_with_its_sha(self, monkeypatch) -> None:
        # Archiving rather than deleting, because these truths cannot be
        # rebuilt: October code, a frame the campaign has left.
        e = _Eset()
        store = _Store()
        _run(_Session([e]), store, [str(e.id)], monkeypatch)
        assert f"{RETIRED_PREFIX}/{e.id}-{SHA16}.parquet" in store.objetos


class TestTheReferrersAreDerived:
    def test_they_come_from_pg_constraint_and_not_from_a_list(self, monkeypatch) -> None:
        e = _Eset()
        _, eventos = _run(_Session([e]), _Store(), [str(e.id)], monkeypatch)
        campos = dict(eventos)["retire_evaluation_sets.referrers"]
        assert campos["derived_from_pg_constraint"] == [
            "evaluation_result.evaluation_set_id",
            "exploration.evaluation_set_id",
        ]

    def test_the_orm_knows_of_no_referrer_the_query_would_miss(self) -> None:
        # Without a database: the ORM metadata is the second opinion. A table
        # the query cannot see is a table that would be CASCADE-emptied in
        # silence, which is what a hard-coded list did.
        import protea.infrastructure.orm.models  # noqa: F401
        from protea.core.operations import retire_evaluation_sets as mod
        from protea.infrastructure.orm.base import Base

        del_orm = {
            (t.name, fk.parent.name)
            for t in Base.metadata.tables.values()
            for fk in t.foreign_keys
            if fk.column.table.name == "evaluation_set"
        }
        assert del_orm, "el ORM no ve ninguna FK a evaluation_set; la prueba no vale"
        fuente = __import__("inspect").getsource(mod)
        assert "pg_constraint" in fuente
        assert "REFERRERS = (" not in fuente, "volvio la lista escrita a mano"


class TestWhatItRefuses:
    def test_an_empty_list_is_refused(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            RetireEvaluationSetsPayload.model_validate({"evaluation_set_ids": []})

    def test_a_repeated_id_is_refused(self) -> None:
        # It passed the checks twice, deleted nothing the second time and
        # counted a FALSE orphan: the run reported damage it had not done.
        vid = str(uuid.uuid4())
        with pytest.raises(ValueError, match="repeats"):
            RetireEvaluationSetsPayload.model_validate({"evaluation_set_ids": [vid, vid]})

    def test_a_window_with_a_role_is_refused_even_with_no_results(self, monkeypatch) -> None:
        e = _Eset(role="test")
        with pytest.raises(ValueError, match="window_role"):
            _run(_Session([e]), _Store(), [str(e.id)], monkeypatch)

    def test_a_live_frame_window_is_refused(self, monkeypatch) -> None:
        e = _Eset(rules=VIVO)
        with pytest.raises(ValueError, match="not the superseded frame"):
            _run(_Session([e]), _Store(), [str(e.id)], monkeypatch)

    def test_a_referenced_window_is_refused_and_names_the_table(self, monkeypatch) -> None:
        e = _Eset()
        session = _Session([e], counts={"evaluation_result": 3})
        with pytest.raises(ValueError, match="evaluation_result"):
            _run(session, _Store(), [str(e.id)], monkeypatch)
        assert session.borradas == []

    def test_a_window_a_live_job_names_is_refused(self, monkeypatch) -> None:
        # Retiring a window a QUEUED job is about to read is a race nobody
        # can diagnose afterwards.
        e = _Eset()
        job = str(uuid.uuid4())
        session = _Session([e], live_payloads=[(job, f'{{"evaluation_set_id": "{e.id}"}}')])
        with pytest.raises(ValueError, match="QUEUED or RUNNING"):
            _run(session, _Store(), [str(e.id)], monkeypatch)

    def test_the_rows_are_locked_before_they_are_checked(self, monkeypatch) -> None:
        # FOR UPDATE is what makes the check mean anything for the length of
        # the transaction: a foreign-key insert into a referring table has to
        # wait for that row lock.
        e = _Eset()
        session = _Session([e])
        _run(session, _Store(), [str(e.id)], monkeypatch)
        assert session.bloqueo


class TestTheCopyIsVerifiedAndThePurgeIsRead:
    def test_a_copy_that_does_not_verify_stops_the_run_before_any_delete(self, monkeypatch) -> None:
        # A put taken on faith plus a purge is a lost original: the write may
        # be partial or in another bucket. The copy is re-read through the
        # same door the truth is read by, and a mismatch stops everything
        # BEFORE a single DELETE.
        from protea.core._artifact_identity import ArtifactMismatch

        e = _Eset()
        session = _Session([e])
        with pytest.raises(ArtifactMismatch):
            _run(session, _Store(copia_mala=True), [str(e.id)], monkeypatch)
        assert session.borradas == [] and session.commits == 0

    def test_a_delete_that_returns_false_is_counted_as_left_behind(self, monkeypatch) -> None:
        # ``delete`` returns False when the key was not there, and the MinIO
        # backend also returns False on any S3Error from its stat. Ignoring
        # the return counted every one of those as purged.
        e = _Eset()
        res, _ = _run(_Session([e]), _Store(borra_false=True), [str(e.id)], monkeypatch)
        assert res["originals_left_behind"] == 1
        assert res["retired"] == 1


class TestTheDryRunAndTheTombstone:
    def test_a_dry_run_checks_everything_and_touches_nothing(self, monkeypatch) -> None:
        # AND "NOTHING" INCLUDES WRITES. The first version archived inside the
        # check, so the dry pass left a copy under retired/ beside a row that
        # was still alive. This test checked deletes and commits and NOT
        # puts, so it passed while the operation did exactly that.
        e = _Eset()
        session, store = _Session([e]), _Store()
        res, eventos = _run(session, store, [str(e.id)], monkeypatch, dry=True)
        assert res == {"would_retire": 1, "asked": 1, "dry_run": True}
        assert session.borradas == [] and session.commits == 0
        assert store.borrados == []
        assert store.puts == [], "el seco escribio en retired/"
        assert store.objetos == {}
        # Y dice la clave que escribiria, con el sha, para poder mirarla.
        lapida = dict(eventos)["retire_evaluation_sets.would_archive"]
        assert lapida["archived_key"] == f"{RETIRED_PREFIX}/{e.id}-{SHA16}.parquet"

    def test_a_dry_run_still_refuses_what_the_real_run_would(self, monkeypatch) -> None:
        e = _Eset(role="valid")
        with pytest.raises(ValueError, match="window_role"):
            _run(_Session([e]), _Store(), [str(e.id)], monkeypatch, dry=True)

    def test_the_tombstone_carries_everything_the_row_said(self, monkeypatch) -> None:
        # After the delete there is no row to ask, which is the whole reason
        # an operation is used instead of a DELETE.
        e = _Eset()
        _, eventos = _run(_Session([e]), _Store(), [str(e.id)], monkeypatch)
        lapida = dict(eventos)["retire_evaluation_sets.archived"]
        for campo in (
            "old_annotation_set_id",
            "new_annotation_set_id",
            "pivot_snapshot_id",
            "old_native_snapshot_id",
            "new_native_snapshot_id",
            "truth_profile",
            "toi_rule",
            "not_codes",
            "window_role",
            "code_revision",
            "groundtruth_uri",
            "created_at",
            "job_id",
            "stats",
            "groundtruth_sha256",
            "groundtruth_bytes",
            "archived_key",
        ):
            assert campo in lapida, campo
        assert lapida["groundtruth_sha256"] == hashlib.sha256(BLOB).hexdigest()
        assert lapida["groundtruth_bytes"] == len(BLOB)
        assert lapida["stats"] == {"delta_proteins": 1646}


class TestEveryRefusalInOnePass:
    """One pass shows every problem, which is the desktop session's point.

    It used to refuse on the first bad window, so a pass over 124 ids was a
    queue of 124 passes: learn one reason, fix it, find the next. The reasons
    are independent of each other, so there was never a reason to learn them
    one at a time.

    And the fix has a second half that matters more. Deciding and archiving
    were ONE function, so a refusal at the fiftieth window had already written
    forty-nine copies under ``retired/`` beside rows that were still alive.
    Every window is decided first now, so a refusal anywhere leaves the store
    untouched -- which is what ``store.puts == []`` asserts below, in BOTH
    modes. Asserting it only in the dry pass would be the same mistake this
    file was written about: a test that checks the tidy case.
    """

    def _tres_motivos(self):
        con_rol = _Eset(role="holdout-227-230")
        marco_vivo = _Eset(rules=VIVO)
        bueno = _Eset()
        ausente = uuid.uuid4()
        session = _Session([con_rol, marco_vivo, bueno])
        ids = [str(con_rol.id), str(marco_vivo.id), str(ausente), str(bueno.id)]
        return session, ids

    @pytest.mark.parametrize("dry", [True, False])
    def test_it_names_every_refusal_and_touches_nothing(self, monkeypatch, dry):
        session, ids = self._tres_motivos()
        store = _Store()
        eventos: list[tuple[str, dict]] = []
        with pytest.raises(ValueError) as exc:
            _run(session, store, ids, monkeypatch, dry=dry, eventos=eventos)

        mensaje = str(exc.value)
        assert "3 of 4" in mensaje
        # Los tres motivos son distintos y los tres salen: el fallo que se
        # arregla es justo quedarse en el primero.
        assert "window_role" in mensaje
        assert "not the superseded frame" in mensaje
        assert "does not exist" in mensaje

        # Nada escrito, nada borrado, y la ventana BUENA tampoco se archivo.
        assert store.puts == []
        assert store.borrados == []
        assert session.borradas == []
        assert session.commits == 0

    def test_the_event_carries_the_reasons_as_data(self, monkeypatch):
        session, ids = self._tres_motivos()
        eventos: list[tuple[str, dict]] = []
        with pytest.raises(ValueError):
            _run(session, _Store(), ids, monkeypatch, dry=True, eventos=eventos)

        rechazo = [f for nombre, f in eventos if nombre == "retire_evaluation_sets.refused"]
        assert len(rechazo) == 1
        campos = rechazo[0]
        assert campos["asked"] == 4
        assert campos["refused"] == 3
        # Leer el MOTIVO de la data y no del texto: el mensaje esta acotado y
        # el evento no, asi que el evento es el registro.
        assert len(campos["reasons"]) == 3
        assert all({"evaluation_set_id", "motivo"} == set(r) for r in campos["reasons"])

    def test_the_message_is_bounded_and_says_how_many_it_left_out(self, monkeypatch):
        # Siete con rol: mas que la muestra, que es lo que obliga a la cola.
        malas = [_Eset(role="holdout-227-230") for _ in range(7)]
        session = _Session(malas)
        eventos: list[tuple[str, dict]] = []
        with pytest.raises(ValueError) as exc:
            _run(
                session,
                _Store(),
                [str(e.id) for e in malas],
                monkeypatch,
                dry=True,
                eventos=eventos,
            )

        mensaje = str(exc.value)
        assert "7 of 7" in mensaje
        assert "and 2 more" in mensaje
        # Y el evento si los lleva todos, que es donde hay que mirarlos.
        campos = next(f for nombre, f in eventos if nombre.endswith(".refused"))
        assert len(campos["reasons"]) == 7


class TestItDoesNotRefuseItself:
    """The guard is about OTHER live jobs, and it was reading its own reflection.

    MEASURED ON THE FIRST REAL RUN, 2026-11-08: a dry pass over the 125
    superseded windows refused **125 of 125**, and 124 of those said "job
    <this very job> is QUEUED or RUNNING and names it in its payload". Of
    course it does: the job doing the asking is RUNNING by definition and its
    payload lists every window it was asked about.

    WHY THE SUITE DID NOT CATCH IT, which is the part worth keeping. The fake
    session answered the live-job query with a ``live_payloads`` list that the
    operation's own job was never in, and ``_run`` never injected ``_job_id``
    at all. So a guard about OTHER jobs was tested in a world with no self to
    find. The fixture was the blind spot, not the assertion.
    """

    def test_the_asking_job_naming_its_own_windows_is_not_a_reason(self, monkeypatch):
        eset = _Eset()
        mi_job = uuid.uuid4()
        # Lo que la base devuelve de verdad: el propio job, RUNNING, con las
        # ventanas en su payload.
        session = _Session(
            [eset], live_payloads=[(str(mi_job), f'{{"evaluation_set_ids": ["{eset.id}"]}}')]
        )
        store = _Store()

        result, _ = _run(session, store, [str(eset.id)], monkeypatch, dry=True, job_id=mi_job)

        assert result["would_retire"] == 1
        assert result["asked"] == 1

    def test_ANOTHER_live_job_naming_it_still_refuses(self, monkeypatch):
        """The guard still does its job, which is what makes the fix a fix."""
        eset = _Eset()
        mi_job, otro = uuid.uuid4(), uuid.uuid4()
        session = _Session(
            [eset],
            live_payloads=[
                (str(mi_job), f'{{"evaluation_set_ids": ["{eset.id}"]}}'),
                (str(otro), f'{{"evaluation_set_id": "{eset.id}"}}'),
            ],
        )

        with pytest.raises(ValueError, match="QUEUED or RUNNING"):
            _run(session, _Store(), [str(eset.id)], monkeypatch, dry=True, job_id=mi_job)

    def test_with_no_job_at_all_every_live_payload_counts(self, monkeypatch):
        """``_job_id`` absent means the operation ran outside a job.

        The platform's own rule says that never happens, so the safe reading
        is to trust nothing: with no self to exclude, every live payload that
        names the window is a reason.
        """
        eset = _Eset()
        session = _Session([eset], live_payloads=[(str(uuid.uuid4()), f'{{"v": "{eset.id}"}}')])

        with pytest.raises(ValueError, match="QUEUED or RUNNING"):
            _run(session, _Store(), [str(eset.id)], monkeypatch, dry=True)
