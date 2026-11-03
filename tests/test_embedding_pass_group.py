"""One forward pass, N embedding configs: the refusal, the fan-out, and what it acts on.

Three tests, because two of them together still leave a hole this project has
fallen into twice.

* Tested only on the refusal, the guard could be a function that always raises.
* Tested only on the happy path, it could be one that never fires.
* Tested only on both, it could still be fanning out over the WRONG sequences:
  the coordinator's ``skip_existing`` filter runs UPSTREAM of inference, and a
  version of it that asks about one config drops every sequence that config
  already holds. The others never see those sequences, the job reports
  SUCCEEDED, and the eleven configs end up at coverages that differ irregularly
  and look plausible, which is worse than the single visible gap this change
  exists to close.
* Tested only on the route the pinned plugin happens to offer, the fan-out is
  verified on one of two branches and the other is reached for the first time by
  a dependency bump. That is the hole the 2026-11-03 bump found: nothing in the
  suite named ``embed_chunks_multi``, and three tests kept out of the backend by
  a patch the shared route bypasses. So the group tests now run ONCE PER ROUTE
  and one of them asserts which route ran.

The fixtures are deliberately HETEROGENEOUS. A group built from three copies of
one config passes against the very defect the guard is for, because every field
compares equal whether or not anything is comparing it.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from protea.core.operations._embedding_pass_group import (
    FORWARD_PASS_FIELDS,
    REDUCTION_ONLY_FIELDS,
    WRITE_ONLY_FIELDS,
    SharedForwardPassError,
    assert_layers_available,
    assert_shared_forward_pass,
)
from protea.core.operations.compute_embeddings import (
    ChunkEmbedding,
    ComputeEmbeddingsBatchOperation,
    ComputeEmbeddingsOperation,
    ComputeEmbeddingsPayload,
)

_noop_emit = lambda *_: None  # noqa: E731


def _config(**overrides):
    """A pretrained ESM config, varied only where a test says so.

    Every field the guard reads is set explicitly. A ``MagicMock`` answers any
    attribute with a fresh mock that compares unequal to itself across
    instances, so a field left unset would make configs look like they diverge
    on it and the guard would pass its test for the wrong reason.
    """
    cfg = MagicMock()
    cfg.id = overrides.pop("id", None) or uuid.uuid4()
    defaults = {
        "model_name": "facebook/esm2_t33_650M_UR50D",
        "model_backend": "esm",
        "max_length": 1022,
        "layer_indices": [0],
        "layer_agg": "mean",
        "pooling": "mean",
        "normalize": True,
        "normalize_residues": False,
        "use_chunking": False,
        "chunk_size": 512,
        "chunk_overlap": 0,
        "embedding_scale": 1.0,
    }
    defaults.update(overrides)
    for field, value in defaults.items():
        setattr(cfg, field, value)
    return cfg


def _layer_grid():
    """The shape this change exists for: one model, three layers, nothing else equal.

    Modelled on the real pending work (esm2_t33_650M at layers 11, 22 and 33).
    The three differ in ``layer_indices`` AND in reduction fields, so a guard
    that silently compared more than it should would be caught here rather than
    in production.
    """
    return [
        _config(layer_indices=[11], pooling="mean", normalize=True),
        _config(layer_indices=[22], pooling="max", normalize=False),
        _config(layer_indices=[33], layer_agg="last", normalize_residues=True),
    ]


# ---------------------------------------------------------------------------
# It REFUSES
# ---------------------------------------------------------------------------


class TestGuardRefuses:
    def test_refuses_group_that_does_not_share_a_forward_pass(self) -> None:
        """Two configs differing in a field that is fed to the model are refused.

        ``max_length`` truncates before tokenisation in every backend, so 1022
        and 2048 are two different inputs and the group is a claim that cannot
        be honoured. The message must NAME the field and both values: a refusal
        that says only "these do not match" sends the caller back to guess which
        of eleven fields it meant.
        """
        group = [_config(max_length=1022), _config(max_length=2048)]

        with pytest.raises(SharedForwardPassError) as exc:
            assert_shared_forward_pass(group)

        message = str(exc.value)
        assert "max_length" in message
        assert "1022" in message and "2048" in message

    def test_refusal_names_every_diverging_field_not_just_the_first(self) -> None:
        """A group wrong in three ways is reported as wrong in three ways."""
        group = [
            _config(model_name="facebook/esm2_t33_650M", model_backend="esm", max_length=1022),
            _config(model_name="Rostlab/prot_t5_xl", model_backend="t5", max_length=2048),
        ]

        with pytest.raises(SharedForwardPassError) as exc:
            assert_shared_forward_pass(group)

        message = str(exc.value)
        for field in FORWARD_PASS_FIELDS:
            assert field in message, f"{field} diverges and the refusal did not say so"

    def test_accepts_the_layer_grid_it_exists_to_enable(self) -> None:
        """The counterweight: a guard that always raises would fail here.

        The three configs differ in ``layer_indices`` and in three separate
        reduction-only fields, and all of that is read after ``hidden_states``
        exists, so one pass serves them.
        """
        assert_shared_forward_pass(_layer_grid())

    def test_refuses_the_same_config_named_twice(self) -> None:
        cfg = _config()
        with pytest.raises(SharedForwardPassError, match="more than once"):
            assert_shared_forward_pass([cfg, _config(layer_indices=[22]), cfg])

    def test_every_identity_field_is_classified(self) -> None:
        """A field added to the recipe and to no tuple here must stop the guard.

        The guard compares an inclusion list, which is only safe while the list
        is known to be complete. This is the check that keeps it complete.
        """
        from protea.core.embedding_identity import IDENTITY_FIELDS

        classified = set(FORWARD_PASS_FIELDS) | set(REDUCTION_ONLY_FIELDS) | set(WRITE_ONLY_FIELDS)
        assert set(IDENTITY_FIELDS) <= classified

    def test_refuses_a_layer_the_model_does_not_have_before_the_pass(self) -> None:
        """Point of the pre-flight: the third config's bad layer costs no inference."""
        model = MagicMock()
        model.config.num_hidden_layers = 33  # 34 hidden states, valid indices 0..33
        group = [
            _config(layer_indices=[11]),
            _config(layer_indices=[22]),
            _config(layer_indices=[99]),
        ]

        with pytest.raises(SharedForwardPassError, match="99"):
            assert_layers_available(group, model)

        assert_layers_available(group[:2], model)


# ---------------------------------------------------------------------------
# It ACTS
# ---------------------------------------------------------------------------


def _sequence(seq_id: int, sequence: str = "ACDEFGHIK"):
    s = MagicMock()
    s.id = seq_id
    s.sequence = sequence
    return s


def _batch_session(configs, sequences):
    session = MagicMock()
    by_id = {cfg.id: cfg for cfg in configs}
    session.get.side_effect = lambda _model, key: by_id.get(key)
    session.query.return_value.filter.return_value.all.return_value = sequences
    # Second ``filter``: the batch honours ``skip_existing`` before the pass,
    # so an unconfigured chain would read as "everything already stored" and
    # skip. Here nothing is stored yet.
    session.query.return_value.filter.return_value.filter.return_value.all.return_value = (
        sequences
    )
    return session


def _marker_of(config):
    """The vector a config's own reduction would produce, and no other's."""
    return np.array([float(config.layer_indices[0]), 0.2, 0.3], dtype=np.float32)


def _plugin(shares_the_pass: bool):
    """A protea-backends double that either offers a shared pass or does not.

    ``spec`` is the discriminator and not a tidiness: ``shared_pass_emitter``
    asks with ``getattr(backend, "embed_chunks_multi", None)``, and a plain
    ``MagicMock`` answers every name, so the fallback route would be
    unreachable from a test and only one of the two branches could ever run.
    """
    names = ["embed_chunks"] + (["embed_chunks_multi"] if shares_the_pass else [])
    plugin = MagicMock(spec=names)
    if shares_the_pass:
        plugin.embed_chunks_multi.side_effect = lambda _m, _t, seq_strs, cfgs, _d: [
            [[ChunkEmbedding(0, None, _marker_of(cfg))] for _ in seq_strs] for cfg in cfgs
        ]
    return plugin


def _group_payload(configs, sequence_ids):
    return {
        "embedding_config_ids": [str(cfg.id) for cfg in configs],
        "sequence_ids": list(sequence_ids),
        "parent_job_id": str(uuid.uuid4()),
        "_job_id": str(uuid.uuid4()),
    }


class TestGroupWritesEveryConfig:
    @pytest.mark.parametrize("plugin_shares_the_pass", [False, True])
    def test_one_pass_publishes_rows_for_every_config_in_the_group(
        self, plugin_shares_the_pass: bool
    ) -> None:
        """The batch worker fans one pass out to N configs' worth of rows.

        The vectors differ per config, because identical vectors would pass
        equally well if the group path quietly wrote the first config's output N
        times under N ids: the fan-out has to be shown carrying each config's own
        reduction, not just N labels.

        RUN ONCE PER ROUTE, because ``_embed_batch_group`` chooses between them
        from the installed plugin and the fan-out has to be right either way.
        Patching ``_embed_batch`` alone reaches only the N-pass branch, so this
        test used to be green or red according to which commit of a separately
        pinned protea-backends was installed: it passed for as long as the pin
        lacked ``embed_chunks_multi`` and went red on the bump with nothing
        about the operation or the assertions having changed.
        """
        op = ComputeEmbeddingsBatchOperation()
        configs = _layer_grid()
        sequences = [_sequence(1, "ACDEF"), _sequence(2, "GHIKL")]
        session = _batch_session(configs, sequences)

        def fake_embed(_model, _tok, seq_strs, config, _device):
            return [[ChunkEmbedding(0, None, _marker_of(config))] for _ in seq_strs]

        payload = _group_payload(configs, [1, 2])
        with (
            patch.object(op, "_load_model", return_value=(MagicMock(), MagicMock())),
            patch(
                "protea.core.operations.compute_embeddings._resolve_backend",
                return_value=_plugin(plugin_shares_the_pass),
            ),
            patch.object(op, "_embed_batch", side_effect=fake_embed),
        ):
            result = op.execute(session, payload, emit=_noop_emit)

        assert result.result["configs_written"] == 3
        assert len(result.publish_operations) == 1  # one message, so one transaction
        _, msg = result.publish_operations[0]
        groups = msg["payload"]["groups"]
        assert [g["embedding_config_id"] for g in groups] == [str(cfg.id) for cfg in configs]
        for cfg, group in zip(configs, groups, strict=True):
            assert len(group["sequences"]) == 2
            for seq in group["sequences"]:
                assert seq["chunks"][0]["vector"][0] == pytest.approx(float(cfg.layer_indices[0]))

    @pytest.mark.parametrize("plugin_shares_the_pass", [False, True])
    def test_exactly_one_route_runs_and_the_plugin_decides_which(
        self, plugin_shares_the_pass: bool
    ) -> None:
        """The two routes are exclusive, and nothing else was asserting it.

        ``_embed_batch_group`` runs ``embed_chunks_multi`` when the installed
        plugin has it and one ``_embed_batch`` per config when it does not. Both
        branches are correct and they are not interchangeable: the shared one is
        where the saved GPU hours are, and the fallback is N forward passes over
        the same sequences. Until this test existed nothing exercised the shared
        branch at all -- no test in the suite named ``embed_chunks_multi`` -- so
        the route production would take once the pin moved was reached for the
        first time by the bump itself.

        The assertion is EXCLUSIVITY rather than a call count, because the
        number of calls follows the batch size and that is not what is being
        pinned down here. What matters is that the route not chosen does not
        also run: a group that paid for the shared pass and then embedded every
        config again would produce exactly the same rows, and the only evidence
        would be the clock.
        """
        op = ComputeEmbeddingsBatchOperation()
        configs = _layer_grid()
        sequences = [_sequence(1, "ACDEF"), _sequence(2, "GHIKL")]
        session = _batch_session(configs, sequences)
        plugin = _plugin(plugin_shares_the_pass)

        def fake_embed(_model, _tok, seq_strs, config, _device):
            return [[ChunkEmbedding(0, None, _marker_of(config))] for _ in seq_strs]

        with (
            patch.object(op, "_load_model", return_value=(MagicMock(), MagicMock())),
            patch(
                "protea.core.operations.compute_embeddings._resolve_backend",
                return_value=plugin,
            ),
            patch.object(op, "_embed_batch", side_effect=fake_embed) as per_config,
        ):
            op.execute(session, _group_payload(configs, [1, 2]), emit=_noop_emit)

        if plugin_shares_the_pass:
            assert plugin.embed_chunks_multi.call_count >= 1
            assert per_config.call_count == 0, (
                "the shared pass ran AND every config was embedded again, which "
                "writes the right rows and silently spends the hours this route saves"
            )
        else:
            assert per_config.call_count >= 1
            assert not hasattr(plugin, "embed_chunks_multi")

    def test_batch_worker_refuses_a_group_handed_to_it_on_the_queue(self) -> None:
        """The guard runs in the worker too, not only in the coordinator.

        A batch message reaches this process directly, so a guard that only ran
        upstream is a guard the message that skipped the coordinator never meets,
        and this is the process that would write the mislabelled rows.
        """
        op = ComputeEmbeddingsBatchOperation()
        configs = [_config(max_length=1022), _config(max_length=2048)]
        session = _batch_session(configs, [_sequence(1)])
        payload = {
            "embedding_config_ids": [str(cfg.id) for cfg in configs],
            "sequence_ids": [1],
            "parent_job_id": str(uuid.uuid4()),
            "_job_id": str(uuid.uuid4()),
        }

        with pytest.raises(SharedForwardPassError, match="max_length"):
            op.execute(session, payload, emit=_noop_emit)


# ---------------------------------------------------------------------------
# What it acts ON
# ---------------------------------------------------------------------------


class TestSkipExistingSpansTheWholeGroup:
    def test_a_sequence_one_config_already_holds_is_still_inferred_for_the_others(
        self,
    ) -> None:
        """The defect this catches: ``skip_existing`` asked about ONE config.

        Sequence 1 is already embedded under config A and missing from B and C.
        Asking only about A drops it from the batch, so B and C never receive it
        and the job reports success; the three end up at coverages that differ by
        an amount nobody is counting. The predicate has to keep any sequence ANY
        config in the group still lacks.
        """
        op = ComputeEmbeddingsOperation()
        config_a, config_b, config_c = _layer_grid()
        payload = ComputeEmbeddingsPayload.model_validate(
            {"embedding_config_ids": [str(c.id) for c in (config_a, config_b, config_c)]}
        )
        group = (config_a, config_b, config_c)
        already_embedded = {(1, config_a.id)}
        session = MagicMock()

        captured: dict[str, object] = {}

        def fake_filter(predicate):
            captured["predicate"] = predicate
            # Stand in for the database: a sequence survives the filter when at
            # least one config in the group has no row for it.
            surviving = [
                (seq_id,)
                for seq_id in (1, 2)
                if any((seq_id, cfg.id) not in already_embedded for cfg in group)
            ]
            q = MagicMock()
            q.all.return_value = surviving
            return q

        query = MagicMock()
        query.filter.side_effect = fake_filter
        session.query.return_value = query

        ids = op._load_sequence_ids(session, payload, [c.id for c in group], _noop_emit)

        assert ids == [1, 2], "a sequence two of the three configs lack must still be inferred"
        rendered = str(captured["predicate"].compile(compile_kwargs={"literal_binds": True}))
        for cfg in group:
            assert cfg.id.hex in rendered, f"{cfg.id} was never asked about"
        assert rendered.count("NOT (EXISTS") == 3

    def test_one_config_filters_exactly_as_it_always_did(self) -> None:
        """The counterweight: with a single config the predicate is the old one."""
        op = ComputeEmbeddingsOperation()
        config = _config()
        payload = ComputeEmbeddingsPayload.model_validate(
            {"embedding_config_id": str(config.id)}
        )
        captured: dict[str, object] = {}

        def fake_filter(predicate):
            captured["predicate"] = predicate
            q = MagicMock()
            q.all.return_value = [(7,)]
            return q

        query = MagicMock()
        query.filter.side_effect = fake_filter
        session = MagicMock()
        session.query.return_value = query

        assert op._load_sequence_ids(session, payload, [config.id], _noop_emit) == [7]
        rendered = str(captured["predicate"].compile(compile_kwargs={"literal_binds": True}))
        assert rendered.count("NOT (EXISTS") == 1
        assert config.id.hex in rendered


# ---------------------------------------------------------------------------
# The payload keeps its old shape working
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    def test_legacy_single_config_payload_still_validates(self) -> None:
        cid = str(uuid.uuid4())
        p = ComputeEmbeddingsPayload.model_validate({"embedding_config_id": cid})
        assert p.embedding_config_ids == [cid]
        assert p.embedding_config_id == cid

    def test_naming_the_configs_twice_is_refused_not_merged(self) -> None:
        with pytest.raises(ValueError, match="name the configs once"):
            ComputeEmbeddingsPayload.model_validate(
                {
                    "embedding_config_id": str(uuid.uuid4()),
                    "embedding_config_ids": [str(uuid.uuid4())],
                }
            )

    def test_one_config_dispatches_in_the_legacy_message_shape(self) -> None:
        """A stale worker must keep working on a one-config job, and REFUSE a group.

        ``ProteaPayload`` forbids extras, so the plural key is what makes a
        consumer too old for the group fail loudly instead of embedding one layer
        of three and reporting success.
        """
        from protea.core.operations._compute_embeddings_helpers import config_group_keys

        assert config_group_keys(["a"]) == {"embedding_config_id": "a"}
        assert config_group_keys(["a", "b"]) == {"embedding_config_ids": ["a", "b"]}
