"""The code width is a level, not a property of the head.

The selection runs after the Linear and after the activation, so a different
width is the same trained map read at a different point. Measured on the lab's
cut-211 head over 500 stored ankh_base vectors: the ReLU leaves 2.262 atoms
alive of 8.192 on average and the declared width of 64 keeps 15,41% of the
activated mass, so the 64 is a bottleneck well inside the activation rather
than its end.

What these tests fix is the part that is not arithmetic: an override must not
change a default, must not collide with the head's own name, and must not let a
row claim the head asked for a width the payload chose.
"""

from __future__ import annotations

import uuid

import numpy as np
import pytest

from protea.core.operations.apply_learned_encoder import (
    ApplyLearnedEncoderPayload,
    _declared_suffix,
    _load_encoder,
    _target_model_name,
    resolve_top_k,
)

HEAD = {"in_dim": 8, "dict_dim": 32, "top_k": 5, "training_release": "211",
        "objective": "sfjepa-world-holes-topk"}


def _payload(**kw):
    return ApplyLearnedEncoderPayload(
        source_embedding_config_id=str(uuid.uuid4()),
        encoder_artifact_path="/tmp/head.pt", **kw)


def _artifact(tmp_path, *, top_k=5, in_dim=8, dict_dim=32, activation=None):
    import torch
    import torch.nn as nn

    meta = {"in_dim": in_dim, "dict_dim": dict_dim, "top_k": top_k,
            "order": "pool-then-select", "training_release": "211",
            "objective": "sfjepa-world-holes-topk"}
    if activation is not None:
        meta["activation"] = activation
    art = tmp_path / f"head-{top_k}.pt"
    torch.save({"state_dict": nn.Linear(in_dim, dict_dim).state_dict(), "meta": meta}, art)
    return str(art)


class TestResolve:
    def test_no_override_applies_the_heads_own_width(self):
        """The whole point of the default. Every head already applied runs
        unchanged, so nothing stored is reinterpreted by this change."""
        assert resolve_top_k(dict(HEAD), None, "h.pt") == (5, 5)

    def test_an_override_is_reported_beside_what_the_head_declared(self):
        assert resolve_top_k(dict(HEAD), 12, "h.pt") == (12, 5)

    def test_the_widest_level_is_the_whole_dictionary(self):
        """Allowed, because no selection at all is a real level of the axis.
        It is also where the code stops being sparse, which is why the refusal
        one component further up says so."""
        assert resolve_top_k(dict(HEAD), 32, "h.pt") == (32, 5)

    @pytest.mark.parametrize("bad", [0, -1, -64])
    def test_a_width_that_keeps_nothing_is_refused(self, bad):
        with pytest.raises(ValueError, match="no atoms at all"):
            resolve_top_k(dict(HEAD), bad, "h.pt")

    def test_a_width_wider_than_the_dictionary_is_refused(self):
        """It names atoms the head cannot produce. ``_topk_real`` would accept
        it in silence, since it returns its input whole for any width at or
        above the dictionary."""
        with pytest.raises(ValueError, match="wider than the dictionary of 32"):
            resolve_top_k(dict(HEAD), 33, "h.pt")

    def test_the_refusal_names_the_dense_level_so_the_bill_is_not_a_surprise(self):
        with pytest.raises(ValueError, match="selection is a no-op and the code is dense"):
            resolve_top_k(dict(HEAD), 999, "h.pt")


class TestTheName:
    def test_the_heads_own_width_leaves_the_name_untouched(self):
        """The reason the segment is conditional. A config written before this
        change is found by this name, so the operation does not recompute
        684.048 codes to restate them under a second id."""
        meta = dict(HEAD, top_k=5, top_k_declared=5)
        assert _target_model_name(_payload(), meta, "mean").startswith(
            "learned-code:mean:sfjepa-world-holes-topk:cut211:")
        assert ":k5:" not in _target_model_name(_payload(), meta, "mean")

    def test_an_overridden_width_separates_the_two_configs(self):
        narrow = _target_model_name(_payload(), dict(HEAD, top_k=5, top_k_declared=5), "mean")
        wide = _target_model_name(_payload(), dict(HEAD, top_k=12, top_k_declared=5), "mean")
        assert narrow != wide
        assert ":k12:" in wide

    def test_the_source_id_stays_the_last_segment(self):
        """``_base_config_prefix`` recovers the base config with
        ``rsplit(":", 1)``, so the width goes in front of it like the cut."""
        from types import SimpleNamespace

        from protea.core.operations._learned_code_embed import _base_config_prefix

        p = _payload()
        name = _target_model_name(p, dict(HEAD, top_k=12, top_k_declared=5), "mean")
        assert name.endswith(str(p.source_embedding_config_id)[:8])
        assert _base_config_prefix(SimpleNamespace(model_name=name)) == (
            str(p.source_embedding_config_id)[:8]
        )

    def test_a_head_with_no_declared_key_reads_as_its_own_width(self):
        """``top_k_declared`` is absent on any meta this operation did not
        resolve, and absence must mean "the head's own" rather than raising in
        a name."""
        assert ":k5:" not in _target_model_name(_payload(), dict(HEAD), "mean")


class TestTheRow:
    def test_the_description_says_which_number_came_from_the_lab(self):
        assert _declared_suffix(dict(HEAD, top_k=12, top_k_declared=5)) == " (head declares 5)"

    def test_and_says_nothing_when_the_width_is_the_heads(self):
        assert _declared_suffix(dict(HEAD, top_k=5, top_k_declared=5)) == ""
        assert _declared_suffix(dict(HEAD)) == ""


class TestThroughTheLoader:
    def test_the_override_reaches_the_arithmetic(self, tmp_path):
        """The measurement that matters: the same head, two widths, and the
        count of non-zeros follows the payload."""
        art = _artifact(tmp_path, top_k=5)
        X = np.random.RandomState(0).randn(4, 8).astype(np.float32)
        groups = [X[i : i + 1] for i in range(4)]

        narrow, meta_n = _load_encoder(art)
        wide, meta_w = _load_encoder(art, top_k=12)
        assert (meta_n["top_k"], meta_n["top_k_declared"]) == (5, 5)
        assert (meta_w["top_k"], meta_w["top_k_declared"]) == (12, 5)
        for row in narrow(groups):
            assert int((row != 0).sum()) == 5
        for row in wide(groups):
            assert int((row != 0).sum()) == 12

    def test_the_wide_code_keeps_the_narrow_ones_atoms(self, tmp_path):
        """Why no refit is needed, stated as a test. The selection is a
        ranking over one set of values, so widening it adds atoms and moves
        none: the narrow code is a subset of the wide one, with equal values on
        the atoms they share."""
        art = _artifact(tmp_path, top_k=5)
        X = np.random.RandomState(7).randn(6, 8).astype(np.float32)
        groups = [X[i : i + 1] for i in range(6)]
        narrow = _load_encoder(art)[0](groups)
        wide = _load_encoder(art, top_k=12)[0](groups)
        for n_row, w_row in zip(narrow, wide, strict=True):
            kept = n_row != 0
            assert int(kept.sum()) == 5
            assert np.array_equal(n_row[kept], w_row[kept])

    def test_the_whole_dictionary_leaves_no_zero_by_selection(self, tmp_path):
        """At ``dict_dim`` the code is dense. This is the level the refusal
        message warns about, and it is reachable on purpose."""
        art = _artifact(tmp_path, top_k=5, dict_dim=32)
        X = np.random.RandomState(1).randn(3, 8).astype(np.float32)
        codes = _load_encoder(art, top_k=32)[0]([X[i : i + 1] for i in range(3)])
        assert all(int((row != 0).sum()) == 32 for row in codes)

    def test_a_refused_width_fails_before_the_model_runs(self, tmp_path):
        with pytest.raises(ValueError, match="wider than the dictionary"):
            _load_encoder(_artifact(tmp_path, top_k=5), top_k=99)


class TestThePayload:
    def test_the_default_is_absent_and_not_a_number(self):
        """A default of 64 in the payload would be this operation deciding for
        every head that does not say so, which is the opposite of reading the
        artifact."""
        assert _payload().top_k is None

    def test_a_width_survives_the_json_round_trip(self):
        import json

        p = ApplyLearnedEncoderPayload.model_validate(
            json.loads(json.dumps({"source_embedding_config_id": str(uuid.uuid4()),
                                   "encoder_artifact_path": "/tmp/h.pt", "top_k": 128})))
        assert p.top_k == 128

    def test_the_summary_line_shows_a_chosen_width(self):
        from protea.core.operations.apply_learned_encoder import ApplyLearnedEncoderOperation

        op = ApplyLearnedEncoderOperation()
        assert "top_k=128" in op.summarize_payload(
            {"source_embedding_config_id": str(uuid.uuid4()), "top_k": 128})
        assert "top_k" not in op.summarize_payload(
            {"source_embedding_config_id": str(uuid.uuid4())})


class TestTheReceipt:
    """Kept out of ``execute``, which the smell budget refused at 62 LOC of 60.
    That was the right refusal: the receipt is its own decision."""

    def test_an_override_is_recorded_on_the_job_at_warning(self):
        from protea.core.operations.apply_learned_encoder import emit_top_k_receipt

        seen = []
        assert emit_top_k_receipt(
            dict(HEAD, top_k=12, top_k_declared=5),
            lambda k, _p, f, lvl: seen.append((k, f, lvl))) is True
        (kind, fields, level), = seen
        assert kind == "apply_learned_encoder.top_k_override"
        assert level == "warning"
        assert (fields["applied"], fields["head_declares"]) == (12, 5)
        assert fields["dense"] is False

    def test_the_dense_level_says_so_in_the_receipt(self):
        from protea.core.operations.apply_learned_encoder import emit_top_k_receipt

        seen = []
        emit_top_k_receipt(dict(HEAD, top_k=32, top_k_declared=5),
                           lambda k, _p, f, lvl: seen.append(f))
        assert seen[0]["dense"] is True

    def test_the_ordinary_path_emits_nothing(self):
        from protea.core.operations.apply_learned_encoder import emit_top_k_receipt

        def boom(*_a, **_k):
            raise AssertionError("the head's own width is not an event")

        assert emit_top_k_receipt(dict(HEAD, top_k=5, top_k_declared=5), boom) is False
        assert emit_top_k_receipt(dict(HEAD), boom) is False
