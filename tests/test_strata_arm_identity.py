"""An arm is a configuration, not a label.

Rung 2's layer axis put ankh-base at layers 48 and 10 into the same
evaluation set. They stayed apart in the comparison only because nobody
had set ``display_name`` on the second, so the fallback to ``model_name``
happened to differ. Had the name been set on both, two layers would have
merged into one series and the comparison would have reported a number
belonging to neither, with nothing on the surface saying so.

The identity is the EmbeddingConfig. Two configs of one model differ in
every axis this campaign varies except the model itself.
"""

from __future__ import annotations

import inspect

from protea.api.routers.strata import _ARMS, _label


def _arm(cid, model, display=None, layers=None, k=1):
    return {
        "evaluation_result_id": f"res-{cid}",
        "embedding_config_id": cid,
        "model": model,
        "display_name": display or model,
        "layer_indices": layers,
        "k": k,
    }


def test_the_query_selects_the_identity_not_only_the_name():
    src = str(_ARMS)
    assert "ec.id" in src
    assert "layer_indices" in src


def test_one_config_per_model_keeps_its_label():
    # Renaming every arm to fix a collision that is not there makes the
    # common case worse, and every arm before rung 2's layer axis is in
    # the common case.
    arms = _label([_arm("aaaa1111", "ankh-base", "ankh_base", [0])])
    assert arms[0]["display_name"] == "ankh_base"


def test_two_configs_of_one_model_are_disambiguated():
    # The exact collision: same model, same display name, different layer.
    arms = _label([
        _arm("aaaa1111", "ankh-base", "ankh_base", [0]),
        _arm("bbbb2222", "ankh-base", "ankh_base", [10]),
    ])
    labels = {a["display_name"] for a in arms}
    assert len(labels) == 2
    assert any("[0]" in x for x in labels)
    assert any("[10]" in x for x in labels)


def test_it_falls_back_to_the_config_id_when_the_layer_is_absent():
    # Two configs of one model differing in something other than layer,
    # pooling or normalisation for instance. The id always distinguishes.
    arms = _label([
        _arm("aaaa1111", "ankh-base", "ankh_base", None),
        _arm("bbbb2222", "ankh-base", "ankh_base", None),
    ])
    labels = {a["display_name"] for a in arms}
    assert len(labels) == 2
    assert any("aaaa1111" in x for x in labels)


def test_different_models_are_untouched():
    arms = _label([
        _arm("aaaa1111", "ankh-base", "ankh_base", [0]),
        _arm("bbbb2222", "prot-t5", "prot_t5", [0]),
    ])
    assert [a["display_name"] for a in arms] == ["ankh_base", "prot_t5"]


def test_the_row_carries_the_config_id_so_a_caller_can_key_on_it():
    # A label is for a reader. A caller comparing arms needs something that
    # cannot collide, and that is what this row was missing.
    src = inspect.getsource(_label)
    assert "embedding_config_id" in src
    arms = _label([_arm("aaaa1111", "ankh-base")])
    assert arms[0]["embedding_config_id"] == "aaaa1111"
