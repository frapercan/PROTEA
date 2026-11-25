"""A window marked ``test_spent`` cannot be re-designated (ADR-D60, decision 5).

A test window is read once. Its truth cannot become unread, so no rebind may move
a spent window back to a role that admits another reading.
Moving a window to ``test_spent`` stays allowed: that is how it is marked spent.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from protea.core.operations.generate_evaluation_set import GenerateEvaluationSetOperation


def _window(role: str | None) -> SimpleNamespace:
    return SimpleNamespace(id=uuid.uuid4(), window_role=role)


def test_a_spent_window_refuses_another_role() -> None:
    spent = _window("test_spent")
    with pytest.raises(ValueError, match="spent test"):
        GenerateEvaluationSetOperation._rebind_window_role(
            MagicMock(), spent, "validation", MagicMock()
        )
    assert spent.window_role == "test_spent"


def test_a_window_can_be_marked_spent() -> None:
    window = _window("validation")
    emit = MagicMock()
    GenerateEvaluationSetOperation._rebind_window_role(MagicMock(), window, "test_spent", emit)
    assert window.window_role == "test_spent"
    emit.assert_called_once()


def test_asking_a_spent_window_for_its_own_role_is_a_no_op() -> None:
    spent = _window("test_spent")
    emit = MagicMock()
    GenerateEvaluationSetOperation._rebind_window_role(MagicMock(), spent, "test_spent", emit)
    emit.assert_not_called()
