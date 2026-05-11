Backend plugin guide
====================

A backend plugin provides a protein language model (PLM) embedding
implementation. ``protea-core`` dispatches embedding jobs to the
backend whose ``name`` attribute matches the ``model_backend`` field
of the ``EmbeddingConfig`` DB row.

Existing backends shipped in `protea-backends
<https://github.com/frapercan/protea-backends>`_: ``esm``, ``t5``,
``ankh``, ``esm3c``.

.. _backend-abc:

The ABC
-------

Your class must subclass :class:`protea_contracts.EmbeddingBackend`
and implement two abstract methods:

.. code-block:: python

   from protea_contracts.embedding_backend import EmbeddingBackend
   import numpy as np
   from typing import Any

   class EmbeddingBackend(ABC):
       name: str

       @abstractmethod
       def load_model(
           self,
           model_name: str,
           device: str,
           emit: Any,
       ) -> tuple[Any, Any]:
           ...  # Returns (model, tokenizer)

       @abstractmethod
       def embed_batch(
           self,
           model: Any,
           tokenizer: Any,
           sequences: list[str],
           *,
           emit: Any,
           layers: list[int] | None = None,
           layer_agg: str = "mean",
           pooling: str = "mean",
       ) -> np.ndarray[Any, Any]:
           ...  # Returns (batch_size, dim) float16 matrix

Key invariants:

- ``load_model`` returns a ``(model, tokenizer)`` pair. Types are
  ``Any`` because ``protea-contracts`` does not depend on ``torch``.
- ``embed_batch`` returns an ``(N, D)`` ``numpy.float16`` matrix.
  Special tokens (``CLS``, ``EOS``, ``BOS``) must be stripped before
  pooling. ``N`` equals ``len(sequences)``; ``D`` is backend-specific.
- Both methods receive an ``emit`` callback. Use it to report
  structured events::

      emit("backend.mymodel.load_done", None, {"model_name": model_name}, "info")

  The signature is ``emit(event: str, payload, fields: dict, level: str)``.
- Heavy ML imports belong inside ``load_model`` and ``embed_batch``,
  not at module top (lazy-import rule).

.. _backend-packaging:

Packaging snippet
-----------------

``pyproject.toml`` for ``protea-backends-mymodel`` (or a new entry
inside the shared ``protea-backends`` package):

.. code-block:: toml

   [tool.poetry]
   name = "protea-backends-mymodel"
   version = "0.1.0"
   packages = [{ include = "protea_backends_mymodel", from = "src" }]

   [tool.poetry.dependencies]
   python = ">=3.12,<4.0"
   protea-contracts = ">=0.2"
   numpy = ">=1.24"
   # Heavy deps as extras so CI stays import-cheap:
   torch = { version = ">=2.1", optional = true }
   transformers = { version = ">=4.40", optional = true }

   [tool.poetry.extras]
   mymodel = ["torch", "transformers"]

   [tool.poetry.plugins."protea.backends"]
   mymodel = "protea_backends_mymodel:plugin"

.. _backend-tests:

Test scaffold
-------------

Copy and adapt the pattern from ``protea-backends/tests/test_esm.py``:

.. code-block:: python

   """Smoke tests for the mymodel backend plugin."""

   from importlib.metadata import entry_points

   from protea_contracts import EmbeddingBackend
   from protea_backends_mymodel import MyModelBackend, plugin


   def test_plugin_is_mymodel_instance() -> None:
       assert isinstance(plugin, MyModelBackend)


   def test_plugin_implements_embedding_backend_abc() -> None:
       assert isinstance(plugin, EmbeddingBackend)


   def test_plugin_name_matches_entry_point_key() -> None:
       assert plugin.name == "mymodel"


   def test_plugin_resolvable_via_entry_points() -> None:
       eps = entry_points(group="protea.backends")
       matches = [ep for ep in eps if ep.name == "mymodel"]
       assert len(matches) == 1
       assert matches[0].load() is plugin


   def test_load_model_and_embed_batch_are_callable() -> None:
       assert callable(plugin.load_model)
       assert callable(plugin.embed_batch)

These five tests run without installing ``torch`` or any heavy dep.
Integration tests that actually call ``load_model`` / ``embed_batch``
belong in ``protea-core``'s test suite or in a separate extras-gated
job in CI.

.. _backend-toy-example:

Worked example: ``toy`` backend
---------------------------------

The ``toy`` backend returns deterministic random vectors (seeded from
the input sequence). It has no heavy dependencies and can be run in
any environment. It is useful as a template and as a CI speed check
(zero GPU, instant embedding).

.. code-block:: python

   # src/protea_backends_toy/__init__.py
   """Toy backend: deterministic random embeddings seeded from sequence.

   No torch, no transformers. Returns float16 vectors of a fixed
   dimension (64). Useful as a template and in CI.

   Install:
       pip install -e .

   Use in protea-core (after pip install so the entry-point registers):
       EmbeddingConfig(model_backend="toy", model_name="toy-v0", ...)
   """

   from __future__ import annotations

   import hashlib
   from typing import Any

   import numpy as np
   from protea_contracts.embedding_backend import EmbeddingBackend

   _DIM = 64  # fixed output dimension


   def _seed_from_sequence(seq: str) -> int:
       """Derive a 32-bit seed from the MD5 of a sequence string."""
       return int(hashlib.md5(seq.encode()).hexdigest()[:8], 16)


   class ToyBackend(EmbeddingBackend):
       """Deterministic random-vector backend for testing and templates.

       Implements the full EmbeddingBackend contract without any heavy
       ML dependency. ``load_model`` is a no-op; ``embed_batch`` seeds
       numpy from each sequence's MD5 and returns a float16 matrix.
       """

       name = "toy"

       def load_model(
           self,
           model_name: str,
           device: str,
           emit: Any,
       ) -> tuple[Any, Any]:
           """No model to load; return (None, None) as the (model, tok) pair."""
           emit("backend.toy.load", None, {"model_name": model_name}, "info")
           return None, None

       def embed_batch(
           self,
           model: Any,
           tokenizer: Any,
           sequences: list[str],
           *,
           emit: Any,
           layers: list[int] | None = None,
           layer_agg: str = "mean",
           pooling: str = "mean",
       ) -> np.ndarray[Any, Any]:
           """Return a (N, 64) float16 matrix of deterministic random vectors.

           Each row is seeded from the MD5 of the corresponding sequence
           string so identical inputs always produce identical outputs
           regardless of call order.
           """
           if not sequences:
               return np.zeros((0, _DIM), dtype=np.float16)

           rows: list[np.ndarray[Any, Any]] = []
           for seq in sequences:
               rng = np.random.default_rng(_seed_from_sequence(seq))
               vec = rng.standard_normal(_DIM).astype(np.float16)
               rows.append(vec)

           emit(
               "backend.toy.embed_done",
               None,
               {"n_sequences": len(sequences)},
               "info",
           )
           return np.stack(rows)  # shape (N, 64), dtype float16


   #: Module-level instance discovered via ``protea.backends`` entry_points.
   plugin = ToyBackend()

The corresponding ``pyproject.toml`` entry-point stanza:

.. code-block:: toml

   [tool.poetry.plugins."protea.backends"]
   toy = "protea_backends_toy:plugin"

Verify the output directly:

.. code-block:: python

   from protea_backends_toy import plugin

   noop = lambda *a, **k: None
   model, tok = plugin.load_model("toy-v0", "cpu", emit=noop)
   emb = plugin.embed_batch(model, tok, ["MKTII", "ACDEF"], emit=noop)

   print(emb.shape)   # Expected output: (2, 64)
   print(emb.dtype)   # Expected output: float16
   # Calling again with the same input produces identical results:
   emb2 = plugin.embed_batch(model, tok, ["MKTII", "ACDEF"], emit=noop)
   assert (emb == emb2).all()
