PLM Attention Complexity
=========================

.. note::

   **Dominant cost: O(L^2 * d) per sequence per layer.**

   Self-attention in a transformer computes a query-key dot product matrix of
   shape ``(L, L)`` for each attention head (``d/h`` head dimension, ``h``
   heads, so ``d`` total), giving O(L^2 * d) time and O(L^2 + L*d) memory
   per layer. With ``num_layers`` layers the total forward pass is
   O(num_layers * L^2 * d). Because ``num_layers`` and ``d`` are fixed per
   PLM checkpoint, the per-sequence cost grows quadratically in length ``L``.

PROTEA supports four PLM families: ESM-2, ESM-C, T5/ProstT5, and Ankh. Each
has a different sequence length cap and a different effective ``d``:

.. list-table::
   :header-rows: 1
   :widths: 24 16 16 44

   * - PLM family
     - Max L (tokens)
     - Embed dim ``d``
     - Notes
   * - ESM-2 (150M, 650M)
     - 1022 residues
     - 480 / 1280
     - CLS and EOS excluded from pooling
   * - ESM-C 300M / 600M
     - 1022 residues
     - 960 / 1152
     - HuggingFace ``EsmModel`` path
   * - T5 / ProstT5-XL
     - configurable (``max_length``)
     - 1024
     - Encoder-only forward; sequence3D prefix for ProstT5
   * - Ankh (base, large)
     - configurable
     - 768 / 1536
     - T5-family encoder

.. rubric:: Chunking to bound VRAM

For sequences longer than the model's ``max_length`` or when VRAM is
tight, PROTEA splits the sequence into overlapping chunks and pools the
per-chunk embeddings. The chunk logic is in
`_compute_chunk_spans <../reference/protea.core.operations.html>`_:

.. literalinclude:: ../../../protea/core/operations/_compute_embeddings_backends.py
   :language: python
   :pyobject: _compute_chunk_spans

Each chunk is an independent forward pass of cost O(chunk_size^2 * d).
The number of chunks is ceil(L / (chunk_size - overlap)), so the total
cost grows linearly in L once chunking kicks in.

.. rubric:: ESM-2 / ESM-C forward pass

The production entry point for a single sequence is ``_embed_esm_one``:

.. literalinclude:: ../../../protea/core/operations/_compute_embeddings_backends.py
   :language: python
   :lines: 180-227

The ``output_hidden_states=True`` flag forces the model to materialise
all intermediate activations (needed for multi-layer pooling), which
doubles peak VRAM compared to a single last-layer pass.

.. rubric:: Cross-reference

Thesis Ch. 5.1 derives the per-sequence VRAM formula and includes a
measured L-vs-throughput curve for ESM-2 650M on the A100 used in
FARM-EXP.13.
