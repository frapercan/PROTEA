ADR-D51: The sparse code of every residue, from a frozen vocabulary, stored as shards
=====================================================================================

:Status: Accepted (2026-11-03, the author: merge and deploy)
:Date: 2026-11-03
:Author: Francisco Miguel Pérez Canales
:Phase: platform consolidation (the lab's research enters the platform)

Context: the platform keeps one vector per sequence
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every embedding the platform stores is a ``SequenceEmbedding`` row, one vector per
sequence and configuration. ``encode_residue_sparse`` computes a code per residue,
but only to pool it, and nothing per residue survives the batch.

The lab's sparse line (October 30 to November 2) froze a vocabulary that is worth
keeping per residue:

* **What it is.** A nested flat dictionary of 8192 features over the residue states
  of five Ankh base layers (hidden state indices 2, 10, 22, 40 and 48), read at 128
  active features per residue, each value quantised to one of 256 levels.
* **What it measured.** A single feature localises a domain with a mean average
  precision of 0.57 to 0.64, where the best dense layer reaches 0.01 to 0.08. The code
  costs about 1331 bits per residue under a sequence context coder.
* **Who reads it per residue.** The term generator (a reader that attends over residue
  codes), the exact explanations (which residues and which named features carry a
  term) and the feature atlas. None of them can work from a pooled vector.

Three facts constrain how the platform can hold it.

1. **The code must be the lab's code.** A reader trained on the lab's codes is valid
   only on codes computed the same way. The lab pinned the vocabulary by sha256 and
   wrote a reference encoder whose joint test reproduced the code of the lab's
   evaluation path bit for bit on 64 proteins (37765 residues, 309 million entries,
   zero differences on the GPU).
2. **Ankh has to run at any length.** The corpus holds proteins of up to 35991
   residues. The backend's eager attention builds a (heads, L, L) score tensor and
   stops near 8300 residues on a 12 GB card. The lab computed its states with exact
   attention through FlexAttention, whose memory is linear in L.
3. **The volume is large but bounded.** The corpus is about 300 million residues.
   Measured on 212446 residues of the lab's validation codes: 144.5 active features per
   residue, 436 bytes per residue as int16 indices plus uint8 levels, and 286 to 304
   bytes after zlib. That is about 87 GB for the corpus, against 2.3 TB for the five
   dense layers in half precision.

Decision
~~~~~~~~

1. A vocabulary artifact
^^^^^^^^^^^^^^^^^^^^^^^^

The vocabulary enters as an artifact of kind ``residue-vocabulary``: the dictionary
weights, the thresholds of the 128 active reading, the quantiser table and the
standardisation statistics, each with its sha256, plus a manifest that names the
layers, the reading and the quantiser. The artifact is refused when a hash does not
match, and its manifest hash is part of the configuration identity.

The vocabulary's population was chosen among proteins annotated at GOA 211, so its
configuration declares that release as its cut (``trained_on_annotation_set_id``) and
the temporal gate keeps it out of windows that start earlier.

The code of one residue, which this record fixes:

1. ``x``: the states of the five layers, each standardised per dimension with the
   artifact's statistics, ``(state - mu) / sd``, rounded to half precision and
   concatenated in layer order (3840 values).
2. Pre-activation of feature ``j``: ``p_j = (x - bd) . We_j + be_j`` in single
   precision, with ``We`` transposed and stored row major as ``(3840, 8192)``. The
   layout is part of the recipe: the lab found that another layout changes the last
   bit of some products.
3. Reading: feature ``j`` is active when ``p_j > exp(logt_j)``, strictly.
4. Level: ``clamp(floor(p_j / range_j * 256), 0, 255)``.
5. The code: per residue, the active features in ascending order and their levels.
6. Read back: ``cen[j, level + 1]``, the training mean of the level's bin. The layers'
   reconstruction is the read back vector times ``Wd`` plus ``bd``.

The lab's reference encoder (``compresion/scripts/codificador_referencia.py`` in the
sf-jepa repository, commit 5e456de) is the specification, and the platform's
implementation is tested against test vectors produced by it.

2. Exact attention, layer by layer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

protea-backends gains a per-layer residue path for Ankh: the selected hidden states
returned separately rather than averaged, computed with exact attention through
FlexAttention (the T5 relative bias enters as a score modifier read from a small
table, so no L by L tensor exists). The precision is the one the vocabulary was
trained on: bfloat16 weights, RMS norms and the feed forward output in single
precision, scores accumulated in single precision, and batches of up to 16384 tokens
sorted by length. The configuration records this policy. The node needs a C compiler
and the Python headers, because Triton builds a small helper the first time.

Layer indices keep the platform's convention (``[0]`` is the last hidden state). The
vocabulary's hidden state indices 2, 10, 22, 40 and 48 are ``layer_indices`` 46, 38,
26, 8 and 0.

3. Shards in the artifact store, an index in the database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A GPU batch writes one shard to the artifact store: the codes of its sequences as
arrays (sequence ids, residue offsets, entry offsets, feature indices delta encoded
within each residue, levels as uint8), compressed with zlib, with the vocabulary's
manifest hash and the backend's precision policy in its header. A new table
``residue_code`` holds one row per sequence and configuration: the shard key, the
position of the sequence in the shard, its residue and entry counts, and the shard's
sha256. A sequence is encoded once per configuration. A batch that fails leaves no
row, and a retry encodes what has no row.

Rows per residue in Postgres were rejected: 300 million residues times 144 entries
is over 40 billion values, on a database the campaign already measured as disk bound.

4. One operation, two queues
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``encode_residue_vocabulary`` has the three stages of ``compute_embeddings``. A
coordinator on ``protea.embeddings`` resolves the artifact, creates the configuration and
fans out batches to ``protea.embeddings.batch``. There the card runs Ankh and the
vocabulary, puts the shard in the artifact store (retrying, since the key names its
content) and publishes one message to ``protea.embeddings.write``. On the host that owns
the database, ``store_residue_codes`` fetches the shard, checks its sha256 and that it
holds what the message lists, inserts the index rows in one transaction and advances the
parent job. The database is written only on that host, never across the link from the
card. The codes do not travel in the message: through the broker, 87 GB would be stored
in its message store and written a second time to the artifact store. It trains nothing.

5. A read path
^^^^^^^^^^^^^^

``load_residue_codes(session, store, embedding_config_id, sequence_ids)`` returns, per
sequence, the active features and levels of every residue, and optionally the read
back values. The generator and the explanations will consume it; their operations
are later records.

Validation gates
~~~~~~~~~~~~~~~~

1. **Unit parity.** The platform's encoder reproduces the reference encoder's test
   vectors bit for bit.
2. **Same states, same code.** On the lab's 64 proteins, the platform's encoder fed
   with the lab's cached states gives the lab's code with zero differences.
3. **Platform states.** On the same 64 proteins, the platform's backend recomputes the
   states. The share of entries that differ is reported (expected rare, at thresholds
   and level edges, from batch composition in bfloat16), and the mean number of active
   features per residue must stay within the range the lab measured (126 to 145).

Measured on 2026-11-03 on the desktop node (RTX 3060):

* **Gate 1.** The platform's encoder reproduces the reference encoder's test vectors bit
  for bit (``tests/test_residue_vocabulary.py``).
* **Gate 2.** On the 64 proteins (37765 residues, 309370880 entries), zero entries
  differ, and the read back equals the lab's values bit for bit.
* **Gate 3.** On the same proteins, the states the platform computed with the
  exact-attention backend (protea-backends PR 46) are identical to the lab's cached
  states after standardisation, and zero entries differ. Both give 126.44 active
  features per residue.
* **Gate 3, long proteins.** The 64 proteins reach 2237 residues, so the platform's
  path was also compared with the lab's code of the 211 to 227 frame's queries (computed
  by the lab's step39 in its own batches): all 1058 queries longer than 2237 residues
  and 300 shorter ones as a control. The share of active decisions that differ is
  0.050 % up to 2237 residues, 0.036 % from 2238 to 7999 and 0 above 8000 (32 proteins,
  up to 34350 residues). Levels differ in 1.0 % to 1.8 % of the common actives, partly
  because the lab's table keeps pre-activations in half precision. The reconstruction
  does not degrade with length (R2 0.706, 0.747, 0.764 by band), and the longest protein
  encoded twice gives the same code.
* **What the differences are.** Not the library: gate 3 run with the platform's
  transformers 4.48.1 is still exact. Not the length. They come from batch composition:
  in bfloat16 the same protein in another batch gets slightly different states, which
  also separates the lab's own two computations. So the code is exact for a given
  batching and deterministic, and between batchings about 0.04 % of active decisions
  move, concentrated in a few proteins.
* **Other checks.** Protea-backends' tokenizer change (#45) gives the same ids on
  138530 sequences under transformers 4.48.1, and the exact path encodes 35213 residues
  in 22 s with a 2.27 GB peak on the RTX 3060. The vocabulary's elementwise steps run in
  chunks of residues (one product per sequence), so the longest proteins fit the card.

What is not validated
~~~~~~~~~~~~~~~~~~~~~

The exact path computes the same Ankh at any length, and the vocabulary reconstructs
those states as well at 30000 residues as at 300. That is a statement about the
computation, not about the representation. Ankh was pretrained on much shorter
sequences, and every exam that supports the vocabulary (domains, sites) used proteins
under 3000 residues. So for proteins above about 3000 residues the codes are computed
exactly, but whether they carry function as well as the codes of ordinary proteins is
unmeasured. About 7.8 % of the corpus residues are above 2237 residues and 0.9 % above
8000.

Until that band is measured, no result is claimed for it: consumers can separate it by
``residue_count``, and function results on a frame report it as its own length stratum.
The two cheap measurements that come first are the agreement between the code of the
whole protein and the code of overlapping windows of it, and the feature usage by length
band (active features per residue and which features appear).

Measured on 2026-11-03 on the 211 label set's sequences, with the vocabulary this record deploys: the code of
the whole protein against the code of overlapping windows of 1021 residues (stride 511,
each residue from the window where it is most central).

.. list-table::
   :header-rows: 1

   * - Length band
     - Proteins
     - Active features per residue, whole / windows
     - Active decisions that differ
     - Mean per-residue Jaccard
   * - 1000 to 1999 (control)
     - 150
     - 117.8 / 119.2
     - 11.3 %
     - 0.894
   * - 3000 to 4999
     - 150
     - 109.5 / 116.8
     - 35.7 %
     - 0.708
   * - 5000 to 7999
     - 150
     - 105.5 / 114.8
     - 40.4 %
     - 0.679
   * - 8000 or more
     - 63
     - 102.3 / 113.5
     - 50.7 %
     - 0.619

Part of the gap is expected, since windows remove more context from longer proteins.
The falling activation of the whole-protein code is the warning: it says the states of
very long proteins drift away from the regime the vocabulary was fitted in, while the
window code stays where ordinary proteins are. Whether whole or windowed codes carry
function better for long proteins is decided on the frame. Until then the long band is
reported apart and nothing is claimed for it.

Consequences
~~~~~~~~~~~~

* A full corpus pass takes about 9 to 10 hours on the RTX 3060 node, at the rate the
  lab measured (25.6 million residues in 45 minutes), and about 87 GB of the artifact
  store.
* Retrieval, predictions and the evaluation do not change. Pooled codes for retrieval,
  the generator as a method and the reader are out of scope here.
* zlib is a placeholder. The sequence context coder the lab measured would bring a
  residue near 166 bytes, and a later record may adopt it without changing the index.

Alternatives considered
~~~~~~~~~~~~~~~~~~~~~~~

* **Store the dense states.** 2.3 TB for five layers in half precision.
* **Pool only.** ``encode_residue_sparse`` already does it, and the readers need
  residues.
* **The eager backend.** It stops near 8300 residues, and it is not the computation
  the vocabulary was trained on.
