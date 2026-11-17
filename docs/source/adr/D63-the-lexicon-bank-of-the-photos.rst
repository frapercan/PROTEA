ADR-D63: The lexicon bank of the photos: the PLM-free arm reads every member sequence through our own lexicon, decoded in the platform
======================================================================================================================================

:Status: Proposed (2026-11-17). The author approved the replication plan with
         the PLM-free arm first ("Sigamos tus recomendaciones"), whose second
         step is this bank: port the lexicon's decoding into PROTEA as a file
         stage, declare the lexicons as label-free weights with a manifest, and
         compute every member sequence of the photos on the cluster.
:Date: 2026-11-17
:Author: Francisco Miguel Perez Canales
:Phase: platform refactor, phase 4 (representations) for the explainable-units family
:Depends on: ADR-D62 (the bank pattern: a file stage on the cluster, loaded by
             the platform), ADR-D54 (the model weights registry), ADR-D59 (the
             photos), ADR-D60 (three families on one universe), ADR-D61 (the
             homology table, the arm's first source)

Context: the arm's representation is a lexicon that lives in the laboratory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The explainable-units family reads proteins without any language model. Its
units come from our lexicon: a segmental model, one hidden Markov model per
protein in which a background and 4,096 themes compete. Each theme is a profile
with match, insert and delete states per column, and a protein is explained as a
succession of hits of themes. The lexicon was trained without any label, a
principle of the author's ("es imprescindible que el lexico no vea la
funcion"), on UniRef50 2022_03 representatives and UniRef90 variants: 146.8
million proteins, three passes, minimising the bits per residue.

Its outputs fed the arm's best clean reading on 211->227: the family map of
``lexico-93k`` (0.3417) and the fusion of a family map with word fingerprints
(0.3765). Two lexicons remain, kept as label-free weights when the laboratory
stores were cleared on 2026-11-15:

- ``lexico-93k``: step 93,080, held-out bound 3.8200 bits per residue;
- ``lexico-final``: step 160,640, held-out bound 3.8052 bits per residue.

``lexico-2h`` and ``lexico-receta-w2``, two of the combiner's sources, were
deleted that night.

The decoder is laboratory code: ``step57_segmental.py`` (the model),
``segmental_triton.py`` (exact forward kernels), ``cortes_viterbi.py`` (the best
path and its traceback) and ``vectores_posterior.py`` (candidates and theme
mass). It is research code with module switches, and the platform runs none of
it.

Decision
~~~~~~~~

**1. The lexicons enter as declared label-free weights.** One ``model_weights``
row per lexicon (kind ``lexicon``), the 1.6-million-protein weight average of
each run, by URI and sha256 in the store. Its ``meta`` names the training data
(UniRef releases and protein counts), the steps, the held-out bound and the
laboratory commit. ``trained_on_annotation_set_id`` is NULL: the lexicon never
saw a function.

**2. The decoder is ported into PROTEA, inference only.** A package
``protea/core/lexicon`` with:

- the checkpoint read into its tables (emissions, transitions, entries, theme
  shares, background and restart);
- the candidate themes of each protein: ungapped scores per region, the gain
  over the hit's cost in bits, the competition within each region and a state
  budget per protein;
- the exact forward over the candidates on Triton, in the sum semiring (the
  theme mass, from the expected occupancy of each state) and in the max semiring
  with its traceback (the cut).

Training stays in the laboratory. The port is accepted only on parity: on a
sample of the bank spread over the lengths, its segments equal the laboratory's
and its theme masses agree within the tolerance of the kernels. The parity
record is part of the bank's manifest.

**3. Two outputs per lexicon, at full length.** Over the bank's population
(``sequences.parquet`` of ADR-D62: 2,523,987 member sequences of the six photos,
with no length cut):

- **the cut:** per segment the sequence md5, start, end and theme, and per
  residue its theme column, which the column fingerprints read;
- **the theme mass:** per protein, the expected residues of each theme over its
  length, a sparse vector of 4,096 values (a median of 8 themes active).

The two lost lexicons' theme masses are not rebuilt: the theme mass is computed
with the two kept lexicons.

**4. Files in the store, declared by a table.** The outputs are parquet shards
in the artifact store, referenced by URI and sha256, as the homology table's
hits are (ADR-D61). No reader joins them in SQL. A ``lexicon_bank`` row
declares each (lexicon, manifest):

- the weights and the photos;
- the counts;
- the card and the software;
- the parity record;
- the manifest;
- the job, both revisions and the ADR-D58 descriptors.

**5. Computed on the cluster, one lexicon per card.** As in ADR-D62: shards
from the same length order, resumable jobs of two hours or less that stop
before their limit, and a manifest that refuses a missing or changed shard or a
mix of cards or software. A ``load_lexicon_bank`` operation verifies everything
again before it writes the row.

**6. What reads it.** The arm's sources are built from the bank by later
operations of the family, each per photo:

- the segment families (MMseqs2 between segments);
- the column fingerprints and the theme-mass neighbours;
- the theme and family maps to GO.

The maps count annotations of the photo at t0 only, so they are fitted per cut
and never with anything after it.

Consequences
~~~~~~~~~~~~

- The arm reads proteins at full length. The PLM banks read 1,022 residues at
  most.
- The cost is set by measurement, as in ADR-D62. ``lexico-final`` cut a
  92-million-residue frame in 13 minutes on a B200, so the 1,402 million
  residues of the population should take some hours of B200 per lexicon. The
  pilot measures it.
- The store grows by the shards: the columns take about two bytes per residue
  per lexicon, and the cut and the theme mass are small next to them.
- The decoder becomes platform code under its gates (size, tests, typing), so
  the port refactors the laboratory's functions rather than copying them.

Alternatives considered
~~~~~~~~~~~~~~~~~~~~~~~

- **Run the laboratory's scripts on the cluster and ingest their files.**
  Faster to start, but the tool would not come from the platform, which the
  author asked for the photos and which holds here for the same reason.
  Rejected.
- **The theme mass as an embedding configuration in** ``sequence_embedding``.
  It is a per-protein vector, but 4,096 halves for a vector that is almost all
  zeros costs about 20 GB in rows. Rejected for now; the sparse file serves its
  readers.
- **Retrain** ``lexico-2h`` **and** ``lexico-receta-w2``. The replication
  rebuilds its sources over the kept lexicons. Rejected.
