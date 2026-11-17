ADR-D65: The explainable sources of a cut: segment families and count maps, built from what the cut knows
==========================================================================================================

:Status: Proposed (2026-11-17). Step 5 of the replication plan the author
         approved that day ("Sigamos tus recomendaciones"): the PLM-free arm,
         rebuilt from zero inside the platform. This record covers its first
         two sources; fingerprints, homology transfer, species and the combiner
         come in later records.
:Date: 2026-11-17
:Author: Francisco Miguel Perez Canales
:Phase: platform refactor, phase 5 (methods), explainable-units family
:Depends on: ADR-D59 (the photo of a cut), ADR-D61 (the homology table, its
             pinned MMseqs2), ADR-D63 (the lexicon bank), ADR-D64 (fitting inside
             the platform)

Context: the arm reads GO from its own units by counting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The laboratory's explainable line names a protein by the units the lexicon cuts
it into, and reads each unit into GO by counting the annotated proteins that
carry it. Its two steps on 211->227 were these (laboratory numbers, set aside
by ADR-D64 as targets, kept here as a guide):

- **The count map over themes** (card 1): for unit u and term t,
  ``p(u, t) = (K_u(t) + m * pi(t)) / (N_u + m)``, where N_u counts the donors
  whose sequence has a segment of u, K_u(t) those whose closed state holds t,
  pi(t) is the aspect's base rate and m one strength per aspect. With
  ``lexico-93k``'s themes it gave 0.2786.
- **The count map over segment families** (card 2): segments linked by residue
  homology across themes with MMseqs2, grouped and joined by profiles, with no
  label and no Pfam. With ``lexico-93k``'s families at the permissive scope it
  gave 0.3417, the arm's best single source.

The lexicon bank (ADR-D63) gives every member sequence of the photos its
segments and columns. Nothing in the platform turns them into families or reads
them into GO.

Decision
~~~~~~~~

**1. A source is a function of one cut.** For a window t0 -> t1, every source
is built at t0 from the photo of t0 and from nothing later:

- its donors are the photo's ``truth`` members (142,514 at 211);
- a donor's state is its truth-channel rows of GOA t0 closed under the photo's
  ontology, with its NOT terms;
- its targets are the window's targets, the superset at t0 that gained.

The source cuts are 160, 177, 194 and 211 for instance 211, and 227 for
instance 227. Cut 205 only ends a window.

**2. The segment families of a cut are a file stage on the cluster** (ADR-D61's
pattern). For one cut, one lexicon and one scope:

- *Inputs:* the lexicon bank's cut shards, the donors' md5s and the md5s of the
  superset members that are not donors (484,157 members at 211).
- *Cores:* a segment's residues minus every insert run longer than its own
  number of match residues.
- *Searches:* ADR-D61's pinned MMseqs2 (18-8cc5c, the laboratory's own
  version): donor cores all against all, their reversed cores as decoys, and
  the other cores against the donor cores. Sensitivity 7.5, at most 1,000
  targets per query, E-value 0.1.
- *Decoys:* every core of a decoy component of more than 10 cores is masked.
- *Links:* both coverages at least 0.5. The strict scope keeps E times the
  donor cores below 8.6e-5; the permissive scope keeps E below 32,768 over the
  donor cores. A link that covers only one side is a partial link.
- *Groups:* a greedy set cover over the links; every member links to its
  representative directly, so nothing chains.
- *Families:* a profile per group, searched against the representatives and
  against the reversed representatives; two groups join when the profile finds
  the other's representative within the scope. The constant is raised until at
  most three reversed hits remain.
- *Clans:* components of the families joined by partial links. They are
  reported, not used as units.
- *Targets:* a non-donor segment takes the family of its best link to a donor
  core.

The output is ``segments.parquet`` (md5, start, end, theme, core length, group,
family, clan, masked) and a manifest. ``load_segment_families`` verifies it
against the lexicon bank and the photo and declares it in a
``segment_families`` table; the files go to the artifact store. No label is
read. The population is chosen by the cut's truth membership, and the row says
so.

**3. The count map of a cut is a fit on** ``protea.training`` **(CPU).**

- *Units:* themes, families, or families with themes.
- *Counts:* per aspect, N_u, K_u(t) and pi(t), over the donors.
- *Strength:* m, one per aspect, fitted on the donors alone by the bits of
  leaving each donor's exclusion group out of its own units (golden search on
  log m). The window's targets never enter it.
- *Exclusion:* a donor never counts for a protein with its sequence. The group
  is the donors that share its md5: the protein itself, its merged aliases and
  its identical twins. The laboratory's strict policy also took out its
  near-identical components; the refined one, which kept twins and
  near-identicals in, moved the map by +0.0002 on 211->227. The platform keeps
  the exact group only.
- *Storage:* the map (K, N, pi and m per unit and term) is a weight file of a
  new kind, ``count_map``, registered with the annotation set it counted.

**4. The reading writes predictions with their witness.**

- A target's score for t is the largest p over its units, floored at pi(t).
- Its NOT terms at t0 and their descendants are taken out, and so are the
  roots.
- The 100 best per aspect are kept.
- Every row keeps its witness (the unit, K and N after the exclusion) as its
  ``prediction_explanation``.

The prediction set belongs to a ``method`` of the explainable-units family
(ADR-D64), one per variant.

**5. The first source replicated is the theme map.** It needs no MMseqs2 and
checks the reading, the explanations and the evaluator end to end. The family
maps follow, with ``lexico-93k`` and ``lexico-final`` compared on 211->227.

Consequences
~~~~~~~~~~~~

- Every cut has its own families. A protein can belong to different families
  at 194 and at 211, because the donors changed. That is the point: instance
  211 sees nothing after 211.
- The families cost MMseqs2 runs on the cluster: per cut, about half a million
  donor cores against themselves and about two million other cores against
  them. A pilot measures one cut before the rest run.
- Half of the cores are shorter than 25 residues and rarely link, so many
  segments stay without a family. The laboratory measured this; the theme units
  cover them.
- The maps are fits, so they wait for the author's word (ADR-D64, decision 6).

Alternatives considered
~~~~~~~~~~~~~~~~~~~~~~~

- **One set of families over all six photos.** Membership after t0 is
  information after t0. Rejected.
- **Families over UniRef at scale** (the laboratory's card 6, discovery).
  Parked: it changes the population, not the method.
- **The laboratory's near-identical components for the exclusion.** The
  policies differed by +0.0002 on the map. The md5 group is exact and needs no
  component table, and it is kept.
