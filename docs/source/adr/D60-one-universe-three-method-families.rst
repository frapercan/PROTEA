ADR-D60: One universe, three method families, four contracts
==============================================================

:Status: Proposed. The requirement is the author's, stated on 2026-11-15:
         "una gran refactorizacion [...] mecanismos esenciales lo mas limpios
         posibles, pero queremos 3 brazos. PLM + Feature engineering +
         reranker, JEPA con representaciones aprendidas, Y este procedimiento
         sin PLM de alta explicabilidad, da igual hasta donde hayamos llegado,
         simplemente procede a integrar la abstraccion [...] La misma base de
         datos, el mismo sistema de workers [...] El universo de corpus de
         proteinas, metadatos, anotaciones, IA, ontologias, representaciones,
         debe de ser absolutamente el mismo"; and the same evening: "Empezamos
         de cero en paralelo. Prod <- dev <- calculos en picasso. Cuando dev
         estabilice, en un hito promocionamos grande". The contracts, the
         operation descriptor, the family names and the branch rule below are
         this record's own, taken from a read-only plan of the same day; the
         decisions that plan leaves open are listed at the end.
:Date: 2026-11-15
:Author: Francisco Miguel Perez Canales
:Phase: platform refactor; development rebuilt from zero beside production
:Depends on: ADR-D59 (the photo at a cut), ADR-D49 (the tiers), ADR-D46
             (information accretion is a corpus artifact), ADR-D40 (temporal
             protocol), ADR-D53 (a laboratory method runs as a platform job),
             ADR-D56 (tables keyed by what they depend on), ADR-D58
             (descriptors)

Context: three lines of method, three universes, and a method the schema can only name as an embedding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The project carries three lines of method. Each reached the platform through a
different door, and each door brought its own universe:

- **PLM embeddings, engineered features and a reranker.** Its predictions are a
  ``prediction_set`` whose identity is an embedding configuration:
  ``prediction_set.embedding_config_id`` is a non-nullable foreign key. A method
  that is not a nearest-neighbour vote over one embedding has no row of its own
  and has to pass as a configuration.
- **A world model with learned representations.** It is trained in the
  laboratory on the transitions dataset of ADR-D56, whose writer declares ten
  tables and implements seven.
- **The PLM-free, explainable procedure.** It runs in the laboratory and enters
  as a container (ADR-D53), with its own bank, taxonomy and term vectors.

Three facts make their numbers incomparable today:

- each line reads its own reference bank, taxonomy and term representations,
  and some of them are dated today rather than at the cut;
- the corpus doors changed under the series (the Swiss-Prot tier of ADR-D49 is
  empty from release 179, see ADR-D59), and the evaluated set was not declared
  at the cut;
- training windows have no role in the schema: ``window_role`` accepts only
  ``valid``, ``test`` or NULL.

Two facts block the execution plan the author asked for, with heavy compute on
a cluster, development on one machine and production on another:

- the heavy stages read the database: ``OperationConsumer`` opens a session per
  message and ``compute_embeddings_batch`` reads its configuration from it, so a
  machine without the database cannot compute;
- the consumer kind of each queue is a hard-coded set in ``scripts/worker.py``
  (``_OPERATION_QUEUES``), and ``POST /jobs`` checks neither the operation, the
  queue nor the kind it is given.

And the cluster connects to nothing: its compute nodes have no route out, and
no permission to open one will be given (2026-11-15). Whatever it computes has
to leave as files.

Decision
~~~~~~~~

**1. One universe, read by id.** For each cut the universe holds:

- the photo of ADR-D59 (the superset, the live tiers and the annotated UniRef50 neighbours),
  loaded as an annotation set that carries its publication date;
- the label state at the cut (truth codes only) and the populations declared at
  the cut;
- frames: windows with a role (``training``, ``validation`` or ``test_spent``)
  and declared truth rules, stored with each window;
- the information accretion tables of ADR-D46, referenced by content sha256;
- one PLM-free homology table and the exclusion groups built from it, with a
  pinned MMseqs2;
- one reference bank, one taxonomy at the cut and one set of term
  representations per ontology snapshot.

Every method of an instance reads these by id, and a test asserts that all the
methods of an instance cite the same ids. The universe refuses a release after
the mark (227), any UniProt release from 2025_04 on, and any source published
after its cut, both where it is built and where it is loaded. Nothing is fitted
on annotations after the instance cut; selecting on a later window is allowed.

**2. Three method families.**

- ``embedding-reranker``: PLM embeddings, engineered features and a reranker.
- ``world-model``: learned representations, a JEPA that predicts the future
  state of knowledge.
- ``explainable-units``: PLM-free, with the explanation read from structure.

The word is *family*, because *arm* already names a published, evaluated
configuration (``protea/api/routers/_arm_identity.py``). A family is a registry
entry with a short and a long descriptor (ADR-D58) and an introduction whose
text is the author's.

**3. The method contract.** A method has its own identity and stops passing as
an embedding configuration.

.. code-block:: python

   @dataclass(frozen=True)
   class MethodSpec:
       family: Family
       code: str
       instance: InstanceCut            # release and ontology snapshot
       recipe: Mapping[str, Any]
       pins: tuple[Pin, ...]            # role, sha256, references
       runtime: Literal["native", "container"]
       explains: bool                   # method_id is uuid5 over the canonical JSON

   @dataclass(frozen=True)
   class TrainingIdentity:
       windows: tuple[TrainingWindow, ...]   # by evaluation set id
       holdout: TrainingWindow | None
       data: tuple[Digest, ...]
       code_revision: str
       sibling_revisions: Mapping[str, str]
       evaluator_revision: str
       label_free: bool
       pretrained_exceptions: tuple[str, ...]

- New tables ``method``, ``method_training_window`` (role ``fit`` or
  ``holdout``), ``method_pin`` and ``prediction_explanation``.
  ``prediction_set`` gains ``method_id``, and its ``embedding_config_id``
  becomes nullable.
- A method declares its instance cut, its training windows by evaluation set
  id and its pins by sha256. The leakage guard reads them.

**4. The compute-stage contract.** A heavy stage is a pure function of its
inputs: a message and the artifact store on the platform's own nodes, files on
the cluster.

.. code-block:: python

   class PureStage(Protocol):
       def run(self, payload: Payload, ctx: StageContext) -> StageOutput: ...
       # StageOutput: the downstream messages, the files and the result

   class FileStage(Protocol):
       def run(self, inputs: Path, out: Path, seal: Seal) -> Manifest: ...
       # Manifest: every output file with its sha256, and the seal

   # StageContext carries emit (buffered, never a session), a deadline, a
   # weights resolver and an optional store.
   check_revision_seal(seal) -> VERIFIED_EQUAL | FOREIGN | UNVERIFIABLE
   StageExecutor(mode=DATABASE | PURE)

- One executor with two modes: ``DATABASE`` (today's behaviour) and ``PURE`` (no
  session).
- A capped retry header. Connection errors are retries. The input is
  acknowledged only after the broker confirms the publication of the output.
- Every message carries a revision seal: the code revision, the six sibling
  revisions and the tree revision. Only a ``FOREIGN`` verdict exits with 78; the
  consumer is cancelled and the message goes back to its queue without spending
  the cap.
- Queues are named by the work and what it needs, never by a machine.
- **The cluster runs file stages only.** A cluster job reads inputs placed
  beforehand and writes files with a seal: the revision seal above, the recipe
  and the digest of the configuration it was given, and the sha256 of every
  output. The files reach the platform through the cluster's login node, the
  way the public downloads already reach the cluster. An ingest operation on
  the platform verifies the seal and writes the rows. ``ingest_method_predictions``
  already does this for a method's predictions, and ``ingest_embeddings`` will
  do it for vectors. The cluster never holds a platform credential and never
  joins a queue.

**5. The evaluation contract.** One evaluator answers an
``EvaluationRequest(prediction_set_id, frame, information_accretion_set_id,
strata, purpose)``, and its refusals live in one place:

- a spent test frame;
- a method whose instance, or any of whose windows or pins, ends after the
  frame starts;
- a missing or mismatched information accretion table;
- a population whose digest does not match;
- a method in smoke status asked for a report;
- an evaluator commit that cannot be verified.

Strata come from the cut's homology table, and every result stores the
evaluator's commit.

**6. The operation descriptor.** Every operation declares
``OperationSpec(domain, kind, queue, purity, resource, family, publishes_to,
status)``. Routing, the consumer kind, the validation of ``POST /jobs`` and the
list of stages the cluster may run as file stages all derive from it, and the
registry refuses an operation without one.

**7. Development from zero, beside production.** Development builds a new
universe with PROTEA operations only, from public sources, in its own stack
(database, broker and bucket) on the development machine. Production keeps its
declared revision and its database until a promotion milestone.

- The refactor lives on a long-lived branch, ``integration/three-families``.
- A merge on ``develop`` does not move production by itself: production serves
  a separate deploy tree, and its declaration is read from the coordination
  repository.
- The branch exists for two reasons. The declaration rule tracks the tip of
  ``develop``, and an urgent production fix must be able to leave from a
  ``develop`` that carries no refactor code.
- The milestone requires four things:
  - the universe rebuilt twice with the same artifact digests;
  - the first path of each family reproduced;
  - no unexplained dead letters for a period the author sets;
  - the three introductions written.
- At the milestone the integration branch merges into ``develop`` with a merge
  commit, and production is declared again.

Consequences
~~~~~~~~~~~~

- The three families become comparable: the same universe ids, the same
  evaluator and the same strata.
- Laboratory numbers do not carry over. Each family reproduces its first path
  in the new universe before any of its numbers is reported.
- The schema changes:
  - it grows the method tables, the frame roles and the photo tables;
  - predictions lose their embedding-configuration identity;
  - the reads that inner-join a prediction set to its embedding configuration
    become outer joins.
- Compute leaves the database stage by stage, starting with the embeddings
  batch. A stage that cannot be made pure stays on a queue that only
  database-holding nodes consume.
- One-time scripts and dead paths are deleted on the integration branch, one
  commit per group, so each deletion can be reverted alone.
- Rebuilding the data does not restore the 227->230 test, which was spent on
  2026-10-27. Development never reads it.

Alternatives considered
~~~~~~~~~~~~~~~~~~~~~~~

- **Three separate data paths, one per family.** Rejected: the numbers would
  describe three universes.
- **Rewriting the platform.** Rejected: the mechanisms that work (the
  two-session job pattern, the artifact store, the guards) are refactored in
  place.
- **Merging the refactor into ``develop`` and suspending the declaration
  rule.** Rejected: an urgent production fix could only be declared after
  landing on a ``develop`` that carries refactor code.
- **Workers on the cluster that join the platform's queues, through tunnels.**
  Dropped on 2026-11-15: the cluster gives no connection and no permission to
  open one, so it runs file stages instead (decision 4).
- **A queue per machine for the cluster.** Rejected by the author: "No quiero
  que las colas sean especificas de picasso".

Implementation order
~~~~~~~~~~~~~~~~~~~~

0. **Rails, with no behaviour change:**
   - the integration branch, the attribution guard and the photo builder of
     ADR-D59;
   - this record and the high-confidence deletions;
   - integration tests on Postgres 18 and the definitions of the development
     stack;
   - an environment check, the declaration path of the revision guard, a
     taxonomy that never downloads and the bootstrap tools.
1. The operation descriptor.
2. The universe from zero.
3. The executor rules, in database mode.
4. The pure embeddings stage, its file-stage form for the cluster and
   ``ingest_embeddings``.
5. The method contract and the first path of each family. The contract and the
   family registry can land right after step 1, because they do not depend on
   the universe's data.
6. The unified evaluation.
7. The front door and the documentation, with the three introductions.
8. The cluster's runbook: the image, the weights placed beforehand and the
   file-stage jobs, each with its ingest.

Decisions taken by the author (2026-11-15)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Weights.** Label-free pretrained weights (PLMs, lexicons, vocabularies)
  enter as declared exceptions with a manifest. Weights fitted on GO labels are
  refit inside the platform.
- **Truth rules of the four windows.** Those of the October headline window:
  experimental gains between t0 and t1, terms live at both ends, NOT rows only
  when experimental, closure under the graph of t0 on both sides.
- **Sequences.** A photo carries the sequences of its paired release (ADR-D59,
  decision 5).
- **Cuts.** Cut 160 takes the ontology its pairing rule gives (2016-10-29). Cut
  205 carries the Swiss-Prot list of its paired release. Cut 227 pairs with
  2025_03, by the mark.
- **The public instrument** stays on the production machine after the
  milestone.
- **Storage.** Every machine works with the disk it has, and nothing is backed
  up: the development universe is rebuilt from public sources. No new hardware.
- **The cluster.** It will give no permission to connect, so it runs file
  stages only, and the platform ingests their files (decision 4).

Still open: which method instance the public instrument serves. The plan
recommends the 227 instance, trained on the four windows and validated through
the 211 instance.
