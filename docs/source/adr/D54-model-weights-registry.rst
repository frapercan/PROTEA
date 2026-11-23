ADR-D54: Model weights in one registry, uploaded or registered, and used from the instrument
=========================================================================================

:Status: Accepted
:Accepted: 2026-11-08
:Date: 2026-11-03
:Author: Francisco Miguel Pérez Canales
:Phase: platform consolidation (the lab's research enters the platform)
:Depends on: ADR-D51 (per-residue codes from a frozen vocabulary)

Context: weights are addresses typed into payloads
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The lab's fitted weights enter the platform as files in the artifact store, and an
operation finds them through an address in its payload. ``encode_residue_vocabulary``
reads ``vocabulary_artifact_uri`` and ``apply_learned_encoder`` reads
``encoder_artifact_uri``. A lab method's bundle lives on the GPU node, where
``run_lab_method`` mounts it. Nothing lists these files. To launch an operation, an
operator has to know a key, type it, and trust that the file behind it is the one meant.

The re-ranker is the exception. Its boosters have their own registry (``reranker_model``),
with an upload and an import by reference, and the instrument page lists and activates
them. The author asked for the same for every kind of weight, on 2026-11-03: "necesitamos
UI/UX total, las operaciones en Panel de control, los pesos almacenados y listado que se
puedan subir... parecido a las representaciones aprendidas o mejor".

Decision
~~~~~~~~

Accepted on 2026-11-08. The author asked for the registry on 2026-11-03, in the words
quoted above. He approved this record together with the four other open points of that
day: in the GPU node's session, reported there as "vale. Vamos a 1-5" and kept in the
lab as decision D34, and in this machine's session when asked directly whether the
status should move.

What made the question concrete is the kind added last: the lab's per-term text vectors
have to reach the platform before ADR-D56's ``terms`` table can be written, and the
author chose the route when asked in this machine's session, "Que los suba el por la
API", with the operator credential he gives the GPU node.

1. One table for every kind of weight
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``model_weights`` has one row per file:

* kind: ``residue_vocabulary``, ``learned_encoder``, ``world_model``, ``reader``,
  ``generator``, ``method_bundle`` or ``term_text_vectors``;
* name, description and format;
* the artifact store key, which is unique;
* sha256 and size;
* free metadata (layers, width, actives, levels, recipe, provenance);
* the annotation set it was trained on, when it has a cut;
* when it was registered and by whom.

The re-ranker keeps its own registry, and the weights page links to it.

* **Content addressed.** An uploaded file is stored at
  ``model-weights/<kind>/<name>-<sha256[:16]><extension>``. The same bytes uploaded twice
  give the same key and the same row, and the API answers 409 with the existing id.
* **Registered by reference.** A file already in the store, such as the deployed residue
  vocabulary at a key of the form ``residue-vocabulary/vocabulary-<name>-<sha256[:16]>.npz``,
  is registered with its key and the sha256 its producer recorded. The API reads the object and refuses the row if the
  hash differs.
* **A kind for the term vectors.** ``term_text_vectors`` is one vector per GO term of an
  ontology axis, from a text embedding of the term's name and definition. ADR-D56's
  ``terms`` table carries them by reference, through its ``vector_uri`` and
  ``vector_sha256``, because one vector per term over a whole axis is a file and not a
  column, so the file needs a row that gives its digest and this is the registry that
  holds one. It is a new kind and not one of the six: none of them describes it, and
  registering it under a name that does not fit would make ``kind`` stop answering the
  one question it exists for, which is what this file is. The bytes are in the lab's own
  store and not in the platform's, so ``terms`` refuses to be written until the row
  exists, and a digest quoted in a message is a locator for bytes nobody on this side
  can check.
* **Uploaded in parts.** The web proxy caps a request at 100 MB, and the deployed residue
  vocabulary alone is 260 MB. A dialog therefore sends the file in parts: create an upload, send numbered
  parts, then complete. The server joins the parts on disk, hashes the result, puts it in
  the store and inserts the row. No request carries more than one part, and the API never
  holds a whole file in memory.

2. What uses a weight is shown, not stored
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The detail of a weight lists the configurations made from it. For a residue vocabulary,
these are the ``residue-vocabulary`` configurations whose name carries the first 16
characters of its sha256, with their coverage from the per-residue codes view of the embeddings page. The
detail also lists the jobs whose payload names its key. Both lists are computed when the
detail is read, so they cannot fall out of step with the rows they describe.

3. The instrument launches from the weight
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The instrument gets a "Model weights" page with a table grouped by kind, a detail view,
an upload dialog and a register-by-reference dialog, all for operators. A weight offers
the operations that read it, each in a dialog with a validated form. A residue vocabulary
offers "Encode a corpus" (``encode_residue_vocabulary``, choosing the source configuration
and the device). A learned encoder offers ``apply_learned_encoder``. A method bundle
offers ``run_lab_method`` once ADR-D53 is in. Each dialog creates the job through
``POST /jobs`` and links to it.

4. The registry is written without a job, and that is the exception
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This project's rule is that every operation goes through ``POST /v1/jobs`` and never a
direct call, because a job leaves a row, an event log and the revision it ran under, and
a direct call leaves rows that nothing accounts for. **The two writes of this registry
are an exception to that rule**, and the exception is stated here so that it is a
decision and not a hole: the multi-part upload and the registration by reference are
endpoints, authenticated with an operator credential, and no job is created for either.

The reason is mechanical. A job's input travels in its payload through the broker, and an
operation message is bounded at twice 600 seconds with nothing pumping the heartbeat, so
a file of hundreds of megabytes cannot be delivered that way. Registering bytes is also
not an operation on the data: it adds no annotation, no vector and no measurement, and
it is the step that makes a later operation attributable.

What replaces the job's guarantees is in the row and in the server, not in the caller.
The key is content addressed, so the same bytes give the same key and the same row and a
second upload answers 409 with the existing id. A registration by reference reads the
object and refuses the row when the hash differs, so the identity of the bytes is
checked here and not asserted by whoever sends them. The row records who registered it
and when, next to the sha256 and the size.

**The exception covers writing the registry and nothing else.** Every operation that
reads a weight is still launched through ``POST /v1/jobs``, with the key in its payload,
which is what section 3 describes.

What stays outside
~~~~~~~~~~~~~~~~~~

Training, as in ADR-D51 and ADR-D52: weights are fitted in the lab and on the cluster and
enter as files with their hashes and cuts. The platform registers, stores, lists and uses
them.

Consequences
~~~~~~~~~~~~

* **One additive migration** (``model_weights``). Downgrade is a drop.
* **Nothing existing moves.** Payloads keep their addresses, so an operation does not need
  the registry to run. The registry is how an operator finds the address and checks the
  bytes behind it.
* **Disk on the API host** while a file is uploaded, in ``api.weights_upload_dir``
  (``~/.cache/protea/weights-uploads`` by default). Never the system temporary folder: on the
  API host it is a tmpfs, and an upload there takes RAM from the machine that runs Postgres,
  RabbitMQ and MinIO. Each part is deleted once joined, so the folder holds the file plus one
  part; an upload that would leave less than a gigabyte free is refused before any byte is
  sent. The folder is deleted when the upload completes or fails, and the purge of abandoned
  uploads, run when an upload is opened, only touches folders it made.
