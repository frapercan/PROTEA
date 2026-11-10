Lend this platform a compute node
=================================

This instance computes what it can on the machine that serves it, and that
machine has no graphics card. Annotating a protein it has already encoded takes
about five seconds; encoding a sequence it has never seen takes about ten on
CPU, and a batch of them takes as long as the arithmetic takes. Nothing is
broken when that happens, and nothing is lost: the work waits in a queue until
somebody runs it.

A compute node is how that gets faster. It is a machine that joins this
platform's queues, takes the heavy stage, computes, and publishes the result
back. **It holds no state.** It can be rebooted without warning and without
telling anyone, which is a property of the design rather than a risk being
tolerated: the server owns the database, the broker and the artifact store, and
a node that disappears mid-batch leaves its message unacknowledged, so the
message returns to the queue and the next node takes it.

What a node is for
~~~~~~~~~~~~~~~~~~

Two stages are worth moving off the server, and they are not the same kind of
work.

``protea.embeddings.batch``
   The protein language model. This is the stage that wants a card: it reads
   residues and writes one vector per sequence. Measured on this server's CPU,
   a 940 residue protein takes 6,8 seconds with the model already in memory;
   the same work on a card is where the whole corpus became feasible at all.

``protea.predictions.batch``
   The nearest-neighbour search and the annotation transfer. **This one stays
   on CPU by decision, not by accident.** The device setting had an "auto"
   default that silently chose the card, and it is the only path that failed
   there. A node serving this queue should leave it on CPU.

A node serves one queue per worker process, and serving only one of the two is
a perfectly good contribution.

What a node needs
~~~~~~~~~~~~~~~~~

* **Linux and Python 3.12.** The platform pins its interpreter and its
  dependencies through ``poetry.lock``, and a node installs from that lock
  rather than from a loose requirements list.
* **The repository at the declared revision.** Not at the tip of a branch: at
  the revision the server declares it is running. See the next section, because
  this is the part a node cannot improvise.
* **Reachability of the broker, the database and the artifact store.** The
  operator gives you the endpoints and one credential. They are not published
  here and they are not sent over chat.
* **A card, if and only if you are serving the embedding stage.** The
  tokeniser and the checkpoint come down from the model hub on first use;
  budget a few gigabytes of disk and a slow first run.

The declared revision is the contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A node must run the same code the server runs, and "the same code" is a
specific commit rather than a branch. The server publishes it in a tracked file
and every worker checks it before it starts:

* a worker started on a revision nobody declared **refuses to start** and stays
  down;
* a worker started while another worker for the same queue is alive **refuses
  to start** and leaves the running one alone.

Both refusals are deliberate and they point the same way: **a silent wrong
consumer is worse than no consumer.** A node running the wrong revision does
not fail visibly. It computes, labels its results with a revision no process
held, and the numbers it produces look exactly like the right ones. That
happened once on a twelve-arm sweep and it is the reason the guard exists.

So the sequence on a node is: read the declared revision, check out exactly
that, install from its lock, then start the worker. If the declaration moves
while a batch is in flight, finish the batch and re-sync before taking the
next one.

What a node must never do
~~~~~~~~~~~~~~~~~~~~~~~~~

* **Run an operation directly.** Every unit of work enters through
  ``POST /v1/jobs`` and leaves a job row with its events. Calling an
  operation's ``execute`` by hand writes database rows with no job to explain
  them, and a measurement whose provenance is a person's memory is not a
  measurement.
* **Declare a set, or write one by hand.** Sets are declared after extraction,
  by the job that extracted them, from what it counted.
* **Take the neighbour search to the card.** See above.
* **Hold state.** If a node needs scratch space it uses its own disk and
  publishes the result through the artifact store, by content digest.

How to ask
~~~~~~~~~~

Contact the author through the support page of this instance. Say which stage
you want to serve and what the machine has. You will be sent the endpoints, one
credential and the current declared revision; the credential arrives by a
channel that is not this page and not a chat message.

If you would rather read before asking, the deployment runbook
(:doc:`/runbooks/deployment`) describes how the server itself is brought up,
and :doc:`/reference/workers` lists every queue with the operations it serves.

What it changes for you
~~~~~~~~~~~~~~~~~~~~~~~

Nothing about how the instrument looks. The same annotation request, submitted
the same way, stops waiting on the server's CPU and starts being computed on
your machine, with its job row, its events and its revision recorded exactly as
before. The only visible difference is how long the page takes to answer.
