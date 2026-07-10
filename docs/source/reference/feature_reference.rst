Feature reference
=================

The re-ranker reads a fixed set of feature columns per candidate protein/GO
pair. This page answers, for the technician who deploys and operates the
stack, what each column *measures*, *who produces it*, and *whether that
producer actually runs in the default export today*. Where the API
:doc:`core` reference documents the code that reads these columns, this page
documents the columns themselves.

The whole table below is generated at build time from
:data:`protea_contracts.feature_docs.FEATURE_DOCS`, the single feature
registry shared across the PROTEA stack. It is never written by hand: edit a
:class:`~protea_contracts.feature_docs.FeatureDoc` in ``protea-contracts`` and
this page follows on the next build. A drift lint,
``scripts/check_feature_docs.py``, fails PROTEA CI if a declared feature loses
its doc, if a doc names an undeclared column, or if a doc's family disagrees
with the canonical schema, so the reference cannot silently fall out of sync
with the fingerprinted feature set.

How to read a row
-----------------

Each column carries a **status** that tells the operator whether the value it
sees in an export is a real signal or a placeholder:

:PRODUCED:
   A wired producer fills the column with a real value in the export. Some
   producers sit behind a performance flag that the canonical export enables;
   read the feature's notes. A ``PRODUCED`` status means the *producer runs*,
   not that the value is non-trivial in a given deployment: a producer whose
   database source is empty (see ``interpro_*``) still keeps its declared
   default, and the notes say so.
:DECLARED_ABSENT:
   The column is a first-class member of the schema and a producer exists, but
   no producer runs in the default export, so the export emits ``NaN`` and
   LightGBM reads it as missing. The six LAFA columns are in this state per
   ADR-D45.
:POOL_INJECTED:
   The PROTEA dump does not write the column at all; the lab's pooled
   multi-manifest loader injects it as a per-source constant at stage time
   (for example ``plm_id`` and ``k_context``). It is absent from the raw
   parquet dumps.
:BROKEN:
   The column is produced but a defect that can be pointed at in code or data
   makes it carry no signal. Used only where that defect is verifiable from
   the source tree.

The **producer** field names the exact callable (or the lab loader) that fills
the column, so an operator can trace a suspect value back to the code path
that wrote it. The **notes** field is where an operator learns the operational
caveats: that the ``interpro_*`` columns keep their zero default whenever the
InterPro GO-prediction table is unset for the deployment, and that
``plm_id``/``k_context`` never appear in a raw dump because the lab injects
them.

The registry
------------

.. feature-docs-table::

Programmatic access
-------------------

The same registry is importable at runtime for tooling that needs the
machine-readable form:

.. code-block:: python

   from protea_contracts.feature_docs import FEATURE_DOCS, FeatureStatus

   doc = FEATURE_DOCS["interpro_hit"]
   print(doc.status, doc.producer)
