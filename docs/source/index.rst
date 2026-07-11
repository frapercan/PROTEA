PROTEA
======

**PROtein functional Embedding-based Annotation**

PROTEA is the target platform for the progressive consolidation of the
`Protein Information System (PIS) <https://github.com/CBBIO/protein-information-system>`_
and `FANTASIA <https://github.com/CBBIO/FANTASIA>`_ codebases.
It provides a clean, decoupled architecture for large-scale protein data ingestion,
metadata enrichment, and job orchestration.

.. raw:: html

   <div style="margin: 1rem 0;"></div>

.. grid:: 1 2 2 2
   :gutter: 3
   :margin: 2 0 2 0

   .. grid-item-card:: Quickstart
      :link: appendix/installation_and_quickstart
      :link-type: doc
      :shadow: md
      :text-align: left

      :bdg-primary:`Start here` Bring up the full stack from a fresh checkout
      and run your first job in about ten minutes.

   .. grid-item-card:: Architecture
      :link: architecture/index
      :link-type: doc
      :shadow: md
      :text-align: left

      :bdg-info:`Design` System layers, job lifecycle, data model, the full
      operation catalogue, the CAFA evaluation protocol, and the ADRs that
      explain *why*.

   .. grid-item-card:: API Reference
      :link: reference/index
      :link-type: doc
      :shadow: md
      :text-align: left

      :bdg-secondary:`autodoc` Symbol-level documentation for ``protea.core``,
      ``protea.infrastructure``, the FastAPI routers, and every worker class.

   .. grid-item-card:: Complexity
      :link: complexity/index
      :link-type: doc
      :shadow: md
      :text-align: left

      :bdg-warning:`Performance` Big-O profile per pipeline stage, measured
      hot paths, and a guide to profiling with scalene and pyinstrument.

   .. grid-item-card:: Results
      :link: results
      :link-type: doc
      :shadow: md
      :text-align: left

      :bdg-danger:`Evidence` The sealed board on the leakage-free temporal
      frame, with the metric definition, scoring recipe, and reproduction
      path cross-referenced from one home.

.. raw:: html

   <div style="margin: 1.5rem 0;"></div>

.. admonition:: What is PROTEA?
   :class: tip

   A platform for protein functional annotation: from sequence ingestion through
   GPU embedding computation (ESM-2, ESM-C, T5/ProstT5, Ankh), a learned k-WTA
   retrieval encoder, KNN candidate generation, and a stacked per-category
   re-ranker, to board-faithful CAFA evaluation, with clean separation of
   infrastructure, execution flow, and domain logic.

   New here? Start with the :doc:`quickstart <appendix/installation_and_quickstart>`,
   then read the sealed board and its evidence in :doc:`results`.

.. toctree::
   :caption: Documentation
   :maxdepth: 2

   overview
   abstract
   introduction
   related_work
   architecture/index
   complexity/index
   guides/plugin-authoring/index
   results
   operate/reproduce-0.4063
   appendix/index
   runbooks/index
   quality/index
   insights
   glossary
   references

.. toctree::
   :caption: API Reference
   :maxdepth: 2
   :hidden:

   reference/index

.. toctree::
   :caption: Historical (superseded)
   :maxdepth: 1
   :hidden:

   historical/index
