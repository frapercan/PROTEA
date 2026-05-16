PROTEA
======

**PROtein funcTional Embedding-based Annotation**

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

      :bdg-info:`Design` System layers, job lifecycle, data model, all 15
      operations, the CAFA evaluation protocol, and the ADRs that explain *why*.

   .. grid-item-card:: API Reference
      :link: reference/index
      :link-type: doc
      :shadow: md
      :text-align: left

      :bdg-secondary:`autodoc` Symbol-level documentation for ``protea.core``,
      ``protea.infrastructure``, the FastAPI routers, and every worker class.

   .. grid-item-card:: Results
      :link: results
      :link-type: doc
      :shadow: md
      :text-align: left

      :bdg-danger:`Evaluation` Benchmark numbers, ablation studies, the
      re-ranker training pipeline, and the figures that back the thesis.

.. raw:: html

   <div style="margin: 1.5rem 0;"></div>

.. admonition:: What is PROTEA?
   :class: tip

   A platform for protein functional annotation: from sequence ingestion through
   GPU embedding computation (ESM-2, ESM-C, T5/ProstT5, Ankh) and KNN-based GO
   term prediction to CAFA evaluation and LightGBM re-ranking, with clean
   separation of infrastructure, execution flow, and domain logic.

.. toctree::
   :caption: Documentation
   :maxdepth: 2

   abstract
   introduction
   related_work
   architecture/index
   plugin-authoring
   guides/plugin-authoring/index
   results
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
