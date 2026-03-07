PROTEA
======

**Protein Research and Operations Technology for Enriched Analysis**

PROTEA is the target platform for the progressive consolidation of the
`Protein Information System (PIS) <https://github.com/CBBIO/protein-information-system>`_
and `FANTASIA <https://github.com/CBBIO/FANTASIA>`_ codebases.
It provides a clean, decoupled architecture for large-scale protein data ingestion,
metadata enrichment, and job orchestration.

.. raw:: html

   <div style="margin: 1rem 0;"></div>

.. grid:: 1 2 2 3
   :gutter: 2
   :margin: 2 0 2 0

   .. grid-item-card:: Quickstart
      :link: appendix/installation_and_quickstart
      :link-type: doc
      :shadow: md
      :text-align: left

      Set up PROTEA and run your first job. :bdg:`5 min` :bdg-primary:`Beginner`
      See :doc:`Start here → <appendix/installation_and_quickstart>`.

   .. grid-item-card:: Architecture
      :link: architecture/index
      :link-type: doc
      :shadow: md
      :text-align: left

      System design, job lifecycle, and data model. :bdg-info:`Design` :bdg:`Clean Architecture`
      Explore :doc:`architecture/index`.

   .. grid-item-card:: Operations
      :link: architecture/operations
      :link-type: doc
      :shadow: md
      :text-align: left

      Built-in operations: insert_proteins, fetch_uniprot_metadata, ping. :bdg-success:`UniProt`
      See :doc:`architecture/operations`.

   .. grid-item-card:: API Reference
      :link: reference/index
      :link-type: doc
      :shadow: md
      :text-align: left

      Autodoc-driven reference for all modules. :bdg-secondary:`autodoc`
      Browse :doc:`reference/index`.

   .. grid-item-card:: Configuration
      :link: appendix/configuration
      :link-type: doc
      :shadow: md
      :text-align: left

      YAML and environment-variable settings. :bdg:`system.yaml`
      See :doc:`appendix/configuration`.

   .. grid-item-card:: How-to Guides
      :link: appendix/howto_guides
      :link-type: doc
      :shadow: md
      :text-align: left

      Adding operations, running workers, extending the system. :bdg-warning:`Guides`
      Go to :doc:`appendix/howto_guides`.

.. raw:: html

   <div style="margin: 1.5rem 0;"></div>

.. admonition:: What is PROTEA?
   :class: tip

   A job-orchestration platform for protein data pipelines: clean separation of
   infrastructure, execution flow, and domain logic — designed for incremental
   migration and horizontal scalability.

.. toctree::
   :caption: Documentation
   :maxdepth: 2

   abstract
   introduction
   architecture/index
   appendix/index

.. toctree::
   :caption: API Reference
   :maxdepth: 2
   :hidden:

   reference/index
