import os
import sys

sys.path.insert(0, os.path.abspath('../..'))
sys.path.insert(0, os.path.abspath('_ext'))

project = 'PROTEA'
copyright = '2025, frapercan'
author = 'frapercan'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
    'sphinx_copybutton',
    'sphinx_design',
    'sphinxcontrib.bibtex',
    'feature_docs_table',
]

# sphinxcontrib-bibtex configuration
bibtex_bibfiles = ['references.bib']
bibtex_default_style = 'alpha'
bibtex_reference_style = 'author_year'

templates_path = ['_templates']
exclude_patterns = []

html_static_path = ['_static']
html_title = 'PROTEA'
html_theme = 'shibuya'

# Stamp every page with the moment it was built.
#
# On 2026-08-23 the published site was found to have been built on 29 July. A
# month of merged work had never been public, and nothing anywhere said so: the
# pages rendered, the processes were healthy, the endpoints returned 200. A
# staleness that produces no error is invisible until somebody goes looking for
# a page that should exist and finds a 404.
#
# A date in the footer is the cheapest thing that makes it visible without
# anyone going looking. `deploy-check.sh` asks the same question in a form that
# can fail a check; this one asks it of the reader.
html_last_updated_fmt = '%Y-%m-%d %H:%M'

autodoc_mock_imports = [
    'yaml',
    'pika',
    'torch',
    'transformers',
    'faiss',
    'parasail',
    'ete3',
    'lightgbm',
    'cafaeval',
    'numpy',
    'pandas',
    'pyarrow',
    'scipy',
    'sklearn',
    'aio_pika',
    'minio',
    'opentelemetry',
    'prometheus_client',
    'pgvector',
]

master_doc = 'index'
