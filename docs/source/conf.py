import os
import sys

sys.path.insert(0, os.path.abspath('../..'))

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
