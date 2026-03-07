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
]

templates_path = ['_templates']
exclude_patterns = []

html_static_path = ['_static']
html_title = 'PROTEA'
html_theme = 'shibuya'

autodoc_mock_imports = [
    'yaml', 'pika',
]

master_doc = 'index'
