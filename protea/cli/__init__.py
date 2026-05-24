"""PROTEA command-line interface package.

Entry points are registered in ``pyproject.toml`` under
``[tool.poetry.scripts]``. Sub-commands live in sibling modules:

* :mod:`protea.cli.admin` — ``protea-cli admin`` group (user management,
  bootstrap helpers).
"""
