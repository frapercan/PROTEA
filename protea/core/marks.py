"""The mark: the last GOA release the universe may read, and the UniProt ceiling.

The campaign's test is the 227->230 window. It was read once, on 2026-10-27,
and a test can be read only once: that read spent it. It stays untouchable for
good, and rebuilding the data from public sources does not restore it
(ADR-D60). So nothing published after the cut it opens may reach the universe,
a model fitted in it or a choice made with it:

- ``MARK_RELEASE`` is that cut, the GOA release of the 227 instance. A frame may
  end at the mark; no cut, photo or source may come after it.
- ``UNIPROT_CEILING`` is the first UniProt release nothing in the universe may
  read. Cut 227 pairs with UniProt 2025_03 by this mark (the author's decision
  recorded in ADR-D60), so no release from 2025_04 on is read, whatever its
  publication date.

Every module that needs either value imports it from here, and
``tests/test_the_mark_is_stated_once.py`` fails when another module restates
it. This module imports nothing, so the code that runs away from the server
(the photo builder of ADR-D59) reads the same values without the database
stack.
"""

MARK_RELEASE = 227

UNIPROT_CEILING = "2025_04"
