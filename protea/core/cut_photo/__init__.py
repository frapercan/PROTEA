"""Cut photos (ADR-D59): the corpus of one cut, built the same way at every cut.

A photo of cut ``t`` is three member sets taken from sources dated on or before
``t``: the Swiss-Prot of the paired UniProt release (the set the cut is asked to
annotate), ADR-D49's two live tiers, and the UniRef50 neighbours of the
superset. Every GOA row of a member is kept; which rows a recipe uses is a filter
on a declared channel table, never a property of the stored data.

The package is the half that runs away from the server. It imports the standard
library, ``pyarrow`` and the pure platform modules it needs
(``protea.core.evidence_codes``, ``protea.core.marks``,
``protea.core.source_guards`` and ``protea.core.swissprot_table``), so a compute
node can run it from a bare checkout with no database stack.

- :mod:`.formats`: the GAF row, the UniRef XML member and the reference field.
- :mod:`.measure`: step 1 of ADR-D59, the inventory of a cut before any photo is
  built.
"""
