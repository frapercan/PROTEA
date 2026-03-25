ADR-006: Sequence deduplication by MD5
======================================

:Date: 2025-12-10
:Author: frapercan

The problem
-----------

UniProt has ~570K accessions in Swiss-Prot, but only ~540K unique sequences.
The remaining 30K are isoforms or cross-references sharing the same amino
acid chain.

Computing the embedding for a sequence costs ~0.5s on GPU.  Processing 30K
duplicates wastes **4+ hours** per full run.

What we do
----------

When inserting proteins, we compute the MD5 hash of the amino acid string.
The ``Sequence`` table has a **unique constraint on ``sequence_hash``**:

1. If the hash already exists -> reuse the existing ``Sequence.id``.
2. If it does not exist -> insert a new row.

Multiple ``Protein`` rows (one per UniProt accession) point to the same
``Sequence``.  The FK ``Protein.sequence_id`` is intentionally non-unique.

When the embedding pipeline runs, it only processes ``Sequence`` rows
without an embedding — duplicates are skipped automatically.

Trade-offs
----------

- MD5 is not cryptographically secure, but that does not matter here:
  there is no adversarial input, only biological sequences.
- Sequences with a single mutation produce different hashes and are stored
  separately.  This is correct — a mutation changes the embedding.

Rejected
--------

- **SHA-256**: digest twice as long, zero practical benefit.
- **UNIQUE on the sequence text column**: indexing multi-kilobyte text
  columns is expensive; the 32-char hex digest is far more efficient.
- **CD-HIT clustering** (90-95% identity): useful for reducing redundancy
  in evolutionary analysis, but here we need exact deduplication (100%).
