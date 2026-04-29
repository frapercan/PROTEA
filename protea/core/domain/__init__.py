"""Domain types shared across the core layer.

These are pure value objects (enums, dataclasses) with no dependencies
on infrastructure (DB, queue, FS). They're safe to import from anywhere
in ``protea/`` without creating cycles.
"""
