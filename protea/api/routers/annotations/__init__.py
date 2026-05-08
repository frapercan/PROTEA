"""Annotations router package — aggregates the four endpoint subrouters
(snapshots / sets / evaluation_sets / evaluation_results) into one
``/annotations``-prefixed APIRouter.

The split was driven by the F2D services-layer AC (router files
< 400 LOC). Keeping every endpoint in one ``annotations.py`` blew
that ceiling well past 600 LOC even after every body had been
extracted to ``annotations_service``.
"""

from __future__ import annotations

from fastapi import APIRouter

from . import evaluation_results, evaluation_sets, sets, snapshots

router = APIRouter(prefix="/annotations", tags=["annotations"])
router.include_router(snapshots.router)
router.include_router(sets.router)
router.include_router(evaluation_sets.router)
router.include_router(evaluation_results.router)

__all__ = ["router"]
