"""Query all EvaluationResult rows and compare PK metrics across configurations."""

from pathlib import Path

from sqlalchemy.orm import joinedload

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.session import build_session_factory, session_scope
from protea.infrastructure.settings import load_settings

PROJECT_ROOT = Path(__file__).resolve().parent.parent
settings = load_settings(PROJECT_ROOT)
factory = build_session_factory(settings.db_url)

with session_scope(factory) as session:
    results = (
        session.query(EvaluationResult)
        .options(
            joinedload(EvaluationResult.prediction_set),
            joinedload(EvaluationResult.scoring_config),
            joinedload(EvaluationResult.reranker_model),
        )
        .order_by(EvaluationResult.created_at)
        .all()
    )

    if not results:
        print("No EvaluationResult rows found.")
        raise SystemExit(0)

    # Header
    header = (
        f"{'eval_id':>8s}  "
        f"{'pred_set_id':>11s}  "
        f"{'K':>5s}  "
        f"{'scoring_config':>40s}  "
        f"{'reranker':>30s}  "
        f"{'PK/BPO Fmax':>11s}  "
        f"{'PK/MFO Fmax':>11s}  "
        f"{'PK/CCO Fmax':>11s}  "
        f"{'NK/BPO Fmax':>11s}  "
        f"{'NK/MFO Fmax':>11s}  "
        f"{'NK/CCO Fmax':>11s}  "
        f"{'LK/BPO Fmax':>11s}  "
        f"{'LK/MFO Fmax':>11s}  "
        f"{'LK/CCO Fmax':>11s}"
    )
    print(header)
    print("-" * len(header))

    for er in results:
        ps = er.prediction_set
        k_val = str(ps.limit_per_entry) if ps else "?"
        pred_id = str(ps.id)[:8] if ps else "?"
        eval_id = str(er.id)[:8]

        sc_name = er.scoring_config.name if er.scoring_config else "(none)"
        sc_formula = er.scoring_config.formula if er.scoring_config else ""
        sc_label = f"{sc_name} [{sc_formula}]" if sc_formula else sc_name

        rr_name = er.reranker_model.name if er.reranker_model else "(none)"

        r = er.results or {}

        def fmax(cat: str, ns: str, _r: dict = r) -> str:
            val = _r.get(cat, {}).get(ns, {}).get("fmax")
            if val is None:
                return "-"
            return f"{val:.4f}"

        print(
            f"{eval_id:>8s}  "
            f"{pred_id:>11s}  "
            f"{k_val:>5s}  "
            f"{sc_label:>40s}  "
            f"{rr_name:>30s}  "
            f"{fmax('PK','BPO'):>11s}  "
            f"{fmax('PK','MFO'):>11s}  "
            f"{fmax('PK','CCO'):>11s}  "
            f"{fmax('NK','BPO'):>11s}  "
            f"{fmax('NK','MFO'):>11s}  "
            f"{fmax('NK','CCO'):>11s}  "
            f"{fmax('LK','BPO'):>11s}  "
            f"{fmax('LK','MFO'):>11s}  "
            f"{fmax('LK','CCO'):>11s}"
        )

    # Summary: group by K and show PK averages
    print("\n\n=== PK Fmax Summary by K value ===\n")
    from collections import defaultdict
    by_k: dict[int, list] = defaultdict(list)
    for er in results:
        ps = er.prediction_set
        if not ps:
            continue
        k = ps.limit_per_entry
        pk = (er.results or {}).get("PK", {})
        by_k[k].append(pk)

    for k in sorted(by_k.keys()):
        entries = by_k[k]
        n = len(entries)
        for ns in ("BPO", "MFO", "CCO"):
            vals = [e.get(ns, {}).get("fmax") for e in entries if e.get(ns, {}).get("fmax") is not None]
            if vals:
                avg = sum(vals) / len(vals)
                best = max(vals)
                worst = min(vals)
                print(f"  K={k:>3d}  {ns}  n={len(vals):>3d}  avg={avg:.4f}  best={best:.4f}  worst={worst:.4f}")
            else:
                print(f"  K={k:>3d}  {ns}  n=  0  (no data)")
