// Method-surface provenance badge strip (slice F-METHOD-EVAL-SURFACE).
//
// Renders the four optional EvaluationResult provenance markers as a
// compact, tasteful badge row: a generation badge (which recompute of the
// cell this number is, from the self-hit audit), a frame badge (LAFA-frame
// vs internal),
// an explicit train/SELECT/FINAL window label, per-arm chips (KNN /
// Reranker / MLP / InterPro), and a leakage-hygiene marker
// (select / test / probe). Every field degrades to an explicit "unknown"
// chip when it predates the provenance columns, so legacy rows render a
// stable, non-blank state instead of vanishing.
//
// Pure presentation over the resolvers in lib/evalProvenance.ts; no data
// fetching, so it never blocks render.

import {
  armsList,
  frameBadge,
  generationBadge,
  hasAnyProvenance,
  leakageBadge,
  windowLabel,
  type EvalProvenance,
} from "@/lib/evalProvenance";

const CHIP = "inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-[10px] font-medium";

const ARM_ON = "bg-blue-50 text-blue-800 ring-1 ring-inset ring-blue-200";
const ARM_OFF = "bg-slate-50 text-slate-400 ring-1 ring-inset ring-slate-200 line-through";
// An arm the record does not mention. Struck through would claim it did not
// contribute; this says nobody looked. The two used to render identically,
// which is the whole reason the distinction now exists.
const ARM_UNKNOWN = "bg-white text-slate-400 ring-1 ring-dashed ring-slate-300 italic";

type Props = {
  provenance: EvalProvenance | null | undefined;
  /** When true, render nothing if every field is unknown (used in dense
   *  tables where an all-unknown strip would just be noise). Defaults to
   *  false so standalone placements always show the explicit empty state. */
  hideWhenEmpty?: boolean;
  className?: string;
};

/** Compact provenance strip for one EvaluationResult. */
export function EvalProvenanceBadges({
  provenance,
  hideWhenEmpty = false,
  className = "",
}: Props) {
  if (hideWhenEmpty && !hasAnyProvenance(provenance)) return null;

  const p = provenance ?? {};
  const frame = frameBadge(p.frame);
  const win = windowLabel(p.temporal_window);
  const role = leakageBadge(p.leakage_role);
  const arms = armsList(p.arms_enabled);
  // Placed first: which generation a number came from decides whether the rest
  // of the strip is worth reading at all.
  const generation = generationBadge(p.prediction_set_status, p.self_hit_rate);

  return (
    <div
      className={`flex flex-wrap items-center gap-1 ${className}`}
      data-testid="eval-provenance-badges"
    >
      {generation ? (
        <span
          className={`${CHIP} ${generation.className}`}
          title={generation.title}
          data-testid="generation-badge"
        >
          {generation.label}
        </span>
      ) : null}
      <span className={`${CHIP} ${frame.className}`} title={frame.title}>
        {frame.label}
      </span>
      <span className={`${CHIP} ${win.className}`} title={win.title}>
        {win.label}
      </span>
      <span className={`${CHIP} ${role.className}`} title={role.title}>
        {role.label}
      </span>
      {arms === null ? (
        <span
          className={`${CHIP} bg-slate-50 text-slate-400 ring-1 ring-inset ring-slate-200`}
          title="Method-arm composition not recorded for this result."
        >
          arms ?
        </span>
      ) : (
        <span className="inline-flex flex-wrap items-center gap-0.5" aria-label="Method arms">
          {arms.map((a) => (
            <span
              key={a.key}
              className={`${CHIP} ${
                a.enabled === null ? ARM_UNKNOWN : a.enabled ? ARM_ON : ARM_OFF
              }`}
              title={
                a.enabled === null
                  ? `${a.label} arm not recorded for this evaluation. Not the same as off: the run did not inspect this arm, so the record says nothing about it.`
                  : `${a.label} arm ${a.enabled ? "enabled" : "disabled"} for this evaluation.`
              }
            >
              {a.label}
              {a.enabled === null ? "?" : null}
            </span>
          ))}
        </span>
      )}
    </div>
  );
}
