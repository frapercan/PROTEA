// Which of the ten stored metrics the benchmark is showing.
//
// The surface used to print one number per cell with no label. Two independent
// axes decide what that number means, and the same cell can read 0.4836 or
// 0.3476 depending on which you hold:
//
//   fmax      macro: the per-protein score averaged over proteins, so a
//             protein with one annotation weighs as much as one with fifty
//   f_micro   micro: tp / fp / fn summed over the population, then divided
//   _w        information-accretion weighted, which both boards use
//
// The two boards differ on the AVERAGE, not on the weighting, and this file
// used to say otherwise. CAFA scores fmax_w, per protein. LAFA scores
// f_micro_w, pooled. A number is comparable to one of them, never both.
//
// The choice lives in the URL so a link carries it: sending someone a
// leaderboard that silently renders under their own last selection is how two
// people end up arguing about different numbers.

"use client";

import { METRICS, metricLabel, metricSpec, metricGroups } from "@/lib/metrics";

type Props = {
  value: string;
  onChange: (key: string) => void;
  /** Keys actually present in the loaded rows; others render disabled. */
  available?: Set<string>;
};

export function MetricSelector({ value, onChange, available }: Props) {
  const spec = metricSpec(value);
  const has = (key: string) => !available || available.has(key);

  return (
    <div className="flex flex-col gap-1">
      <div className="flex items-center gap-2">
        <label
          htmlFor="metric-select"
          className="text-[10px] font-semibold uppercase tracking-wider text-slate-400"
        >
          metric
        </label>
        <select
          id="metric-select"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="rounded-md border border-slate-300 bg-white px-2 py-1 text-[12px] text-slate-800"
        >
          {metricGroups().map((group) => (
            <optgroup key={group.title} label={group.title}>
              {group.keys.map((key) => (
                <option key={key} value={key} disabled={!has(key)}>
                  {metricLabel(key)}
                  {has(key) ? "" : " (not in these rows)"}
                </option>
              ))}
            </optgroup>
          ))}
        </select>
      </div>
      {spec ? (
        // Always spelled out, never left to the suffix. A reader who cannot
        // see whether a number is macro or micro, weighted or plain, cannot
        // compare it with anything.
        <p className="max-w-xl text-[11px] leading-snug text-slate-600">
          <span className="font-medium text-slate-700">
            {spec.averaging ? `${spec.averaging}-averaged` : "per-threshold"},{" "}
            {spec.weighting === "ia" ? "IA-weighted" : "unweighted"}
          </span>
          {". "}
          {spec.meaning}
        </p>
      ) : null}
    </div>
  );
}

/** Metric keys present in at least one row, for disabling the rest. */
export function availableMetrics(rows: Record<string, unknown>[]): Set<string> {
  const out = new Set<string>();
  for (const key of METRICS.map((m) => m.key)) {
    if (rows.some((r) => r[key] !== undefined && r[key] !== null)) out.add(key);
  }
  return out;
}
