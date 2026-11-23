"use client";

// The paired comparisons of a window's results against one baseline, per cell.
//
// Each cell is the arm's f_micro_w minus the baseline's, with the interval of a
// paired bootstrap over proteins (compare_paired_panels, read from its job). A
// cell where either side's headline peaks at an edge of the threshold grid is
// marked, since no verdict is read from an edge (ADR-D55).

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useTranslations } from "next-intl";
import { baseUrl } from "@/lib/api";
import { edgeMark, headlineMaximum, type Maximum } from "@/lib/headlineMaximum";
import {
  armLabel,
  baselinesOf,
  deltaText,
  panelNote,
  verdictTone,
  type Arm,
  type PairedComparison,
} from "@/lib/pairedComparison";

const CATEGORIES = ["NK", "LK", "PK"] as const;
const NAMESPACES = ["BPO", "MFO", "CCO"] as const;

/** What this section reads of a window's result. */
export type ComparedResult = {
  id: string;
  prediction_set_id: string;
  score_reading: string | null;
  arm?: Arm | null;
  results: Record<string, unknown>;
};

type Cell = { maxima?: Record<string, Maximum | undefined> };

function cellOf(r: ComparedResult | undefined, cat: string, ns: string): Cell | undefined {
  const setting = r?.results?.[cat] as Record<string, Cell> | undefined;
  return setting?.[ns];
}

function resultLabel(r: ComparedResult | undefined, id: string): string {
  if (!r) return id.slice(0, 8);
  const reading = r.score_reading === "percentile_rank" ? "percentile" : (r.score_reading ?? "");
  return [armLabel(r.arm) ?? r.prediction_set_id.slice(0, 8), reading, r.id.slice(0, 8)]
    .filter(Boolean)
    .join(" · ");
}

const TONE = {
  higher: "text-emerald-700",
  lower: "text-rose-700",
  unresolved: "text-slate-500",
} as const;

export default function PairedComparisons({
  evaluationSetId,
  results,
  locale,
}: {
  evaluationSetId: string;
  results: ComparedResult[];
  locale: string;
}) {
  const t = useTranslations("evaluation");
  const [comparisons, setComparisons] = useState<PairedComparison[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [baseline, setBaseline] = useState<string>("");

  useEffect(() => {
    let cancelled = false;
    fetch(`${baseUrl()}/annotations/evaluation-sets/${evaluationSetId}/paired-comparisons`, {
      cache: "no-store",
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<PairedComparison[]>;
      })
      .then((rows) => {
        if (!cancelled) setComparisons(rows);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [evaluationSetId]);

  const byId = useMemo(() => new Map(results.map((r) => [r.id, r])), [results]);
  const baselines = useMemo(() => baselinesOf(comparisons ?? []), [comparisons]);
  const chosen = baseline || baselines[0] || "";
  const rows = (comparisons ?? []).filter((c) => c.baseline_evaluation_result_id === chosen);

  return (
    <section className="mt-6 rounded-lg border bg-white shadow-sm" id="paired-comparisons">
      <header className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-600">
          {t("pairedTitle")}
        </h2>
      </header>
      {error ? (
        <p className="px-4 py-3 text-xs text-rose-600">{error}</p>
      ) : comparisons === null ? null : baselines.length === 0 ? (
        <p className="px-4 py-6 text-center text-sm text-slate-600">{t("pairedNone")}</p>
      ) : (
        <>
          <p className="border-b px-4 py-2 text-xs text-slate-600">{t("pairedLegend")}</p>
          <div className="flex flex-wrap items-center gap-2 border-b px-4 py-2 text-xs">
            <label htmlFor="paired-baseline" className="text-slate-600">
              {t("pairedBaseline")}
            </label>
            <select
              id="paired-baseline"
              value={chosen}
              onChange={(e) => setBaseline(e.target.value)}
              className="rounded-md border bg-white px-2 py-1 font-mono text-xs text-slate-700"
            >
              {baselines.map((b) => (
                <option key={b} value={b}>
                  {resultLabel(byId.get(b), b)}
                </option>
              ))}
            </select>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b bg-slate-50 font-semibold uppercase tracking-wide text-slate-600">
                  <th className="px-3 py-2 text-left">arm</th>
                  {CATEGORIES.flatMap((cat) =>
                    NAMESPACES.map((ns) => (
                      <th key={`${cat}-${ns}`} className="px-2 py-2 text-right">
                        {cat}/{ns}
                      </th>
                    )),
                  )}
                </tr>
              </thead>
              <tbody>
                {rows.map((c) => {
                  const arm = byId.get(c.evaluation_result_id);
                  const base = byId.get(c.baseline_evaluation_result_id);
                  return (
                    <tr key={c.job_id} className="border-b text-slate-700">
                      <td className="px-3 py-2 font-mono">
                        <div className="flex flex-col gap-0.5">
                          <span>{resultLabel(arm, c.evaluation_result_id)}</span>
                          <Link
                            href={`/${locale}/instrument/jobs/${c.job_id}`}
                            className="text-[10px] text-blue-600 hover:underline"
                            title={c.job_id}
                          >
                            job {c.job_id.slice(0, 8)}
                          </Link>
                        </div>
                      </td>
                      {CATEGORIES.flatMap((cat) =>
                        NAMESPACES.map((ns) => {
                          const panel = c.panels[`${cat}:${ns}`];
                          if (!panel) {
                            return (
                              <td key={`${c.job_id}-${cat}-${ns}`} className="px-2 py-2 text-right">
                                {"—"}
                              </td>
                            );
                          }
                          const edge =
                            edgeMark(headlineMaximum(cellOf(arm, cat, ns) ?? {})) ||
                            edgeMark(headlineMaximum(cellOf(base, cat, ns) ?? {}));
                          return (
                            <td
                              key={`${c.job_id}-${cat}-${ns}`}
                              className={`px-2 py-2 text-right font-mono tabular-nums ${TONE[verdictTone(panel)]}`}
                              title={panelNote(panel, c)}
                            >
                              {deltaText(panel)}
                              {edge ? <span className="text-amber-700"> {edge}</span> : null}
                            </td>
                          );
                        }),
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}
