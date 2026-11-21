"use client";

// A window's panel: hundreds of configurations of one window, read side by side.
//
// One summary (GET /annotations/evaluation-sets/{id}/summary) feeds five views:
// the cells of every arm against a reference, the depth of the neighbours read,
// the trajectory of the best result, the strata by identity and length, and the
// detail table. The window's paired comparisons (GET .../paired-comparisons) say
// whether each new best in the trajectory settled its step: over the whole cell,
// read with the summary, or on one identity band, read when the band is shown. The state lives in the address (``usePanelState``), so a view
// is shared by its link and the back button undoes the last navigation.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import { baseUrl } from "@/lib/api";
import type { PairedComparison } from "@/lib/pairedComparison";
import { listOf, toggled, toggledFocus, usePanelState, type PanelPatch, type PanelState } from "@/lib/panelState";
import {
  DEPTHS,
  NAMESPACES,
  SCORINGS,
  SCORING_SHORT,
  SETTINGS,
  armLabel,
  modelsOf,
  referenceOf,
  visibleResults,
  type SummaryResult,
  type WindowSummary,
} from "@/lib/windowPanel";
import { FOCUS } from "./chart";
import DepthView from "./DepthView";
import DetailView from "./DetailView";
import StrataView from "./StrataView";
import SummaryView from "./SummaryView";
import TrajectoryView from "./TrajectoryView";

export type PanelView = {
  state: PanelState;
  commit: (patch: PanelPatch, options?: { push?: boolean }) => void;
  results: SummaryResult[];
  shown: SummaryResult[];
  reference: SummaryResult | undefined;
  focus: SummaryResult[];
  focusColour: (r: SummaryResult) => string | null;
  toggleFocus: (r: SummaryResult) => void;
  open: (r: SummaryResult, cell: string) => void;
  /** The window's paired comparisons: null while they are read, ``"failed"`` if they could not be. */
  comparisons: PairedComparison[] | null | "failed";
  /** The comparisons restricted to each identity band asked for so far, by band. */
  bandComparisons: Record<string, PairedComparison[] | "failed" | "loading">;
  /** Read a band's comparisons, once. */
  loadBand: (band: string) => void;
  locale: string;
};

/** The same arm under the reading on show, so a focus survives a change of reading. */
function counterpart(r: SummaryResult, results: SummaryResult[], reading: string): SummaryResult | undefined {
  if (r.arm.reading === reading) return r;
  const same = (a: SummaryResult["arm"], b: SummaryResult["arm"]) =>
    a.model === b.model &&
    a.scoring === b.scoring &&
    a.method === b.method &&
    a.depth === b.depth &&
    a.exclude_self === b.exclude_self;
  return results.find((x) => x.arm.reading === reading && same(x.arm, r.arm));
}

const TABS = ["summary", "depth", "trajectory", "strata", "detail"] as const;

export default function WindowPanel({ evaluationSetId }: { evaluationSetId: string }) {
  const t = useTranslations("windowPanel");
  const locale = useLocale();
  const [state, commit] = usePanelState();
  const [summary, setSummary] = useState<WindowSummary | null>(null);
  const [comparisons, setComparisons] = useState<PairedComparison[] | null | "failed">(null);
  const [bandComparisons, setBandComparisons] = useState<Record<string, PairedComparison[] | "failed" | "loading">>({});
  const [error, setError] = useState<string | null>(null);
  const [filtersOpen, setFiltersOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    fetch(`${baseUrl()}/annotations/evaluation-sets/${evaluationSetId}/summary`, { cache: "no-store" })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<WindowSummary>;
      })
      .then((s) => !cancelled && setSummary(s))
      .catch((e) => !cancelled && setError(e instanceof Error ? e.message : String(e)));
    fetch(`${baseUrl()}/annotations/evaluation-sets/${evaluationSetId}/paired-comparisons`, { cache: "no-store" })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<PairedComparison[]>;
      })
      .then((c) => !cancelled && setComparisons(c))
      .catch(() => !cancelled && setComparisons("failed"));
    return () => {
      cancelled = true;
    };
  }, [evaluationSetId]);

  const loadBand = useCallback(
    (band: string) => {
      if (band in bandComparisons) return;
      setBandComparisons((current) => ({ ...current, [band]: "loading" }));
      const query = `axis=homology&band=${encodeURIComponent(band)}`;
      fetch(`${baseUrl()}/annotations/evaluation-sets/${evaluationSetId}/paired-comparisons?${query}`, { cache: "no-store" })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          return res.json() as Promise<PairedComparison[]>;
        })
        .then((list) => setBandComparisons((current) => ({ ...current, [band]: list })))
        .catch(() => setBandComparisons((current) => ({ ...current, [band]: "failed" })));
    },
    [evaluationSetId, bandComparisons],
  );

  const view = useMemo<PanelView | null>(() => {
    if (!summary) return null;
    const results = summary.results;
    const shown = visibleResults(results, {
      reading: state.reading,
      superseded: state.superseded === "1",
      models: listOf(state.models),
      scorings: listOf(state.scorings),
      depths: listOf(state.depths).map(Number),
    });
    const byId = new Map(results.map((r) => [r.id, r]));
    const focus = listOf(state.focus)
      .map((id) => byId.get(id))
      .map((r) => (r ? counterpart(r, results, state.reading) : undefined))
      .filter((r): r is SummaryResult => r != null);
    const focusColour = (r: SummaryResult) => {
      const i = focus.indexOf(r);
      return i >= 0 ? FOCUS[i] : null;
    };
    const toggleFocus = (r: SummaryResult) => {
      const current = listOf(state.focus).filter((id) => {
        const x = byId.get(id);
        return x && counterpart(x, results, state.reading)?.id !== r.id;
      });
      const had = current.length !== listOf(state.focus).length;
      commit({ focus: had ? current.join(",") : toggledFocus(current.join(","), r.id) }, { push: true });
    };
    const open = (r: SummaryResult, cell: string) =>
      commit({ tab: "strata", cell, band: "", focus: toggledFocus(listOf(state.focus).filter((x) => x !== r.id).join(","), r.id) }, { push: true });
    return {
      state,
      commit,
      results,
      shown,
      reference: referenceOf(shown, state.ref || null),
      focus,
      focusColour,
      toggleFocus,
      open,
      comparisons,
      bandComparisons,
      loadBand,
      locale,
    };
  }, [summary, comparisons, bandComparisons, loadBand, state, commit, locale]);

  if (error) {
    return <p className="rounded-lg border border-rose-200 bg-rose-50 p-4 text-sm text-rose-800">{t("error", { message: error })}</p>;
  }
  if (!summary || !view) {
    return <p className="rounded-lg border bg-white p-4 text-sm text-slate-500">{t("loading")}</p>;
  }
  if (!summary.results.length) {
    return <p className="rounded-lg border bg-white p-4 text-sm text-slate-500">{t("empty")}</p>;
  }

  const models = modelsOf(summary.results);
  const active = listOf(state.models).length + listOf(state.scorings).length + listOf(state.depths).length;
  const pool = summary.results.filter((r) => r.arm.reading === state.reading && (state.superseded === "1" || r.arm.exclude_self !== false));
  const chip = (key: "models" | "scorings" | "depths", value: string, label: string) => {
    const on = listOf(state[key]).includes(value);
    return (
      <button
        key={value}
        type="button"
        aria-pressed={on}
        onClick={() => commit({ [key]: toggled(state[key], value) })}
        className={`rounded-full border px-2.5 py-0.5 text-xs ${on ? "border-blue-700 bg-blue-50 font-semibold text-blue-800" : "border-slate-200 bg-white text-slate-700"}`}
      >
        {label}
      </button>
    );
  };

  return (
    <section className="mb-6 grid min-w-0 gap-3" aria-label={t("title")}>
      <div className="sticky top-0 z-10 grid gap-2 border-b bg-stone-50/95 py-2.5 backdrop-blur">
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2">
          <div className="flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("cell")}</span>
            <div role="group" aria-label={t("cell")} className="grid grid-cols-[auto_repeat(3,auto)] items-center gap-x-1 gap-y-0.5">
              <span />
              {NAMESPACES.map((n) => (
                <span key={n} className="px-1 text-center text-[10px] text-slate-500">{n}</span>
              ))}
              {SETTINGS.map((s) => [
                <span key={s} className="pr-1 text-[10px] text-slate-500">{s}</span>,
                ...NAMESPACES.map((n) => {
                  const cell = `${s}:${n}`;
                  const on = state.cell === cell;
                  return (
                    <button
                      key={cell}
                      type="button"
                      aria-pressed={on}
                      onClick={() => commit({ cell, band: "" }, { push: true })}
                      className={`rounded border px-1.5 text-[11px] ${on ? "border-blue-800 bg-blue-800 font-semibold text-white" : "border-slate-200 bg-white text-slate-700"}`}
                    >
                      {n}
                    </button>
                  );
                }),
              ])}
            </div>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("reading")}</span>
            <div role="group" className="inline-flex overflow-hidden rounded-md border bg-white">
              {(["percentile_rank", "raw"] as const).map((r) => (
                <button
                  key={r}
                  type="button"
                  aria-pressed={state.reading === r}
                  onClick={() => commit({ reading: r, ref: "" }, { push: true })}
                  className={`px-2.5 py-1 text-xs ${state.reading === r ? "bg-blue-50 font-semibold text-blue-800" : "text-slate-700"}`}
                >
                  {r === "raw" ? t("readingRaw") : t("readingPercentile")}
                </button>
              ))}
            </div>
          </div>
          <label className="flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("reference")}</span>
            <select
              value={view.reference?.id ?? ""}
              onChange={(e) => commit({ ref: e.target.value })}
              className="max-w-[18rem] rounded-md border bg-white px-2 py-1 text-xs"
            >
              {pool.map((r) => (
                <option key={r.id} value={r.id}>{armLabel(r)}</option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-1.5 text-xs text-slate-600">
            <input type="checkbox" checked={state.superseded === "1"} onChange={(e) => commit({ superseded: e.target.checked ? "1" : "0" })} />
            {t("superseded")}
          </label>
        </div>
        <details open={active > 0 || filtersOpen} onToggle={(e) => setFiltersOpen((e.target as HTMLDetailsElement).open)}>
          <summary className="cursor-pointer text-xs text-slate-500">
            {active ? t("filtersActive", { count: active }) : t("filtersAll")}
          </summary>
          <div className="mt-2 flex flex-wrap items-center gap-x-5 gap-y-2">
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("model")}</span>
              {models.map((m) => chip("models", m, m))}
            </div>
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("scoring")}</span>
              {SCORINGS.map((s) => chip("scorings", s, SCORING_SHORT[s]))}
            </div>
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="text-[11px] uppercase tracking-wide text-slate-500">k</span>
              {DEPTHS.map((k) => chip("depths", String(k), String(k)))}
            </div>
            {active ? (
              <button type="button" onClick={() => commit({ models: "", scorings: "", depths: "" })} className="rounded-full border border-dashed px-2.5 py-0.5 text-xs text-slate-500">
                {t("clearFilters")}
              </button>
            ) : null}
          </div>
        </details>
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("focus")}</span>
          {view.focus.length ? (
            <>
              {view.focus.map((r, i) => (
                <span key={r.id} className="inline-flex items-center gap-1.5 rounded-full border bg-white py-0.5 pl-2 pr-1 text-xs">
                  <i className="inline-block h-2.5 w-2.5 rounded-sm" style={{ background: FOCUS[i] }} />
                  {armLabel(r)}
                  <button type="button" aria-label={t("unfocus", { arm: armLabel(r) })} onClick={() => view.toggleFocus(r)} className="px-1 text-slate-500">
                    ×
                  </button>
                </span>
              ))}
              <button type="button" onClick={() => commit({ focus: "" }, { push: true })} className="rounded-full border border-dashed px-2 py-0.5 text-xs text-slate-500">
                {t("clearFocus")}
              </button>
            </>
          ) : (
            <span className="text-xs text-slate-500">{t("focusNone")}</span>
          )}
        </div>
      </div>
      <div role="tablist" className="flex gap-1 overflow-x-auto border-b">
        {TABS.map((tab) => (
          <button
            key={tab}
            role="tab"
            type="button"
            aria-selected={state.tab === tab}
            onClick={() => commit({ tab }, { push: true })}
            className={`whitespace-nowrap border-b-2 px-3 py-2 text-sm ${state.tab === tab ? "border-blue-800 font-semibold text-slate-900" : "border-transparent text-slate-500"}`}
          >
            {t(`tab.${tab}`)}
          </button>
        ))}
      </div>
      <div role="tabpanel" className="grid min-w-0 gap-3">
        {state.tab === "depth" ? (
          <DepthView view={view} />
        ) : state.tab === "trajectory" ? (
          <TrajectoryView view={view} />
        ) : state.tab === "strata" ? (
          <StrataView view={view} />
        ) : state.tab === "detail" ? (
          <DetailView view={view} />
        ) : (
          <SummaryView view={view} />
        )}
      </div>
    </section>
  );
}
