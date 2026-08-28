"use client";

/**
 * One panel, opened into the proteins it is made of.
 *
 * WHAT THIS SURFACE IS FOR. The experiment graph ends at a cell: NK x BPO holds
 * 1,509 units and the leading level reaches 0.2652. Both numbers are pooled,
 * and neither can be opened, so a reader who distrusts one of them, or who
 * wants to see what makes the twilight band hard, has nowhere to go. This is
 * where they go. It is the last hop of the chain, from a published number to
 * the proteins the number is about.
 *
 * WHY IT IS ITS OWN ROUTE. Three reasons, and the first is the binding one.
 * The graph is the surface an operator opens first and refreshes while a run
 * seals its rows, and the descent costs a parquet read plus a retrieval query
 * per arm; loading that on the graph's first paint would make the page that
 * answers "is anything attributable" wait on a page that answers "which
 * proteins". Second, a cell is a set of coordinates and coordinates belong in
 * a URL: category, aspect, the two bands, the arm and the ordering all survive
 * a reload and travel in a link, which a side panel's state does not. Third,
 * the table is wide and the arm picker is sixteen rows deep, and neither fits
 * beside the nine.
 *
 * WHAT IT NEVER DOES. It never averages cells. With the two refined axes
 * unpinned, an arm holds every band of the panel, and the picker then shows how
 * many cells it covers rather than a number over them: the populations differ
 * by an order of magnitude, so any average across them is a reweighting nobody
 * chose and it moves in the flattering direction. A cell below its floor is
 * marked and still shown, never dropped and never quietly read as a small
 * result.
 *
 * IT COMPUTES NOTHING. Every score in the table is the score the endpoint
 * returned, read out of the per-protein artefact the evaluation wrote. The only
 * client-side work is ordering, labelling and filtering, none of which produces
 * a number a reader could quote.
 */

import { Suspense, useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";
import { ChevronLeft, ChevronRight, RefreshCw } from "lucide-react";

import { ApiError } from "@/lib/api";
import { PANEL_ASPECTS, PANEL_CATEGORIES, getGraph } from "@/lib/graph";
import { axisLabel } from "@/lib/strata";
import { HOMOLOGY_MEANING, HOMOLOGY_ORDER, LENGTH_ORDER, sortBands } from "@/lib/strataView";
import { atStratum, bandsPresent } from "@/lib/stratumCompare";
import {
  ARM_FIELDS,
  SORTS,
  aspectCafa,
  aspectWire,
  armLabel,
  getSettingStrata,
  getStratumProteins,
  groupArms,
  isSort,
  soleCell,
  varyingArmFields,
  type PanelArm,
  type SettingStrata,
  type Sort,
  type StratumProteinsResponse,
} from "@/lib/stratumProteins";

/** Rows per page. Large enough to scan a band, small enough to arrive fast. */
const PAGE = 100;

// ── small shared pieces ──────────────────────────────────────────────────

function Stat({
  label,
  value,
  hint,
  tone = "plain",
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "plain" | "warn";
}) {
  return (
    <div
      className={`rounded-md border px-3 py-1.5 ${
        tone === "warn" ? "border-amber-300 bg-amber-50" : "border-slate-200 bg-white"
      }`}
      title={hint}
    >
      <div
        className={`text-[11px] uppercase tracking-wider ${
          tone === "warn" ? "text-amber-800" : "text-slate-500"
        }`}
      >
        {label}
      </div>
      <div
        className={`font-mono text-lg leading-tight ${
          tone === "warn" ? "font-semibold text-amber-900" : "text-slate-900"
        }`}
      >
        {value}
      </div>
    </div>
  );
}

/**
 * One axis as a row of chips, with an explicit "any" at the head.
 *
 * The values come from the strata artefact and never from a list in this file.
 * A band the campaign stopped producing has to disappear from the control, and
 * a band it started producing has to appear, without a frontend release.
 */
function BandChips({
  label,
  hint,
  value,
  options,
  anyLabel,
  meaning,
  onPick,
}: {
  label: string;
  hint?: string;
  value: string | null;
  options: string[];
  /** The head chip that clears this axis, or absent when it cannot be cleared.
   *  Category and aspect name the panel and always hold a value; length and
   *  homology narrow it and can be released back to the whole panel. */
  anyLabel?: string;
  meaning?: Record<string, string>;
  onPick: (next: string | null) => void;
}) {
  return (
    <div className="flex flex-wrap items-baseline gap-2">
      <span
        className={`text-[11px] uppercase tracking-wider text-slate-500 ${
          hint ? "cursor-help decoration-dotted underline-offset-4 hover:underline" : ""
        }`}
        title={hint}
      >
        {label}
      </span>
      <div className="flex flex-wrap gap-1">
        {anyLabel !== undefined && (
          <button
            type="button"
            onClick={() => onPick(null)}
            aria-pressed={value === null}
            className={`rounded-full border px-2 py-0.5 font-mono text-xs ${
              value === null
                ? "border-slate-800 bg-slate-800 text-white"
                : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50"
            }`}
          >
            {anyLabel}
          </button>
        )}
        {options.map((o) => (
          <button
            key={o}
            type="button"
            onClick={() => onPick(o)}
            aria-pressed={value === o}
            title={meaning?.[o]}
            className={`rounded-full border px-2 py-0.5 font-mono text-xs ${
              value === o
                ? "border-slate-800 bg-slate-800 text-white"
                : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50"
            }`}
          >
            {o}
          </button>
        ))}
      </div>
    </div>
  );
}

// ── the arm ──────────────────────────────────────────────────────────────

/**
 * Which arm's scores are on the table, and what it scored on this cell.
 *
 * The score beside an arm appears only when the coordinates name exactly one
 * cell for it. Unpinned, an arm holds every band of the panel and there is no
 * single number for it here; printing a mean over the bands would promote the
 * smallest and easiest of them, which is the collapse this project refuses
 * everywhere else.
 */
function ArmPicker({
  arms,
  selected,
  onSelect,
}: {
  arms: PanelArm[];
  selected: string | null;
  onSelect: (id: string) => void;
}) {
  const t = useTranslations("stratum");
  const locale = useLocale();
  const fields = useMemo(() => varyingArmFields(arms), [arms]);
  const current = arms.find((a) => a.evaluation_result_id === selected) ?? arms[0];
  const cell = current ? soleCell(current) : null;

  if (arms.length === 0) return null;

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-3">
      <label
        htmlFor="stratum-arm"
        className="text-[11px] uppercase tracking-wider text-slate-500"
      >
        {t("armLabel")}
      </label>
      <p className="mt-0.5 max-w-3xl text-xs leading-relaxed text-slate-500">
        {t("armHint", { count: arms.length })}
      </p>
      <select
        id="stratum-arm"
        value={current?.evaluation_result_id ?? ""}
        onChange={(e) => onSelect(e.target.value)}
        className="mt-2 w-full rounded-md border border-slate-300 bg-white px-2 py-1.5 font-mono text-xs text-slate-900"
      >
        {arms.map((a) => {
          const one = soleCell(a);
          const suffix = one
            ? ` · ${one.f_micro_w.toFixed(4)} / ${one.n_proteins.toLocaleString(locale)}${
                one.reportable === false ? ` ${t("withheldShort")}` : ""
              }`
            : ` · ${t("cellsCovered", { count: a.cells.length })}`;
          return (
            <option key={a.evaluation_result_id} value={a.evaluation_result_id}>
              {armLabel(a, fields)}
              {suffix}
            </option>
          );
        })}
      </select>

      {/* What the arm holds, spelled out rather than left in the option text.
          Every field is printed, including the ones that do not vary: a reader
          arriving from a link has no way to know which of the five the label
          was built from. */}
      {current && (
        <dl className="mt-2 flex flex-wrap gap-x-5 gap-y-1 text-xs">
          {ARM_FIELDS.map((f) => {
            const raw = current.row[f];
            return (
              <div key={f} className="flex items-baseline gap-1.5">
                <dt className="text-slate-500">{t(`arm.${f}`)}</dt>
                <dd className="font-mono text-slate-900">
                  {raw == null || raw === "" ? t("unrecorded") : String(raw)}
                </dd>
              </div>
            );
          })}
        </dl>
      )}

      {cell && cell.reportable === false && (
        <p
          className="mt-2 rounded border border-amber-300 bg-amber-50 px-2 py-1 text-xs text-amber-900"
          role="status"
        >
          {t("withheldCell", { count: cell.n_proteins })}
        </p>
      )}
    </div>
  );
}

// ── the population ───────────────────────────────────────────────────────

/**
 * The four counts, and they stay four.
 *
 * A panel's population, how many of it could be placed on the retrieval axes,
 * how many survive the pinned bands, and how many are on this page. One total
 * would fold a retrieval gap, a filter and a page size into a number that
 * answers none of the three questions a reader has.
 */
function Counts({ data }: { data: StratumProteinsResponse }) {
  const t = useTranslations("stratum");
  const locale = useLocale();
  const n = (v: number) => v.toLocaleString(locale);
  const gap = data.panel_population - data.placed;

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2" data-testid="stratum-counts">
        <Stat label={t("panelPopulation")} value={n(data.panel_population)} hint={t("panelPopulationHint")} />
        <Stat
          label={t("placed")}
          value={n(data.placed)}
          hint={t("placedHint")}
          tone={gap > 0 ? "warn" : "plain"}
        />
        <Stat label={t("matched")} value={n(data.matched)} hint={t("matchedHint")} />
        <Stat
          label={t("scoredZero")}
          value={n(data.scored_zero)}
          hint={t("scoredZeroHint")}
          tone={data.matched > 0 && data.scored_zero * 2 > data.matched ? "warn" : "plain"}
        />
        <Stat
          label={t("tau")}
          value={data.tau == null ? "∅" : data.tau.toFixed(2)}
          hint={t("tauHint")}
        />
      </div>
      {gap > 0 && (
        <p className="text-xs leading-relaxed text-amber-800" role="status">
          {t("unplacedWhy", {
            count: gap,
            noDonor: data.unplaced.no_donor,
            noLength: data.unplaced.no_length,
            offScale: data.unplaced.off_scale,
          })}
        </p>
      )}
    </div>
  );
}

// ── the proteins ─────────────────────────────────────────────────────────

function num(v: number | null, digits = 4): string {
  return v == null ? "∅" : v.toFixed(digits);
}

/**
 * The rows, wide and scrolling in their own container.
 *
 * Each row carries both halves: what the protein scored, and what put it in
 * its band. A reader who cannot see the identity to the nearest donor cannot
 * check the band, and a band nobody can check is a label.
 */
function ProteinTable({ data }: { data: StratumProteinsResponse }) {
  const t = useTranslations("stratum");
  const locale = useLocale();

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
      <table className="min-w-[62rem] w-full text-sm" data-testid="stratum-table">
        <thead>
          <tr className="border-b border-slate-200 bg-slate-50 text-left">
            <th scope="col" className="px-3 py-2 text-xs font-semibold text-slate-600">{t("col.accession")}</th>
            <th scope="col" className="px-3 py-2 text-right text-xs font-semibold text-slate-600">{t("col.f")}</th>
            <th scope="col" className="px-3 py-2 text-right text-xs font-semibold text-slate-600">{t("col.precision")}</th>
            <th scope="col" className="px-3 py-2 text-right text-xs font-semibold text-slate-600">{t("col.recall")}</th>
            <th scope="col" className="px-3 py-2 text-right text-xs font-semibold text-slate-600" title={t("col.groundTruthHint")}>
              {t("col.groundTruth")}
            </th>
            <th scope="col" className="px-3 py-2 text-right text-xs font-semibold text-slate-600">{t("col.identity")}</th>
            <th scope="col" className="px-3 py-2 text-xs font-semibold text-slate-600">{t("col.homology")}</th>
            <th scope="col" className="px-3 py-2 text-xs font-semibold text-slate-600" title={t("col.donorHint")}>
              {t("col.donor")}
            </th>
            <th scope="col" className="px-3 py-2 text-xs font-semibold text-slate-600">{t("col.taxon")}</th>
            <th scope="col" className="px-3 py-2 text-right text-xs font-semibold text-slate-600">{t("col.residues")}</th>
            <th scope="col" className="px-3 py-2 text-xs font-semibold text-slate-600">{t("col.length")}</th>
          </tr>
        </thead>
        <tbody>
          {data.proteins.map((p) => (
            <tr key={p.accession} className="border-b border-slate-100 last:border-0 hover:bg-slate-50">
              <td className="px-3 py-1.5">
                <Link
                  href={`/instrument/proteins/${p.accession}`}
                  className="font-mono text-xs text-blue-700 hover:underline"
                >
                  {p.accession}
                </Link>
              </td>
              <td className="px-3 py-1.5 text-right font-mono text-xs font-semibold text-slate-900">
                {num(p.f_w)}
              </td>
              <td className="px-3 py-1.5 text-right font-mono text-xs text-slate-700">{num(p.precision_w)}</td>
              <td className="px-3 py-1.5 text-right font-mono text-xs text-slate-700">{num(p.recall_w)}</td>
              <td className="px-3 py-1.5 text-right font-mono text-xs text-slate-700">{num(p.n_gt_w, 2)}</td>
              <td className="px-3 py-1.5 text-right font-mono text-xs text-slate-700">
                {p.best_identity == null ? "∅" : `${p.best_identity.toFixed(1)}%`}
              </td>
              <td className="px-3 py-1.5">
                {p.homology_band == null ? (
                  <span className="text-xs text-slate-400">{t("unplacedShort")}</span>
                ) : (
                  <span
                    className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700"
                    title={HOMOLOGY_MEANING[p.homology_band]}
                  >
                    {p.homology_band}
                  </span>
                )}
              </td>
              <td className="px-3 py-1.5 text-xs text-slate-700">
                {p.donor_is_experimental == null
                  ? t("noDonor")
                  : p.donor_is_experimental
                    ? t("donorExperimental")
                    : t("donorOther")}
              </td>
              <td className="px-3 py-1.5 text-xs text-slate-700">
                {p.taxonomic_relation ?? "∅"}
              </td>
              <td className="px-3 py-1.5 text-right font-mono text-xs text-slate-700">
                {p.residues == null ? "∅" : p.residues.toLocaleString(locale)}
              </td>
              <td className="px-3 py-1.5">
                {p.length_band == null ? (
                  <span className="text-xs text-slate-400">{t("unplacedShort")}</span>
                ) : (
                  <span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700">
                    {p.length_band}
                  </span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Pager({
  data,
  onOffset,
}: {
  data: StratumProteinsResponse;
  onOffset: (next: number) => void;
}) {
  const t = useTranslations("stratum");
  const locale = useLocale();
  const first = data.matched === 0 ? 0 : data.offset + 1;
  const last = data.offset + data.returned;
  const back = Math.max(0, data.offset - data.limit);
  const next = data.offset + data.limit;

  return (
    <div className="flex flex-wrap items-center justify-between gap-2">
      <p className="text-xs text-slate-600" data-testid="stratum-range">
        {t("range", {
          first: first.toLocaleString(locale),
          last: last.toLocaleString(locale),
          total: data.matched.toLocaleString(locale),
        })}
      </p>
      <div className="flex items-center gap-1.5">
        <button
          type="button"
          onClick={() => onOffset(back)}
          disabled={data.offset === 0}
          className="inline-flex items-center gap-1 rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs text-slate-700 hover:bg-slate-50 disabled:opacity-40"
        >
          <ChevronLeft className="h-3.5 w-3.5" aria-hidden />
          {t("previous")}
        </button>
        <button
          type="button"
          onClick={() => onOffset(next)}
          disabled={next >= data.matched}
          className="inline-flex items-center gap-1 rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs text-slate-700 hover:bg-slate-50 disabled:opacity-40"
        >
          {t("next")}
          <ChevronRight className="h-3.5 w-3.5" aria-hidden />
        </button>
      </div>
    </div>
  );
}

function Failure({ error }: { error: unknown }) {
  const t = useTranslations("stratum");
  const api = error instanceof ApiError ? error : null;
  const message = error instanceof Error ? error.message : String(error);
  return (
    <div className="rounded-lg border border-rose-200 bg-rose-50 p-4" role="alert" data-testid="stratum-error">
      <h2 className="text-sm font-semibold text-rose-900">
        {api?.status === 404 ? t("errorAbsentTitle") : t("errorTitle")}
      </h2>
      <p className="mt-1 text-sm text-rose-800">{message}</p>
      {api && (
        <p className="mt-1 font-mono text-xs text-rose-700">
          {api.kind} · {api.status || "∅"} · {api.path}
        </p>
      )}
    </div>
  );
}

// ── page ─────────────────────────────────────────────────────────────────

/**
 * Every coordinate, written in one merge.
 *
 * `useUrlParam` writes one key at a time from the search params captured at
 * render, so two calls in the same tick both build from the same snapshot and
 * the second silently discards the first. Every move on this page changes two
 * things at once, because moving the cell has to restart the paging, so the
 * one-key hook would drop the axis change and keep the reader on page four of
 * a band with two pages.
 */
function useCoordinates() {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();

  const patch = useCallback(
    (next: Record<string, string | null>) => {
      const merged = new URLSearchParams(params.toString());
      for (const [key, value] of Object.entries(next)) {
        if (value == null || value === "") merged.delete(key);
        else merged.set(key, value);
      }
      const qs = merged.toString();
      router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false });
    },
    [router, pathname, params],
  );

  return { params, patch };
}

function StratumView() {
  const t = useTranslations("stratum");

  // Coordinates live in the URL. A cell IS its coordinates, so a reader who
  // found something has a link to it, and a reload lands back on the same cell
  // rather than on a default nobody chose.
  const { params, patch } = useCoordinates();
  const setId = params.get("set");
  const setting = params.get("category") ?? PANEL_CATEGORIES[0];
  const panelAspect = params.get("aspect") ?? PANEL_ASPECTS[0];
  const length = params.get("length");
  const homology = params.get("homology");
  const armId = params.get("arm");
  const sortRaw = params.get("sort");
  const offsetRaw = params.get("offset");

  const sort: Sort = isSort(sortRaw) ? sortRaw : SORTS[0];
  const offset = Number.isFinite(Number(offsetRaw)) ? Math.max(0, Number(offsetRaw)) : 0;

  const [strata, setStrata] = useState<SettingStrata | null>(null);
  const [strataError, setStrataError] = useState<unknown>(null);
  const [data, setData] = useState<StratumProteinsResponse | null>(null);
  const [dataError, setDataError] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);

  // The evaluation set is normally carried in the link from the graph. Resolved
  // from the frame only when it is not, so a bare URL still opens on the run
  // the rest of the instrument is showing rather than on nothing.
  useEffect(() => {
    if (setId) return;
    let live = true;
    getGraph()
      .then((g) => {
        if (live && g.frame.evaluation_set_id) patch({ set: g.frame.evaluation_set_id });
      })
      .catch((e: unknown) => {
        if (live) setStrataError(e);
      });
    return () => {
      live = false;
    };
  }, [setId, patch]);

  // Nothing is set synchronously here. An effect body that calls setState
  // cascades a render before the fetch has even left, and the page then paints
  // an empty frame it is about to replace. The state moves only when the
  // promise settles, and what makes the wait visible is the freshness check
  // below rather than a cleared payload.
  useEffect(() => {
    if (!setId) return;
    let live = true;
    getSettingStrata(setId, setting)
      .then((s) => {
        if (!live) return;
        setStrata(s);
        setStrataError(null);
      })
      .catch((e: unknown) => {
        if (!live) return;
        setStrata(null);
        setStrataError(e);
      });
    return () => {
      live = false;
    };
  }, [setId, setting]);

  // The axes, read off the artefact and never off a list in this file. Rows
  // from another setting are not this setting's rows: the three are different
  // populations, and a payload still in flight when the reader moves must not
  // be drawn as if it answered the question now on screen.
  const rows = useMemo(
    () => (strata && strata.setting === setting ? strata.rows : []),
    [strata, setting],
  );
  // Offered in the CAFA spelling the panels print, ordered as the panels order
  // them, so an aspect does not change its name between the grid and the cell
  // the grid links into. The artefact's own column is the source; it holds the
  // single-char wire code, which is translated at the boundary and nowhere else.
  const aspects = useMemo(
    () => sortBands(bandsPresent(rows, "aspect").map(aspectCafa), PANEL_ASPECTS),
    [rows],
  );
  const panelRows = useMemo(
    () => atStratum(rows, { aspect: aspectWire(panelAspect) }),
    [rows, panelAspect],
  );
  const lengths = useMemo(
    () => sortBands(bandsPresent(panelRows, "length"), LENGTH_ORDER),
    [panelRows],
  );
  const homologies = useMemo(
    () => sortBands(bandsPresent(atStratum(panelRows, { length: length ?? undefined }), "homology"), HOMOLOGY_ORDER),
    [panelRows, length],
  );
  const arms = useMemo(
    () =>
      groupArms(
        atStratum(panelRows, {
          length: length ?? undefined,
          homology: homology ?? undefined,
        }),
      ),
    [panelRows, length, homology],
  );

  const chosen = arms.find((a) => a.evaluation_result_id === armId) ?? arms[0] ?? null;

  // Split in two for the same reason the graph page is. `fetchProteins`
  // touches no state until its promise settles, which is what lets the effect
  // call it; `reload` is the button's entry point and may mark the page busy
  // at once, because it runs from an event and not from an effect.
  const fetchProteins = useCallback(() => {
    if (!chosen) return;
    getStratumProteins({
      evaluationResultId: chosen.evaluation_result_id,
      setting,
      aspect: panelAspect,
      length,
      homology,
      sort,
      limit: PAGE,
      offset,
    })
      .then((d) => {
        setData(d);
        setDataError(null);
      })
      .catch((e: unknown) => {
        setData(null);
        setDataError(e);
      })
      .finally(() => setBusy(false));
  }, [chosen, setting, panelAspect, length, homology, sort, offset]);

  const reload = useCallback(() => {
    setBusy(true);
    fetchProteins();
  }, [fetchProteins]);

  useEffect(() => {
    fetchProteins();
  }, [fetchProteins]);

  // Whether what is on screen answers the question now in the URL. A page that
  // keeps drawing the previous cell while the next one loads is worse than one
  // that says it is waiting: the coordinates above the table would name a cell
  // the table is not of, and nothing on screen would say so.
  const fresh =
    data !== null &&
    chosen !== null &&
    data.evaluation_result_id === chosen.evaluation_result_id &&
    data.setting === setting &&
    data.where.aspect === panelAspect &&
    (data.where.length ?? null) === (length ?? null) &&
    (data.where.homology ?? null) === (homology ?? null) &&
    data.offset === offset &&
    data.sort === sort;

  // Moving the cell restarts the paging. Keeping the offset would land a
  // reader on page four of a band with two pages, which reads as an empty cell.
  const move = useCallback(
    (next: Record<string, string | null>) => patch({ ...next, offset: null }),
    [patch],
  );

  return (
    <div className="mx-auto max-w-7xl space-y-5 px-4 py-8 sm:px-6">
      <header className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-slate-900">{t("title")}</h1>
          <p className="mt-1 max-w-3xl text-sm leading-relaxed text-slate-600">{t("subtitle")}</p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Link
            href="/instrument/graph"
            className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50"
          >
            {t("backToGraph")}
          </Link>
          <button
            type="button"
            onClick={reload}
            disabled={busy || !chosen}
            className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${busy ? "animate-spin" : ""}`} aria-hidden />
            {busy ? t("reloading") : t("reload")}
          </button>
        </div>
      </header>

      {/* The cell. Category and aspect first because they are the panel; the
          two refined axes after, because they narrow it. */}
      <section className="space-y-2.5 rounded-lg border border-slate-200 bg-white p-4" data-testid="stratum-coordinates">
        <BandChips
          label={t("axis.category")}
          hint={t("axis.categoryHint")}
          value={setting}
          options={[...PANEL_CATEGORIES]}
          onPick={(v) => move({ category: v ?? setting, arm: null })}
        />
        <BandChips
          label={t("axis.aspect")}
          value={panelAspect}
          options={aspects}
          onPick={(v) => move({ aspect: v ?? panelAspect })}
        />
        <BandChips
          label={t("axis.length")}
          hint={t("axis.lengthHint")}
          value={length}
          options={lengths}
          anyLabel={t("anyBand")}
          onPick={(v) => move({ length: v })}
        />
        <BandChips
          label={t("axis.homology")}
          hint={t("axis.homologyHint")}
          value={homology}
          options={homologies}
          anyLabel={t("anyBand")}
          meaning={HOMOLOGY_MEANING}
          onPick={(v) => move({ homology: v })}
        />
        {strata && strata.setting === setting && (
          <p className="pt-1 text-xs text-slate-500">
            {t("armsStratified", {
              with: strata.arms_with_strata,
              total: strata.arms_total,
            })}
          </p>
        )}
      </section>

      {strataError !== null && <Failure error={strataError} />}

      {strata && strata.setting === setting && arms.length === 0 && (
        <div className="rounded-lg border border-slate-200 bg-white p-6" data-testid="stratum-no-arms">
          <h2 className="text-base font-semibold text-slate-900">{t("noArmsTitle")}</h2>
          <p className="mt-1 max-w-2xl text-sm leading-relaxed text-slate-600">{t("noArmsBody")}</p>
        </div>
      )}

      {arms.length > 0 && (
        <ArmPicker
          arms={arms}
          selected={chosen?.evaluation_result_id ?? null}
          onSelect={(id) => move({ arm: id })}
        />
      )}

      {dataError !== null && <Failure error={dataError} />}

      {fresh && data && (
        <>
          <Counts data={data} />

          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex flex-wrap items-baseline gap-2">
              <span className="text-[11px] uppercase tracking-wider text-slate-500">{t("sortLabel")}</span>
              <div className="inline-flex overflow-hidden rounded-md border border-slate-300">
                {SORTS.map((s) => (
                  <button
                    key={s}
                    type="button"
                    onClick={() => move({ sort: s === SORTS[0] ? null : s })}
                    aria-pressed={sort === s}
                    className={`px-2.5 py-1 font-mono text-[11px] ${
                      sort === s ? "bg-slate-800 text-white" : "bg-white text-slate-700 hover:bg-slate-50"
                    }`}
                  >
                    {t(`sort.${s}`)}
                  </button>
                ))}
              </div>
            </div>
            <span className="font-mono text-[11px] text-slate-500">
              {axisLabel("category", data.where.category)} · {data.where.aspect}
              {data.where.length ? ` · ${data.where.length}` : ""}
              {data.where.homology ? ` · ${data.where.homology}` : ""}
            </span>
          </div>

          {data.matched === 0 ? (
            <div className="rounded-lg border border-slate-200 bg-white p-6" data-testid="stratum-empty">
              <h2 className="text-base font-semibold text-slate-900">{t("emptyTitle")}</h2>
              <p className="mt-1 max-w-2xl text-sm leading-relaxed text-slate-600">{t("emptyBody")}</p>
            </div>
          ) : (
            <>
              <ProteinTable data={data} />
              <Pager data={data} onOffset={(o) => patch({ offset: o === 0 ? null : String(o) })} />
            </>
          )}
        </>
      )}

      {!fresh && dataError === null && chosen && (
        <p className="text-sm text-slate-500" data-testid="stratum-loading">
          {t("loading")}
        </p>
      )}

      <p className="max-w-3xl text-xs leading-relaxed text-slate-500">
        {t("footnote")}
      </p>
    </div>
  );
}

export default function StratumPage() {
  return (
    <Suspense fallback={null}>
      <StratumView />
    </Suspense>
  );
}
