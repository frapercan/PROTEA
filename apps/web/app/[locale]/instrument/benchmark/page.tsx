"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { useTranslations } from "next-intl";
import { BenchmarkHeatmap } from "@/components/BenchmarkHeatmap";
import { EvalProvenanceBadges } from "@/components/EvalProvenanceBadges";
import { Skeleton } from "@/components/Skeleton";
import { Tooltip } from "@/components/Tooltip";
import {
  cellKey,
  formatParams,
  formatProteins,
  formatStd,
  hasCiBand,
  indexBestPerCell,
  indexRows,
  pickDefaultStage,
  stageLabel,
} from "@/lib/benchmarkHelpers";
import { downloadCsv, rowsToCsv } from "@/lib/benchmarkCsv";
import { MetricSelector, availableMetrics } from "@/components/MetricSelector";
import { StratumCompare } from "@/components/StratumCompare";
import { PRIMARY_METRIC, isUnweighted, metricValue } from "@/lib/metrics";
import {
  activeEvalSet,
  emptyKind,
  preferCatalog,
  resolveLineage,
  rowsInCategories,
  stageContext,
  visibleEvalSets,
} from "@/lib/benchmarkView";
import { useUrlNumber, useUrlParam } from "@/lib/useUrlParam";
import {
  bestPerCellFromRows,
  curateBestCells,
  curateRows,
  evalSetIdsWithRows,
  lafaCellKeys,
  perTaskFromRows,
} from "@/lib/benchmarkCuration";
import {
  getBenchmarkEmbeddings,
  getBenchmarkMatrix,
  type BenchmarkBestCell,
  type BenchmarkEmbedding,
  type BenchmarkEvalSet,
  type BenchmarkMatrixResponse,
  type BenchmarkStage,
  type PerTaskAggregate,
} from "@/lib/api";

/** Canonical category definitions. Anchored to the leakage-fixed champion
 *  memory (project_lb2_leakage_fixed_champion) and the paired bootstrap
 *  result (project_lb3_paired_ci_2026_05_18): NK = the strict CAFA "no
 *  knowledge" split (no experimental annotations exist for the test
 *  protein in the train cutoff), LK = limited knowledge (some aspects
 *  annotated but not the one under evaluation), PK = partial knowledge
 *  (the protein is annotated for the same aspect but with at least one
 *  new term added in the evaluation cutoff). Selective-deploy memory
 *  (project_v27_binary_multiseed) explains why PK is policy-zero for
 *  the v27-binary reranker. */
const CATEGORY_TOOLTIPS: Record<string, string> = {
  NK: "No Knowledge — the test protein has no experimental annotations of any aspect at the train cutoff. Strictest CAFA split: 0.7291 ± 0.0028 multiseed Fmax on v27-binary (NK+LK pooled).",
  LK: "Limited Knowledge — the protein carries annotations in other aspects but not the one being evaluated. Paired with NK in the publishable selective-deploy claim.",
  PK: "Partial Knowledge — the protein already has annotations for the same aspect; the evaluation only scores newly added terms. Reranker stays policy-zero here; KNN baseline drives selective deploy.",
};

/** Lineage chip filter — decomposes the BenchmarkEvalSet.stats counters
 *  (nk_proteins / lk_proteins / pk_proteins) along the same NK / LK / PK
 *  axis used by the matrix rows. Selecting a chip keeps only rows whose
 *  ``category`` matches one of the listed categories; ``All`` keeps every
 *  category returned by the matrix endpoint. This is purely a UI filter
 *  on top of the data already in hand — no extra request. */
type LineageChipKey = "all" | "nk_lk" | "pk_only" | "nk" | "lk";

const LINEAGE_CHIPS: {
  key: LineageChipKey;
  label: string;
  cats: string[];
  tooltip: string;
}[] = [
  {
    key: "all",
    label: "All",
    cats: ["NK", "LK", "PK"],
    tooltip:
      "Show every CAFA split (No / Limited / Partial Knowledge). Restores the full benchmark matrix.",
  },
  {
    key: "nk_lk",
    label: "NK + LK only",
    cats: ["NK", "LK"],
    tooltip:
      "Selective-deploy window: the v27-binary reranker is published only on NK + LK splits (0.7291 ± 0.0028 multiseed pooled). Removes PK from the matrix.",
  },
  {
    key: "pk_only",
    label: "PK only",
    cats: ["PK"],
    tooltip:
      "Partial Knowledge in isolation. The reranker stays policy-zero here, so the matrix surfaces the KNN baseline that drives the selective-deploy decision.",
  },
  {
    key: "nk",
    label: "NK",
    cats: ["NK"],
    tooltip:
      "No Knowledge in isolation — the strictest CAFA split where the test protein has zero experimental annotations at train cutoff.",
  },
  {
    key: "lk",
    label: "LK",
    cats: ["LK"],
    tooltip:
      "Limited Knowledge in isolation — proteins annotated in other aspects but not the one under evaluation.",
  },
];

/** IA-weighted headline metric definition (FIX-METRIC-IA). */
const FMICRO_TOOLTIP =
  "f_micro_w: the information-accretion-weighted micro-averaged F score. The LAFA / CAFA headline metric, comparable to the external leaderboards. Rare, specific GO terms count more than common ones.";

/** Shown on cells still carrying the legacy unweighted Fmax (no real IA yet). */
const NOT_IA_TOOLTIP =
  "Not IA-weighted: this cell predates real information-accretion scoring and reports the unweighted Fmax, which is not directly comparable to the LAFA / CAFA leaderboards.";

// ── Helpers ──────────────────────────────────────────────────────────────

export default function BenchmarkPage() {
  const params = useParams<{ locale: string }>();
  const locale = params?.locale ?? "en";
  const t = useTranslations("benchmark");
  const [embeddings, setEmbeddings] = useState<BenchmarkEmbedding[] | null>(
    null,
  );
  const [matrix, setMatrix] = useState<BenchmarkMatrixResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  // URL-synced filters: copy the page link and the chips persist.
  const [stage, setStage] = useUrlParam("stage", null);
  // An absent param means "all", and that is a weaker default than the one
  // it replaces. It used to mean "whatever the current campaign step is",
  // resolved through the retired ladder surface, which kept a reader who
  // arrived with no opinion from getting two temporal windows in one table.
  // That surface is retired, so the resolution is gone with it. The risk it
  // guarded against is not live today, because the database holds a single
  // evaluation set and "all" therefore selects exactly that one; it returns
  // the moment a second one lands, and the experiment graph is where the
  // frame to pick between them will come from.
  const [evalSetIdRaw, setEvalSetIdRaw] = useUrlParam("eval_set", null);
  const evalSetId = (evalSetIdRaw ?? "all") as string | "all";
  const setEvalSetId = (v: string | "all") => setEvalSetIdRaw(v);
  const [selectedK, setSelectedK] = useUrlNumber("k", null);
  // Lineage chip filter (URL-synced). Default "all" is encoded as the
  // missing param so a copied benchmark URL stays clean for the default
  // view. Anchored to the BenchmarkEvalSet.stats nk/lk/pk counters and
  // the selective-deploy memory (project_v27_binary_multiseed): NK+LK is
  // the published reranker window; PK is policy-zero.
  const [lineageRaw, setLineageRaw] = useUrlParam("lineage", null);
  const lineage = (lineageRaw ?? "all") as LineageChipKey;
  const setLineage = (v: LineageChipKey) =>
    setLineageRaw(v === "all" ? null : v);
  // Reversible curation (slice F-METHOD-EVAL-SURFACE). The default view hides
  // diagnostic probe rows and legacy internal-frame rows superseded by a
  // current LAFA-frame result; "Show all" reveals everything. Encoded as the
  // missing param when off so a copied default URL stays clean. No data is
  // dropped: this is a pure presentation filter on rows already in hand.
  const [showAllRaw, setShowAllRaw] = useUrlParam("all", null);
  // In the URL like every other filter: a link that renders under the
  // recipient's own last selection is how two people end up arguing about
  // different numbers while looking at "the same" page.
  const [metricRaw, setMetric] = useUrlParam("metric", PRIMARY_METRIC);
  const metric = metricRaw ?? PRIMARY_METRIC;
  const showAll = showAllRaw === "1";
  const setShowAll = (v: boolean) => setShowAllRaw(v ? "1" : null);
  // Default view: heatmap small-multiples (#81). The full numeric matrix is
  // still one click away under the toggle for export workflows.
  const [viewMode, setViewMode] = useState<"heatmap" | "table">("heatmap");

  // Unfiltered catalog fetch — populates the full set of known stages and
  // eval sets, so selector chips don't disappear when a filtered query
  // returns zero rows.
  const [catalog, setCatalog] = useState<{
    stages: BenchmarkStage[];
    evalSets: BenchmarkEvalSet[];
    categories: string[];
    aspects: string[];
    ks: number[];
  }>({ stages: [], evalSets: [], categories: [], aspects: [], ks: [] });

  // Filter signature most recently used to fetch matrix data. The
  // filter-change effect compares the current filter state against this
  // ref and skips when they match — that swallows the programmatic
  // ``setStage(default)`` / ``setSelectedK(default)`` calls the first
  // effect issues right after applying the URL-pinned response.
  const lastFetchedSig = useRef<string | null>(null);

  function filterSig(s: string | null, esid: string, k: number | null): string {
    return `${s ?? ""}|${esid}|${k ?? ""}`;
  }

  // First effect: runs ONCE on mount. Fires a SINGLE Promise.all that
  // requests the matrix using whatever filters the URL provides
  // (``stage`` / ``evaluation_set_id`` / ``k`` — any combination) and
  // the embeddings list in parallel. Catalog (stages, evalSets, ks) is
  // derived from the response, which always echoes the full observed
  // catalog regardless of filter — see ``_aggregate_benchmark_matrix``
  // in protea/api/routers/benchmark.py.
  //
  // When the URL is bare or partial, ``setStage(default)`` /
  // ``setSelectedK(default)`` push derived defaults into URL state. The
  // second effect compares the resulting filter signature against the
  // one we just fetched and short-circuits when they match, so this
  // first paint is always exactly ONE matrix call. UX trade-off on the
  // bare/partial path: leaderboards reflect the request filters (which
  // may be a superset of the resolved chip selection) until the user
  // changes a chip; the global-champion banner stays correct either
  // way, and the user's URL ``/en/instrument/benchmark/?k=3`` (the slow path that
  // motivated this refactor) now resolves in one round-trip.
  useEffect(() => {
    setError(null);
    const urlPinned = {
      stage: stage ?? undefined,
      evaluation_set_id: evalSetId === "all" ? undefined : evalSetId,
      k: selectedK ?? undefined,
      // With no stage pinned the server picks the preferred one and sends
      // that alone. One evaluation set carries nine stages of 432 cells,
      // so the unfiltered payload is 3 MB against 383 kB, and the client
      // was discarding eight ninths of it to show one stage. The catalogue
      // is unaffected, so every stage stays in the selector.
      resolve_stage: stage == null,
    };
    const hasAnyPin = stage != null || evalSetId !== "all" || selectedK != null;
    Promise.all([
      getBenchmarkMatrix(hasAnyPin ? urlPinned : { resolve_stage: true }),
      getBenchmarkEmbeddings(),
    ])
      .then(([m, e]) => {
        setCatalog({
          stages: m.stages,
          evalSets: m.evaluation_sets,
          categories: m.categories,
          aspects: m.aspects,
          ks: m.ks ?? [],
        });
        setEmbeddings(e.embeddings);
        setMatrix(m);
        const resolvedStage = stage ?? pickDefaultStage(m.stages);
        const resolvedK = selectedK ?? m.ks?.[0] ?? null;
        // Record the resolved signature BEFORE pushing defaults to URL
        // state, so the second effect's no-op pass on the same filter
        // combo short-circuits and we don't issue a redundant call.
        lastFetchedSig.current = filterSig(resolvedStage, evalSetId, resolvedK);
        if (stage == null && resolvedStage != null) setStage(resolvedStage);
        if (selectedK == null && resolvedK != null) setSelectedK(resolvedK);
      })
      .catch((e) => setError(e.message));
    // Intentionally mount-only. URL state read once; subsequent changes
    // go through the filter-change effect below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Second effect: fires when the user actively changes a filter chip.
  // Skips runs whose filter signature already matches the data we hold,
  // which absorbs both the initial defaults-application from the first
  // effect and any no-op re-renders that don't change filters.
  useEffect(() => {
    if (stage === null) return;
    const sig = filterSig(stage, evalSetId, selectedK);
    if (lastFetchedSig.current === sig) return;
    setError(null);
    lastFetchedSig.current = sig;
    getBenchmarkMatrix({
      stage,
      evaluation_set_id: evalSetId === "all" ? undefined : evalSetId,
      k: selectedK ?? undefined,
    })
      .then(setMatrix)
      .catch((e) => setError(e.message));
  }, [stage, evalSetId, selectedK]);

  // Reversible curation: drop probe + superseded-internal rows in the default
  // view. Everything downstream (table, heatmap, leaderboards, headline) is
  // recomputed from this curated set so the page can never point at a row it
  // no longer shows. ``curation.rows`` === ``matrix.rows`` when showAll.
  const curation = useMemo(
    () => curateRows(matrix?.rows ?? [], showAll),
    [matrix, showAll],
  );
  const curatedRows = curation.rows;

  // LAFA-frame cell keys come from the full row set so the superseded-internal
  // test for the global leaderboard sees every frame, not just curated rows.
  const lafaKeys = useMemo(() => lafaCellKeys(matrix?.rows ?? []), [matrix]);

  const rowIndex = useMemo(() => indexRows(curatedRows), [curatedRows]);

  // In-selection leaderboard recomputed from curated rows (mirrors the backend
  // max-primary-per-cell), so a hidden probe row never wins a cell by default.
  const bestPerCell = useMemo(
    () =>
      matrix
        ? indexBestPerCell(
            bestPerCellFromRows(curatedRows, matrix.categories, matrix.aspects),
          )
        : new Map<string, BenchmarkBestCell>(),
    [matrix, curatedRows],
  );

  // Global champions ignore stage/K, so they cannot be recomputed from the
  // (stage-filtered) row set; instead the probe / superseded entries are
  // filtered out of the server list in the default view.
  const bestPerCellGlobal = useMemo(
    () =>
      indexBestPerCell(
        curateBestCells(matrix?.best_per_cell_global ?? [], showAll, lafaKeys),
      ),
    [matrix, showAll, lafaKeys],
  );

  const embeddingsWithData = useMemo(() => {
    if (!embeddings) return new Set<string>();
    return new Set(curatedRows.map((r) => r.embedding_config_id));
  }, [embeddings, curatedRows]);

  // Honest headline: per-task mean ± 95% CI recomputed across the curated
  // models in the current selection (FIX-METRIC-IA), keyed by (category|aspect)
  // so probe / superseded rows never inflate the mean.
  const perTaskIndex = useMemo(() => {
    const out = new Map<string, PerTaskAggregate>();
    if (!matrix) return out;
    for (const t of perTaskFromRows(
      curatedRows,
      matrix.categories,
      matrix.aspects,
    )) {
      out.set(`${t.category}|${t.aspect}`, t);
    }
    return out;
  }, [matrix, curatedRows]);

  if (error) {
    return (
      <div className="max-w-6xl mx-auto px-4 sm:px-6 py-12">
        <div className="rounded-lg border border-red-200 bg-red-50 p-6 text-center">
          <p className="text-red-800 text-sm">{error}</p>
        </div>
      </div>
    );
  }

  if (!embeddings || !matrix || stage === null) {
    // Two-step progress hint: matrix fetch typically dominates (200-1200ms
    // cold, instant when the Next.js 60s cache is warm); the embeddings
    // catalog usually returns first. We surface both so users see motion
    // even when one half is mid-flight, and a shimmer-row preview hints at
    // the table structure (PLM rows x aspect/category columns) that's
    // about to render. Memory feedback_no_blocking_slow_endpoints: the
    // real fix is the cache on getBenchmarkMatrix, not a synchronous wait.
    const matrixReady = matrix !== null;
    const embeddingsReady = embeddings !== null;
    return (
      <div
        className="max-w-6xl mx-auto px-4 sm:px-6 py-12 space-y-6"
        role="status"
        aria-live="polite"
        aria-busy="true"
      >
        <div className="space-y-2">
          <p className="text-sm font-semibold text-slate-700">
            {t("loadingTitle")}
          </p>
          <p className="text-xs text-slate-500 max-w-2xl">{t("loadingHint")}</p>
        </div>
        <ul className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs">
          <li className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-3 py-2">
            <span
              aria-hidden
              className={`inline-block h-2 w-2 rounded-full ${
                matrixReady ? "bg-emerald-500" : "bg-blue-400 animate-pulse"
              }`}
            />
            <span className={matrixReady ? "text-slate-700" : "text-slate-500"}>
              {t("loadingStepMatrix")}
            </span>
          </li>
          <li className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-3 py-2">
            <span
              aria-hidden
              className={`inline-block h-2 w-2 rounded-full ${
                embeddingsReady ? "bg-emerald-500" : "bg-blue-400 animate-pulse"
              }`}
            />
            <span
              className={embeddingsReady ? "text-slate-700" : "text-slate-500"}
            >
              {t("loadingStepEmbeddings")}
            </span>
          </li>
        </ul>
        {/* Shimmer preview: two header rows (categories then aspects) and
            four embedding rows hint at the table structure so the user
            knows what's about to land. */}
        <div className="rounded-lg border bg-white shadow-sm overflow-hidden">
          <p className="px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500 border-b bg-slate-50">
            {t("loadingRowsPreview")}
          </p>
          <div className="divide-y">
            {Array.from({ length: 6 }).map((_, i) => (
              <div
                key={i}
                className="grid grid-cols-[180px_repeat(9,minmax(0,1fr))] gap-2 px-4 py-2.5 items-center"
              >
                <Skeleton className="h-4 w-32" />
                {Array.from({ length: 9 }).map((__, j) => (
                  <Skeleton key={j} className="h-4 w-12 mx-auto" />
                ))}
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  const hasData = curatedRows.length > 0;
  const empty = emptyKind(curatedRows, matrix.rows.length);
  const curatedToEmpty = empty === "curated-to-empty";

  const { stages: stageList, label: currentStageLabel } = stageContext(
    catalog.stages,
    matrix.stages,
    stage,
  );
  const allEvalSetList = preferCatalog(catalog.evalSets, matrix.evaluation_sets);
  const evalSetList = visibleEvalSets(
    allEvalSetList,
    evalSetIdsWithRows(curatedRows),
    evalSetId,
    showAll,
  );
  const hiddenEvalSetCount = allEvalSetList.length - evalSetList.length;
  const allCategories = preferCatalog(catalog.categories, matrix.categories);
  const aspects = preferCatalog(catalog.aspects, matrix.aspects);

  const activeChip = LINEAGE_CHIPS.find((c) => c.key === lineage) ?? LINEAGE_CHIPS[0];
  const lineageResolution = resolveLineage(allCategories, activeChip);
  const effectiveCategories = lineageResolution.categories;
  const lineageHasNoMatch = lineageResolution.fellBack;
  // Row-level lookups (heatmap, table) skip rows whose category the chip
  // filtered out. Identical to the chip on the happy path, and the full set
  // when the fallback above fired.
  const effectiveCatSet = new Set(effectiveCategories);
  const filteredRows = rowsInCategories(curatedRows, effectiveCategories);

  const activeEvalSetRow = activeEvalSet(evalSetList, evalSetId);

  return (
    <div className="max-w-6xl mx-auto px-4 sm:px-6 py-8 space-y-6">
      {/* A campaign line used to sit here, above the header, so a reader
          landing on a matrix of numbers could see which campaign produced
          them. It is retired. It read the ladder surface, which derived a
          full grid of finished work by counting jobs against evaluation
          results that had been deleted, so the context it supplied was the
          one thing on this page with nothing behind it. The matrix below is
          unaffected: it reads persisted results directly. The experiment
          graph at /v1/graph is where the question comes back. */}
      {/* Above the matrix, because the matrix ranks over the whole
          population and that number is the one for the band that needs
          least help. The spread across eight representations was measured
          at 0.0848 with no close donor and 0.0081 with one. Only shown for
          a single evaluation set: "all" would compare across temporal
          windows. */}
      {evalSetId !== "all" ? (
        <StratumCompare evaluationSetId={evalSetId} k={selectedK} />
      ) : null}
      {/* Header */}
      <header className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">
            Benchmark matrix
          </h1>
          <p className="text-sm text-slate-500 mt-1">
            Per-embedding IA-weighted{" "}
            <span className="font-mono">f_micro_w</span> (LAFA / CAFA
            comparable) across categories and aspects for every evaluation run
            in the database.{" "}
            <Link
              href={`/${locale}/`}
              className="text-blue-600 hover:underline"
            >
              {t("backToHome")}
            </Link>
          </p>
        </div>
        <div className="flex gap-2">
          {/* Reversible curation toggle. Default view hides probe + legacy
              superseded-internal rows; one click reveals everything. */}
          <Tooltip
            text={
              showAll
                ? "Showing every row, including diagnostic probes and legacy internal-frame results. Click to return to the curated default."
                : "Curated default: diagnostic probe rows and legacy internal-frame results superseded by a current LAFA-frame number are hidden. Click to show all."
            }
          >
            <button
              type="button"
              onClick={() => setShowAll(!showAll)}
              aria-pressed={showAll}
              data-testid="benchmark-show-all"
              className={`rounded-md border px-3 py-1.5 text-sm transition-colors ${
                showAll
                  ? "border-amber-300 bg-amber-50 text-amber-800 hover:bg-amber-100"
                  : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50"
              }`}
            >
              {showAll ? "Showing all" : "Show all (incl. legacy / probes)"}
            </button>
          </Tooltip>
          <button
            disabled={!hasData}
            onClick={() =>
              downloadCsv(
                `benchmark_${stage}${lineage !== "all" ? `_${lineage}` : ""}${showAll ? "_all" : ""}.csv`,
                rowsToCsv(embeddings, filteredRows, stage),
              )
            }
            className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Download CSV
          </button>
        </div>
      </header>

      {/* Curation banner: explains what the default view hides and offers the
          reveal. Only shown when curation actually removed something so the
          clean default stays uncluttered (and stays silent mid-recompute when
          the provenance columns are still null). */}
      {!showAll && (curation.hiddenTotal > 0 || hiddenEvalSetCount > 0) && (
        <div
          className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600"
          role="status"
          data-testid="benchmark-curation-banner"
        >
          <span>
            Curated view ·{" "}
            {curation.hiddenProbe > 0 && (
              <span>
                <strong>{curation.hiddenProbe}</strong> probe
                {curation.hiddenProbe === 1 ? "" : "s"}
              </span>
            )}
            {curation.hiddenProbe > 0 && curation.hiddenLegacy > 0 && " · "}
            {curation.hiddenLegacy > 0 && (
              <span>
                <strong>{curation.hiddenLegacy}</strong> legacy internal-frame
              </span>
            )}
            {curation.hiddenTotal > 0 && hiddenEvalSetCount > 0 && " · "}
            {hiddenEvalSetCount > 0 && (
              <span>
                <strong>{hiddenEvalSetCount}</strong> eval set
                {hiddenEvalSetCount === 1 ? "" : "s"}
              </span>
            )}{" "}
            hidden. Nothing is deleted.
          </span>
          <button
            type="button"
            onClick={() => setShowAll(true)}
            className="font-semibold text-blue-600 hover:text-blue-800 underline underline-offset-2"
          >
            Show all
          </button>
        </div>
      )}
      {showAll && (
        <div
          className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800"
          role="status"
        >
          <span>
            Showing <strong>all</strong> rows, including diagnostic probes and
            legacy internal-frame results that the curated default hides.
          </span>
          <button
            type="button"
            onClick={() => setShowAll(false)}
            className="font-semibold text-amber-700 hover:text-amber-900 underline underline-offset-2"
          >
            Back to curated
          </button>
        </div>
      )}

      {/* Eval set context banner */}
      {activeEvalSetRow && (
        <section className="rounded-lg border border-blue-100 bg-blue-50 px-4 py-3">
          <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
            <div>
              <span className="text-xs font-semibold text-blue-700 uppercase tracking-wider">
                Evaluation split
              </span>
              <div className="text-sm font-semibold text-blue-900 mt-0.5">
                {activeEvalSetRow.label}
              </div>
            </div>
            <div className="flex gap-3 text-xs text-blue-800">
              {activeEvalSetRow.stats.delta_proteins != null && (
                <span>
                  <span className="text-blue-500">Δ</span>{" "}
                  <span className="font-mono font-semibold">
                    {activeEvalSetRow.stats.delta_proteins.toLocaleString()}
                  </span>{" "}
                  proteins
                </span>
              )}
              {/* NK/LK/PK counters dim when the lineage chip filters
                  them out, so the banner doubles as the chip's effective
                  scope readout. The active chip's categories stay bold. */}
              {activeEvalSetRow.stats.nk_proteins != null && (
                <span
                  className={effectiveCatSet.has("NK") ? "" : "opacity-40"}
                  title={
                    effectiveCatSet.has("NK")
                      ? undefined
                      : "Hidden by lineage chip"
                  }
                >
                  <span className="text-blue-500">NK</span>{" "}
                  <span className="font-mono font-semibold">
                    {formatProteins(activeEvalSetRow.stats.nk_proteins)}
                  </span>
                </span>
              )}
              {activeEvalSetRow.stats.lk_proteins != null && (
                <span
                  className={effectiveCatSet.has("LK") ? "" : "opacity-40"}
                  title={
                    effectiveCatSet.has("LK")
                      ? undefined
                      : "Hidden by lineage chip"
                  }
                >
                  <span className="text-blue-500">LK</span>{" "}
                  <span className="font-mono font-semibold">
                    {formatProteins(activeEvalSetRow.stats.lk_proteins)}
                  </span>
                </span>
              )}
              {activeEvalSetRow.stats.pk_proteins != null && (
                <span
                  className={effectiveCatSet.has("PK") ? "" : "opacity-40"}
                  title={
                    effectiveCatSet.has("PK")
                      ? undefined
                      : "Hidden by lineage chip"
                  }
                >
                  <span className="text-blue-500">PK</span>{" "}
                  <span className="font-mono font-semibold">
                    {formatProteins(activeEvalSetRow.stats.pk_proteins)}
                  </span>
                </span>
              )}
              {activeEvalSetRow.new_obo_version && (
                <span>
                  <span className="text-blue-500">OBO</span>{" "}
                  <span className="font-mono">{activeEvalSetRow.new_obo_version}</span>
                </span>
              )}
            </div>
          </div>
        </section>
      )}

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-end">
        <div>
          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
            Pipeline stage
          </label>
          <div
            role="group"
            aria-label="Pipeline stage"
            className="flex flex-wrap gap-1 rounded-lg bg-slate-100 p-0.5"
          >
            {stageList.map((s) => (
              <button
                key={s.name}
                onClick={() => setStage(s.name)}
                aria-pressed={stage === s.name}
                title={`${s.kind}${s.is_baseline ? " · baseline" : ""}`}
                className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                  stage === s.name
                    ? "bg-white text-slate-900 shadow-sm"
                    : "text-slate-500 hover:text-slate-700"
                }`}
              >
                {s.label}
                {s.is_baseline && (
                  <span className="ml-1 text-[9px] text-slate-600 uppercase">
                    base
                  </span>
                )}
              </button>
            ))}
          </div>
        </div>

        {catalog.ks.length > 0 && (
          <div>
            <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
              Neighbours (K)
            </label>
            <div
              role="group"
              aria-label="Neighbours (K)"
              className="flex gap-1 rounded-lg bg-slate-100 p-0.5"
            >
              {catalog.ks.map((n) => (
                <button
                  key={n}
                  onClick={() => setSelectedK(n)}
                  aria-pressed={selectedK === n}
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                    selectedK === n
                      ? "bg-white text-slate-900 shadow-sm"
                      : "text-slate-500 hover:text-slate-700"
                  }`}
                >
                  K={n}
                </button>
              ))}
            </div>
          </div>
        )}

        {evalSetList.length > 1 && (
          <div>
            <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
              Evaluation set
            </label>
            <select
              value={evalSetId}
              onChange={(e) => setEvalSetId(e.target.value)}
              className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700"
            >
              <option value="all">All splits</option>
              {evalSetList.map((es) => (
                <option key={es.id} value={es.id}>
                  {es.label}
                </option>
              ))}
            </select>
          </div>
        )}

        {/* Lineage chip filter — decomposes the matrix along the same
            NK / LK / PK axis used by BenchmarkEvalSet.stats counters.
            Pure UI filter, no extra request. */}
        {allCategories.length > 1 && (
          <div data-testid="benchmark-lineage-chips">
            <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
              Lineage
            </label>
            <div
              role="group"
              aria-label="Lineage decomposition"
              className="flex flex-wrap gap-1 rounded-lg bg-slate-100 p-0.5"
            >
              {LINEAGE_CHIPS.map((chip) => {
                const active = chip.key === lineage;
                return (
                  <Tooltip key={chip.key} text={chip.tooltip}>
                    <button
                      type="button"
                      onClick={() => setLineage(chip.key)}
                      aria-pressed={active}
                      className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                        active
                          ? "bg-white text-slate-900 shadow-sm"
                          : "text-slate-500 hover:text-slate-700"
                      }`}
                    >
                      {chip.label}
                    </button>
                  </Tooltip>
                );
              })}
            </div>
          </div>
        )}

        <div className="text-xs text-slate-600 ml-auto self-end">
          {filteredRows.length} cells
          {filteredRows.length !== curatedRows.length && (
            <span className="text-slate-400"> / {curatedRows.length}</span>
          )}
          {!showAll && curation.hiddenTotal > 0 && (
            <span className="text-slate-400"> ({matrix.total} all)</span>
          )}{" "}
          · {embeddingsWithData.size} embeddings · {evalSetList.length} eval set
          {evalSetList.length === 1 ? "" : "s"}
        </div>
      </div>

      {lineageHasNoMatch && (
        <div
          className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800"
          role="status"
        >
          The <strong>{activeChip.label}</strong> chip would hide every row in
          the current evaluation set (no matching {activeChip.cats.join(" / ")}{" "}
          cells exist). Showing all categories instead — pick a chip with data,
          or choose a different evaluation set above.
        </div>
      )}

      {/* Headline: honest per-task mean ± 95% CI across every model in the
          current selection. This is the f_micro_w aggregate the page leads
          with; the best-cell tables below report maxima, never the headline
          (winner's-curse, FIX-METRIC-IA). */}
      {perTaskIndex.size > 0 && (
        <section className="rounded-2xl border border-emerald-100 bg-gradient-to-br from-emerald-50/60 via-white to-cyan-50/40 shadow-sm p-4 sm:p-5">
          <div className="flex items-baseline justify-between mb-3 flex-wrap gap-2">
            <h2 className="text-sm font-semibold text-slate-800 flex items-center gap-2">
              <span aria-hidden className="text-base">
                📈
              </span>
              Per-task headline
              <Tooltip text={FMICRO_TOOLTIP}>
                <span className="ml-1 font-mono text-[11px] font-normal text-slate-600 underline decoration-dotted decoration-slate-300 underline-offset-4 cursor-help">
                  mean f_micro_w ± 95% CI
                </span>
              </Tooltip>
              <span className="ml-1 text-[11px] font-normal text-slate-500">
                across all models in the current selection
              </span>
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr>
                  <th
                    scope="col"
                    className="px-2 py-1 text-left font-medium text-slate-500"
                  ></th>
                  {aspects.map((asp) => (
                    <th
                      key={asp}
                      scope="col"
                      className="px-2 py-1 text-center text-[10px] font-medium text-slate-500 uppercase tracking-wide"
                    >
                      {asp}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {effectiveCategories.map((cat) => (
                  <tr key={cat} className="border-t border-slate-200/60">
                    <th
                      scope="row"
                      className="px-2 py-2.5 font-semibold text-slate-700 text-left"
                    >
                      {CATEGORY_TOOLTIPS[cat] ? (
                        <Tooltip text={CATEGORY_TOOLTIPS[cat]}>
                          <span className="underline decoration-dotted decoration-slate-300 underline-offset-4 cursor-help">
                            {cat}
                          </span>
                        </Tooltip>
                      ) : (
                        cat
                      )}
                    </th>
                    {aspects.map((asp) => {
                      const agg = perTaskIndex.get(`${cat}|${asp}`);
                      if (!agg) {
                        return (
                          <td
                            key={asp}
                            className="px-2 py-2.5 text-center text-slate-300 border-l border-slate-200/60"
                          >
                            —
                          </td>
                        );
                      }
                      return (
                        <td
                          key={asp}
                          className="px-2 py-2.5 text-center border-l border-slate-200/60"
                          title={`mean ${agg.mean.toFixed(3)} ± ${agg.ci95.toFixed(3)} · best cell ${agg.max.toFixed(3)} · ${agg.n_models} models`}
                        >
                          <div className="font-bold text-lg text-slate-900 tabular-nums leading-none">
                            {agg.mean.toFixed(3)}
                          </div>
                          <div className="text-[11px] text-slate-500 tabular-nums mt-1">
                            ± {agg.ci95.toFixed(3)}
                          </div>
                          <div className="text-[9px] text-slate-400 tabular-nums mt-0.5">
                            best {agg.max.toFixed(3)} · n={agg.n_models}
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="mt-3 text-[11px] text-slate-500">
            Headline = the calibrated mean across every model per task. The
            best-cell tables below report the single top model per cell (a
            maximum), which runs ~+0.02 to +0.03 above this mean and must not be
            read as the headline.
          </p>
        </section>
      )}

      <div className="mb-3">
        {/* Every number below is this metric. Named once here rather than
            repeated on each cell, but never left unstated. */}
        <MetricSelector
          value={metric}
          onChange={(k) => setMetric(k === PRIMARY_METRIC ? null : k)}
          available={availableMetrics(
            curatedRows as unknown as Record<string, unknown>[],
          )}
        />
      </div>

      {/* Global champions: best primary metric per (cat, asp) ignoring stage/K
          filters. Stable across filter changes — anchors the best-cell read. */}
      {bestPerCellGlobal.size > 0 && (
        <section className="rounded-2xl border border-blue-100 bg-gradient-to-br from-blue-50/60 via-white to-violet-50/40 shadow-sm p-4 sm:p-5">
          <div className="flex items-baseline justify-between mb-3 flex-wrap gap-2">
            <h2 className="text-sm font-semibold text-slate-800 flex items-center gap-2">
              <span aria-hidden className="text-base">
                🏆
              </span>
              Best cell per task
              <span className="ml-1 text-[11px] font-normal text-slate-500">
                single top model per cell across every stage and K · best-cell
                maximum, not the headline
              </span>
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr>
                  <th
                    scope="col"
                    className="px-2 py-1 text-left font-medium text-slate-500"
                  ></th>
                  {aspects.map((asp) => (
                    <th
                      key={asp}
                      scope="col"
                      className="px-2 py-1 text-center text-[10px] font-medium text-slate-500 uppercase tracking-wide"
                    >
                      {asp}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {effectiveCategories.map((cat) => (
                  <tr key={cat} className="border-t border-slate-200/60">
                    <th
                      scope="row"
                      className="px-2 py-2.5 font-semibold text-slate-700 text-left"
                    >
                      {CATEGORY_TOOLTIPS[cat] ? (
                        <Tooltip text={CATEGORY_TOOLTIPS[cat]}>
                          <span className="underline decoration-dotted decoration-slate-300 underline-offset-4 cursor-help">
                            {cat}
                          </span>
                        </Tooltip>
                      ) : (
                        cat
                      )}
                    </th>
                    {aspects.map((asp) => {
                      const best = bestPerCellGlobal.get(`${cat}|${asp}`);
                      if (!best) {
                        return (
                          <td
                            key={asp}
                            className="px-2 py-2.5 text-center text-slate-300"
                          >
                            —
                          </td>
                        );
                      }
                      const emb = embeddings.find(
                        (e) => e.id === best.embedding_config_id,
                      );
                      return (
                        <td
                          key={asp}
                          className="px-2 py-2.5 text-center border-l border-slate-200/60"
                          title={`${emb?.display_name ?? best.embedding_config_id} · ${stageLabel(stageList, best.stage)} · K=${best.k}`}
                        >
                          <Link
                            href={`/${locale}/instrument/evaluation/${best.evaluation_set_id}?result=${best.evaluation_result_id}`}
                            className="block group rounded-md px-1 py-0.5 hover:bg-blue-50 focus:bg-blue-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-400 transition-colors"
                          >
                            <div className="font-bold text-lg text-slate-900 group-hover:text-blue-700 tabular-nums leading-none flex items-center justify-center gap-1">
                              {metricValue(
                                best as unknown as Record<string, unknown>,
                                metric,
                              )?.toFixed(3) ?? "n/a"}
                              {isUnweighted(metric) && (
                                <Tooltip text={NOT_IA_TOOLTIP}>
                                  <span className="text-[8px] font-bold uppercase text-rose-600 cursor-help">
                                    fmax
                                  </span>
                                </Tooltip>
                              )}
                            </div>
                            <div className="text-[11px] font-medium text-slate-700 truncate max-w-[140px] mx-auto mt-1">
                              {emb?.display_name ?? "—"}
                            </div>
                            <div className="text-[10px] text-slate-500 truncate max-w-[140px] mx-auto mt-0.5">
                              {stageLabel(stageList, best.stage)}
                              <span className="mx-1 text-slate-300">·</span>
                              <span className="tabular-nums">K={best.k}</span>
                            </div>
                          </Link>
                          {/* Method-surface provenance for the champion cell.
                              hideWhenEmpty keeps the grid clean on legacy
                              cells; the /evaluation page shows the explicit
                              unknown state per result. */}
                          <EvalProvenanceBadges
                            provenance={best}
                            hideWhenEmpty
                            className="mt-1.5 justify-center"
                          />
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Leaderboard: best primary metric per (cat, asp) across every model & stage */}
      {bestPerCell.size > 0 && (
        <section className="rounded-xl border bg-white shadow-sm p-4">
          <div className="flex items-baseline justify-between mb-3">
            <h2 className="text-sm font-semibold text-slate-800">
              Best cell per task
              <span className="ml-2 text-xs font-normal text-slate-600">
                in current selection · best-cell maximum, not the headline
                {stage ? ` · stage=${stageLabel(stageList, stage)}` : ""}
                {selectedK ? ` · K=${selectedK}` : ""}
              </span>
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr>
                  <th
                    scope="col"
                    className="px-2 py-1 text-left font-medium text-slate-500"
                  ></th>
                  {aspects.map((asp) => (
                    <th
                      key={asp}
                      scope="col"
                      className="px-2 py-1 text-center text-[10px] font-medium text-slate-500 uppercase tracking-wide"
                    >
                      {asp}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {effectiveCategories.map((cat) => (
                  <tr key={cat} className="border-t">
                    <th
                      scope="row"
                      className="px-2 py-2 font-semibold text-slate-700 text-left"
                    >
                      {CATEGORY_TOOLTIPS[cat] ? (
                        <Tooltip text={CATEGORY_TOOLTIPS[cat]}>
                          <span className="underline decoration-dotted decoration-slate-300 underline-offset-4 cursor-help">
                            {cat}
                          </span>
                        </Tooltip>
                      ) : (
                        cat
                      )}
                    </th>
                    {aspects.map((asp) => {
                      const best = bestPerCell.get(`${cat}|${asp}`);
                      if (!best) {
                        return (
                          <td
                            key={asp}
                            className="px-2 py-2 text-center text-slate-300"
                          >
                            —
                          </td>
                        );
                      }
                      const emb = embeddings.find(
                        (e) => e.id === best.embedding_config_id,
                      );
                      return (
                        <td
                          key={asp}
                          className="px-2 py-2 text-center border-l"
                          title={`${emb?.display_name ?? best.embedding_config_id} · ${stageLabel(stageList, best.stage)}`}
                        >
                          <Link
                            href={`/${locale}/instrument/evaluation/${best.evaluation_set_id}?result=${best.evaluation_result_id}`}
                            className="block group rounded-md px-1 py-0.5 hover:bg-blue-50 focus:bg-blue-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-400 transition-colors"
                          >
                            <div className="font-semibold text-slate-900 group-hover:text-blue-700 tabular-nums flex items-center justify-center gap-1">
                              {metricValue(
                                best as unknown as Record<string, unknown>,
                                metric,
                              )?.toFixed(3) ?? "n/a"}
                              {isUnweighted(metric) && (
                                <Tooltip text={NOT_IA_TOOLTIP}>
                                  <span className="text-[8px] font-bold uppercase text-rose-600 cursor-help">
                                    fmax
                                  </span>
                                </Tooltip>
                              )}
                            </div>
                            <div className="text-[10px] text-slate-500 truncate max-w-[120px] mx-auto">
                              {emb?.display_name ?? "—"}
                            </div>
                            <div className="text-[9px] text-slate-600 truncate max-w-[120px] mx-auto">
                              {stageLabel(stageList, best.stage)}
                            </div>
                          </Link>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* View toggle: Heatmap (default) | Table */}
      {hasData && (
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div
            role="tablist"
            aria-label="View mode"
            className="inline-flex rounded-xl border border-slate-200 bg-white p-0.5 shadow-sm"
          >
            {(["heatmap", "table"] as const).map((mode) => {
              const active = viewMode === mode;
              return (
                <button
                  key={mode}
                  role="tab"
                  aria-selected={active}
                  onClick={() => setViewMode(mode)}
                  className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[12px] font-semibold transition-colors ${
                    active
                      ? "bg-blue-600 text-white shadow-sm"
                      : "text-slate-600 hover:bg-slate-100"
                  }`}
                >
                  <span aria-hidden>{mode === "heatmap" ? "📊" : "🔢"}</span>
                  {mode === "heatmap" ? "Heatmap" : "Table"}
                </button>
              );
            })}
          </div>
          <div className="flex items-center gap-2 text-xs text-slate-500">
            <Tooltip text={FMICRO_TOOLTIP}>
              <span className="inline-flex items-center rounded-md bg-emerald-50 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-700 ring-1 ring-inset ring-emerald-200 font-mono cursor-help">
                f_micro_w · IA-weighted
              </span>
            </Tooltip>
            <span>
              {viewMode === "heatmap"
                ? "Visual ranking per cell. Bars sorted by IA-weighted f_micro_w."
                : "Full matrix with raw numbers. Useful for export."}
            </span>
          </div>
        </div>
      )}

      {/* Matrix view */}
      {!hasData ? (
        <section className="rounded-xl border-2 border-dashed border-slate-200 bg-slate-50 p-12 text-center">
          {curatedToEmpty ? (
            <>
              <p className="text-slate-500 text-sm">
                Every result for{" "}
                <span className="font-semibold">{currentStageLabel}</span> is a
                probe or a legacy internal-frame run, hidden by the curated
                view.
              </p>
              <button
                type="button"
                onClick={() => setShowAll(true)}
                className="mt-3 inline-flex items-center gap-1 rounded-md border border-amber-300 bg-amber-50 px-3 py-1.5 text-xs font-semibold text-amber-800 hover:bg-amber-100"
              >
                Show all (incl. legacy / probes)
              </button>
            </>
          ) : (
            <>
              <p className="text-slate-500 text-sm">
                No evaluation results for{" "}
                <span className="font-semibold">{currentStageLabel}</span> yet.
              </p>
              <p className="text-slate-600 text-xs mt-2">
                Run <code>run_cafa_evaluation</code> for an embedding to
                populate this cell of the matrix.
              </p>
            </>
          )}
        </section>
      ) : viewMode === "heatmap" ? (
        <BenchmarkHeatmap
          rows={filteredRows}
          embeddings={embeddings}
          categories={effectiveCategories}
          aspects={aspects}
        />
      ) : (
        <div className="rounded-lg border bg-white shadow-sm">
          <table className="w-full text-sm">
            <thead className="protea-thead-sticky bg-slate-50">
              <tr>
                <th
                  rowSpan={2}
                  scope="col"
                  className="px-4 py-2 text-left font-medium text-slate-600 border-b sticky left-0 bg-slate-50"
                >
                  Embedding
                </th>
                {effectiveCategories.map((cat) => (
                  <th
                    key={cat}
                    colSpan={aspects.length}
                    scope="colgroup"
                    className="px-2 py-1.5 text-center font-semibold text-slate-700 border-b border-l"
                  >
                    {CATEGORY_TOOLTIPS[cat] ? (
                      <Tooltip text={CATEGORY_TOOLTIPS[cat]}>
                        <span className="underline decoration-dotted decoration-slate-300 underline-offset-4 cursor-help">
                          {cat}
                        </span>
                      </Tooltip>
                    ) : (
                      cat
                    )}
                  </th>
                ))}
              </tr>
              <tr>
                {effectiveCategories.flatMap((cat) =>
                  aspects.map((asp) => (
                    <th
                      key={`${cat}-${asp}`}
                      scope="col"
                      className="px-2 py-1 text-center text-[10px] font-medium text-slate-500 uppercase tracking-wide border-b border-l"
                    >
                      {asp}
                    </th>
                  )),
                )}
              </tr>
            </thead>
            <tbody>
              {embeddings.map((emb) => {
                const hasRow = embeddingsWithData.has(emb.id);
                return (
                  <tr
                    key={emb.id}
                    className={`border-t ${hasRow ? "" : "opacity-50"}`}
                  >
                    <td className="px-4 py-2 sticky left-0 bg-white border-r">
                      <div className="font-medium text-slate-900">
                        {emb.display_name}
                      </div>
                      <div className="text-[10px] text-slate-600 font-mono">
                        {emb.family}
                        {emb.param_count != null
                          ? ` · ${formatParams(emb.param_count)}`
                          : ""}
                      </div>
                    </td>
                    {effectiveCategories.flatMap((cat) =>
                      aspects.map((asp) => {
                        const row = rowIndex.get(cellKey(emb.id, cat, asp));
                        const best = bestPerCell.get(`${cat}|${asp}`);
                        const isWinner =
                          row &&
                          best &&
                          row.evaluation_result_id ===
                            best.evaluation_result_id;
                        const std = formatStd(row?.fmax_std ?? null);
                        const ciBand =
                          row && hasCiBand(row)
                            ? `CI95 [${row.fmax_ci_low!.toFixed(3)}, ${row.fmax_ci_high!.toFixed(3)}]`
                            : "";
                        return (
                          <td
                            key={`${emb.id}-${cat}-${asp}`}
                            className={`px-2 py-2 text-center tabular-nums border-l ${
                              isWinner ? "bg-green-50" : ""
                            }`}
                            title={
                              row
                                ? `precision=${row.precision ?? "—"} recall=${row.recall ?? "—"}${ciBand ? ` · ${ciBand}` : ""} (eval_result=${row.evaluation_result_id.slice(0, 8)}…)`
                                : "no data"
                            }
                          >
                            {row ? (
                              <Link
                                href={`/${locale}/instrument/evaluation/${row.evaluation_set_id}?result=${row.evaluation_result_id}`}
                                className="inline-flex items-baseline gap-1 rounded-md px-1 py-0.5 group hover:bg-blue-50 focus:bg-blue-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-400 transition-colors"
                              >
                                <span
                                  className={`font-semibold group-hover:text-blue-700 ${
                                    isWinner
                                      ? "text-green-700"
                                      : "text-slate-900"
                                  }`}
                                >
                                  {metricValue(
                                    row as unknown as Record<string, unknown>,
                                    metric,
                                  )?.toFixed(3) ?? "n/a"}
                                </span>
                                {isUnweighted(metric) && (
                                  <span
                                    className="text-[8px] font-bold uppercase text-rose-600"
                                    title="Not IA-weighted (legacy Fmax)"
                                  >
                                    fmax
                                  </span>
                                )}
                                {std && (
                                  <span className="text-[10px] font-normal text-slate-500 group-hover:text-blue-600 tabular-nums">
                                    {std}
                                  </span>
                                )}
                              </Link>
                            ) : (
                              <span className="text-slate-300">—</span>
                            )}
                            {row && hasCiBand(row) && (
                              <span className="mt-0.5 block text-[9px] leading-none text-slate-400 tabular-nums">
                                CI [{row.fmax_ci_low!.toFixed(3)},{" "}
                                {row.fmax_ci_high!.toFixed(3)}]
                              </span>
                            )}
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
      )}

      <p className="text-xs text-slate-600">
        Display names and stage labels come from <code>embedding_config</code>{" "}
        (DB) and <code>protea/config/benchmark.yaml</code>. Edit the YAML to
        change ordering, labels, or the baseline tag.
      </p>
    </div>
  );
}
