"use client";

import Link from "next/link";
import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { useParams, useSearchParams } from "next/navigation";
import { useTranslations } from "next-intl";
import { baseUrl } from "@/lib/api";
import { EvalProvenanceBadges } from "@/components/EvalProvenanceBadges";
import StrataPanel from "@/components/StrataPanel";
import PairedComparisons from "@/components/PairedComparisons";
import WindowPanel from "@/components/window-panel/WindowPanel";
import { RunReceipt } from "@/components/RunReceipt";
import {
  GRID_FILE,
  HEADLINE_FILE,
  describeSource,
  fileSource,
  type ArtifactsRecord,
  type FileSource,
} from "@/lib/cellSource";
import { edgeMark, headlineMaximum, maximumNote, type Maximum } from "@/lib/headlineMaximum";
import { PRIMARY_METRIC, metricTooltip, metricValue } from "@/lib/metrics";
import { armLabel, type Arm } from "@/lib/pairedComparison";

type NsMetrics = {
  /**
   * The IA-weighted micro-averaged F: ``PRIMARY_METRIC``, and the only figure
   * in this cell comparable to the LAFA / CAFA leaderboards. Optional because
   * the ``_w`` keys are written only when a real IA table reached cafaeval.
   */
  f_micro?: number;
  precision?: number;
  recall?: number;
  /** The largest IA-weighted coverage over every threshold (cafaeval's cov_max). */
  coverage_w?: number;
  /**
   * The IA-weighted coverage at the threshold f_micro_w was reported at: LAFA's
   * cov_w, the coverage printed beside the headline. Absent on results made
   * before 2026-11-21.
   */
  coverage_w_at_tau?: number;
  /** The cell's size, unweighted and weighted: the denominators of its coverage. */
  n_targets?: number;
  n_targets_w?: number;
  /**
   * Unweighted, kept for history and NOT LAFA-comparable -- the
   * EvaluationResult model says exactly that in its own docstring. This page
   * printed it as the cell value.
   */
  fmax: number;
  aupr?: number;
  smin?: number;
  coverage: number;
  n_proteins?: number;
  /**
   * Per F variant, the threshold its curve peaks at and whether that is an edge
   * of the grid (ADR-D55). The cell's `tau` belongs to the unweighted fmax; the
   * headline's own threshold is `maxima.f_micro_w.tau`.
   */
  maxima?: Record<string, Maximum | undefined>;
};

type SettingResults = Record<string, NsMetrics>;

/**
 * The two numbers LAFA prints beside the headline: its coverage at the reported
 * threshold and the cell's size, both in the weighted variant. A non-breaking
 * space keeps a cell from an older result, which records neither, the same height.
 */
function cellCompanions(cell: NsMetrics): string {
  const cov = metricValue(cell, "coverage_w_at_tau");
  const n = cell.n_targets_w ?? null;
  if (cov === null && n === null) return "\u00a0";
  const parts = [];
  if (cov !== null) parts.push(`cov ${cov.toFixed(3)}`);
  if (n !== null) parts.push(`n ${n.toLocaleString("en-US")}`);
  return parts.join(" · ");
}

/** Every figure the cell holds, each named for what it is and the file it was read from. */
function cellTooltip(cell: NsMetrics, sources: CategorySources): string {
  const parts = [metricTooltip(PRIMARY_METRIC)];
  const maximum = headlineMaximum(cell);
  if (maximum) parts.push(maximumNote(maximum));
  const cov = metricValue(cell, "coverage_w_at_tau");
  if (cov !== null) parts.push(`coverage at the reported threshold (cov_w) = ${cov.toFixed(3)}`);
  const largest = metricValue(cell, "coverage_w");
  if (largest !== null) parts.push(`largest coverage over thresholds = ${largest.toFixed(3)}`);
  if (cell.n_targets_w != null) parts.push(`cell size, weighted = ${cell.n_targets_w}`);
  if (cell.n_targets != null) parts.push(`cell size, unweighted = ${cell.n_targets}`);
  parts.push(`fmax = ${cell.fmax.toFixed(4)} (unweighted, not LAFA-comparable)`);
  if (sources.headline) parts.push(`f_micro_w and cov_w read from ${describeSource(sources.headline)}`);
  if (sources.grid) parts.push(`cell size read from ${describeSource(sources.grid)}`);
  return parts.join(" · ");
}

/** The two files a category's cells are read from, when the result wrote them. */
type CategorySources = { headline: FileSource | null; grid: FileSource | null };

function categorySources(r: EvaluationResult, cat: Category): CategorySources {
  const record = r.results?.artifacts;
  const source = (file: string) =>
    fileSource(baseUrl(), r.evaluation_set_id, r.id, record, `${cat}/${file}`);
  return { headline: source(HEADLINE_FILE), grid: source(GRID_FILE) };
}

/** The headline file of each category, as links that name the digest. */
function CellFiles({ r, label, hint }: { r: EvaluationResult; label: string; hint: string }) {
  const heads = CATEGORIES.flatMap((cat) => {
    const { headline } = categorySources(r, cat);
    return headline ? [{ cat, headline }] : [];
  });
  if (heads.length === 0) return null;
  return (
    <>
      <span
        className="ml-2 text-[10px] font-semibold uppercase tracking-wider text-slate-400"
        title={hint}
      >
        {label}
      </span>
      {heads.map(({ cat, headline }) => (
        <a
          key={cat}
          href={headline.href}
          className="font-mono text-[10px] text-blue-600 hover:underline"
          title={describeSource(headline)}
        >
          {cat} {headline.sha256 ? headline.sha256.slice(0, 8) : "no digest"}
        </a>
      ))}
    </>
  );
}

type EvaluationResult = {
  id: string;
  evaluation_set_id: string;
  prediction_set_id: string;
  scoring_config_id: string | null;
  reranker_model_id: string | null;
  reranker_config: Record<string, Record<string, string>> | null;
  job_id: string | null;
  /** The code revision the evaluating job recorded when it started. */
  code_revision: string | null;
  /**
   * How the scores were read before cafaeval thresholded them (ADR-D55): ``raw``,
   * LAFA's reading, or ``percentile_rank``, the comparison reading.
   */
  score_reading: string | null;
  /** What the evaluated set is: its model, the scoring it read and its method. */
  arm?: Arm | null;
  created_at: string;
  // Method-surface provenance (slice F-METHOD-EVAL-SURFACE). Optional:
  // null on rows written before the columns existed; the badge strip then
  // renders an explicit "unknown" state.
  frame: string | null;
  frame_digest: string | null;
  temporal_window: string | null;
  arms_enabled: Record<string, boolean> | null;
  leakage_role: string | null;
  results: ResultsRecord;
};

/** A result's record: one entry per category, and the files it wrote. */
type ResultsRecord = Partial<Record<Category, SettingResults>> & {
  artifacts?: ArtifactsRecord;
};

type EvaluationSet = {
  id: string;
  old_annotation_set_id: string;
  new_annotation_set_id: string;
  job_id: string | null;
  created_at: string;
  stats: {
    delta_proteins?: number;
    nk_proteins?: number;
    lk_proteins?: number;
    pk_proteins?: number;
    nk_annotations?: number;
    lk_annotations?: number;
    pk_annotations?: number;
    known_terms_count?: number;
  };
};

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${baseUrl()}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${path}`);
  return res.json();
}

function shortId(id: string): string {
  return id.slice(0, 8);
}

const CATEGORIES = ["NK", "LK", "PK"] as const;
type Category = (typeof CATEGORIES)[number];
const NAMESPACES = ["BPO", "MFO", "CCO"] as const;

export default function EvaluationDetailPage() {
  const params = useParams<{ locale: string; id: string }>();
  const search = useSearchParams();
  const t = useTranslations("evaluation");
  const highlightId = search.get("result");
  const [evalSet, setEvalSet] = useState<EvaluationSet | null>(null);
  const [results, setResults] = useState<EvaluationResult[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const highlightRef = useRef<HTMLTableRowElement | null>(null);
  // Which result has its strata panel open. Separate from `highlightId`,
  // which comes from the ?result= query param and exists so another page can
  // link INTO a row. Gating the panel on that alone made it unreachable for
  // anyone who navigated here normally: there was no control to open it.
  const [openStrata, setOpenStrata] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    (async () => {
      try {
        const [set, rows] = await Promise.all([
          fetchJson<EvaluationSet>(`/annotations/evaluation-sets/${params.id}`),
          fetchJson<EvaluationResult[]>(
            `/annotations/evaluation-sets/${params.id}/results`,
          ),
        ]);
        if (cancelled) return;
        setEvalSet(set);
        setResults(rows);
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [params.id]);

  useEffect(() => {
    if (highlightRef.current) {
      highlightRef.current.scrollIntoView({
        behavior: "smooth",
        block: "center",
      });
    }
  }, [results.length, highlightId]);

  const sortedResults = useMemo(() => {
    return [...results].sort(
      (a, b) =>
        new Date(b.created_at).getTime() - new Date(a.created_at).getTime(),
    );
  }, [results]);
  // Whether any headline on the page sits at an edge of the threshold grid, so
  // the legend explains the mark only where there is one to explain.
  const anyEdge = useMemo(
    () =>
      results.some((r) =>
        CATEGORIES.some((cat) =>
          NAMESPACES.some((ns) => {
            const cell = r.results?.[cat]?.[ns];
            return cell ? edgeMark(headlineMaximum(cell)) !== "" : false;
          }),
        ),
      ),
    [results],
  );

  return (
    <main className="mx-auto max-w-6xl px-4 py-6">
      <div className="mb-4 flex items-center gap-3 text-sm">
        <Link
          href={`/${params.locale}/evaluation`}
          className="text-blue-600 hover:underline"
        >
          {t("backToList", { default: "Back to evaluation sets" })}
        </Link>
        <span className="text-slate-400">·</span>
        <span className="font-mono text-slate-600" title={params.id}>
          set {shortId(params.id)}
        </span>
      </div>

      <h1 className="mb-4 text-2xl font-semibold text-slate-900">
        {t("detailTitle", { default: "Evaluation set detail" })}
      </h1>

      {loading && (
        <div className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-slate-600 shadow-sm">
          {t("loading", { default: "Loading evaluation set..." })}
        </div>
      )}

      {error && (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-4 text-sm text-red-700">
          {error}
        </div>
      )}

      {!loading && !error && evalSet && (
        <>
          <section className="mb-6 rounded-lg border bg-white px-4 py-4 shadow-sm">
            <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-600">
              {t("statsTitle", { default: "Set stats" })}
            </h2>
            <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm text-slate-700 md:grid-cols-4">
              <div>
                <dt className="text-xs text-slate-500">
                  {t("statDelta", { default: "delta proteins" })}
                </dt>
                <dd className="font-mono">
                  {evalSet.stats.delta_proteins ?? 0}
                </dd>
              </div>
              <div>
                <dt className="text-xs text-slate-500">NK</dt>
                <dd className="font-mono">
                  {evalSet.stats.nk_proteins ?? 0} /{" "}
                  {evalSet.stats.nk_annotations ?? 0}
                </dd>
              </div>
              <div>
                <dt className="text-xs text-slate-500">LK</dt>
                <dd className="font-mono">
                  {evalSet.stats.lk_proteins ?? 0} /{" "}
                  {evalSet.stats.lk_annotations ?? 0}
                </dd>
              </div>
              <div>
                <dt className="text-xs text-slate-500">PK</dt>
                <dd className="font-mono">
                  {evalSet.stats.pk_proteins ?? 0} /{" "}
                  {evalSet.stats.pk_annotations ?? 0}
                </dd>
              </div>
            </dl>
          </section>

          {/* Highlighted-result quick-jump cards: when the page is reached
              from the benchmark heatmap (?result=<id>) we surface two
              shortcut links so the reviewer doesn't have to scan the table
              to find the model and the prediction set behind the cell. */}
          {highlightId &&
            (() => {
              const hr = sortedResults.find((r) => r.id === highlightId);
              if (!hr) return null;
              return (
                <section
                  className="mb-6 flex flex-wrap gap-3"
                  aria-label={t("jumpToContext", {
                    default: "Jump to context",
                  })}
                >
                  {hr.reranker_model_id && (
                    <Link
                      href={`/${params.locale}/reranker#${hr.reranker_model_id}`}
                      className="group flex-1 min-w-[220px] rounded-lg border border-blue-200 bg-blue-50/60 px-4 py-3 hover:bg-blue-50 transition-colors"
                    >
                      <p className="text-[10px] font-semibold uppercase tracking-wider text-blue-700">
                        {t("modelUsed", { default: "Reranker model used" })}
                      </p>
                      <p className="mt-1 text-sm font-mono text-slate-900 group-hover:text-blue-700">
                        {shortId(hr.reranker_model_id)}{" "}
                        <span aria-hidden>→</span>
                      </p>
                    </Link>
                  )}
                  <Link
                    href={`/${params.locale}/functional-annotation/${hr.prediction_set_id}`}
                    className="group flex-1 min-w-[220px] rounded-lg border border-blue-200 bg-blue-50/60 px-4 py-3 hover:bg-blue-50 transition-colors"
                  >
                    <p className="text-[10px] font-semibold uppercase tracking-wider text-blue-700">
                      {t("proteinExamples", {
                        default: "Protein examples (prediction set)",
                      })}
                    </p>
                    <p className="mt-1 text-sm font-mono text-slate-900 group-hover:text-blue-700">
                      {shortId(hr.prediction_set_id)} <span aria-hidden>→</span>
                    </p>
                  </Link>
                </section>
              );
            })()}

          {/* The strata were one small toggle per result row; the reader who comes for
              them gets a plain way in (the author, 2026-11-25). */}
          <Link
            href={`/${params.locale}/instrument/evaluation/${params.id}?tab=strata`}
            className="mb-4 flex items-center justify-between gap-4 rounded-lg border border-blue-200 bg-blue-50 px-4 py-3 hover:bg-blue-100"
          >
            <span>
              <span className="block text-sm font-semibold text-blue-900">{t("strataCta.title")}</span>
              <span className="block text-xs text-blue-800">{t("strataCta.body")}</span>
            </span>
            <span aria-hidden className="text-lg text-blue-700">→</span>
          </Link>
          <WindowPanel evaluationSetId={params.id} />

          <section className="rounded-lg border bg-white shadow-sm">
            <header className="border-b px-4 py-3">
              <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-600">
                {t("resultsTitle", {
                  count: sortedResults.length,
                  default: "Results ({count})",
                })}
              </h2>
            </header>
            {sortedResults.length > 0 ? (
              <p className="border-b px-4 py-2 text-xs text-slate-600">
                {t("cellLegend")}
                {anyEdge ? ` ${t("edgeLegend")}` : null}
              </p>
            ) : null}
            {sortedResults.length === 0 ? (
              <div className="px-4 py-10 text-center text-sm text-slate-600">
                {t("noResults", {
                  default: "No evaluation results yet for this set.",
                })}
              </div>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-slate-50 text-xs font-semibold uppercase tracking-wide text-slate-600">
                    <th className="px-3 py-2 text-left">id</th>
                    <th className="px-3 py-2 text-left">prediction</th>
                    <th className="px-3 py-2 text-left">scoring</th>
                    <th className="px-3 py-2 text-left">reranker</th>
                    {CATEGORIES.flatMap((cat) =>
                      NAMESPACES.map((ns) => (
                        <th
                          key={`${cat}-${ns}`}
                          className="px-2 py-2 text-right"
                        >
                          {cat}/{ns}
                        </th>
                      )),
                    )}
                  </tr>
                </thead>
                <tbody>
                  {sortedResults.map((r) => {
                    const isHighlight = highlightId === r.id;
                    return (
                      <Fragment key={r.id}>
                        <tr
                          ref={isHighlight ? highlightRef : null}
                          className={`text-slate-700 ${isHighlight ? "bg-amber-50 ring-2 ring-amber-300" : "hover:bg-slate-50"}`}
                        >
                          <td
                            className="px-3 py-2 font-mono text-xs"
                            title={r.id}
                          >
                            {/* Where the row's numbers came from: the result,
                              the job that computed them and the code it ran. */}
                            <div className="flex flex-col gap-0.5">
                              <span>{shortId(r.id)}</span>
                              {r.job_id ? (
                                <Link
                                  href={`/${params.locale}/instrument/jobs/${r.job_id}`}
                                  className="text-[10px] text-blue-600 hover:underline"
                                  title={r.job_id}
                                >
                                  job {shortId(r.job_id)}
                                </Link>
                              ) : null}
                              {r.code_revision ? (
                                <a
                                  href={`https://github.com/frapercan/PROTEA/commit/${r.code_revision}`}
                                  target="_blank"
                                  rel="noreferrer"
                                  className="text-[10px] text-slate-500 hover:underline"
                                  title={r.code_revision}
                                >
                                  {r.code_revision.slice(0, 7)}
                                </a>
                              ) : null}
                            </div>
                          </td>
                          {/* Forward link to the prediction set so reviewers can
                            jump straight to the proteins+candidate-GO rows
                            that this evaluation scored. Closes the audit's
                            'no examples link from eval set' dead-end. */}
                          <td className="px-3 py-2 font-mono text-xs">
                            <Link
                              href={`/${params.locale}/functional-annotation/${r.prediction_set_id}`}
                              className="text-blue-600 hover:underline"
                              title={`${r.prediction_set_id} — ${t("openPredictionSet", { default: "Open prediction set" })}`}
                            >
                              {shortId(r.prediction_set_id)}
                            </Link>
                            {armLabel(r.arm) ? (
                              <div className="text-[10px] text-slate-500">{armLabel(r.arm)}</div>
                            ) : null}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs">
                            {r.scoring_config_id
                              ? shortId(r.scoring_config_id)
                              : "—"}
                            {r.score_reading ? (
                              <div
                                className="text-[10px] text-slate-500"
                                title="how the scores were read before the thresholds (ADR-D55)"
                              >
                                {r.score_reading === "percentile_rank" ? "read by percentile" : `read ${r.score_reading}`}
                              </div>
                            ) : null}
                          </td>
                          {/* Forward link to the reranker registry so reviewers
                            can inspect the booster (provenance, metrics,
                            feature importance) that produced this row.
                            The detail page is the list view filtered/anchored
                            by id since reranker has no /reranker/[id] route
                            yet. Closes the audit's 'no link from eval to
                            reranker model' dead-end. */}
                          <td className="px-3 py-2 font-mono text-xs">
                            {r.reranker_model_id ? (
                              <Link
                                href={`/${params.locale}/reranker#${r.reranker_model_id}`}
                                className="text-blue-600 hover:underline"
                                title={`${r.reranker_model_id} — ${t("openReranker", { default: "Open reranker model" })}`}
                              >
                                {shortId(r.reranker_model_id)}
                              </Link>
                            ) : (
                              "—"
                            )}
                          </td>
                          {CATEGORIES.flatMap((cat) => {
                            const sources = categorySources(r, cat);
                            return NAMESPACES.map((ns) => {
                              const cell = r.results?.[cat]?.[ns];
                              return (
                                <td
                                  key={`${r.id}-${cat}-${ns}`}
                                  className="px-2 py-2 text-right font-mono tabular-nums"
                                  title={cell ? cellTooltip(cell, sources) : "no data"}
                                >
                                  {cell === undefined ? (
                                    "—"
                                  ) : (
                                    <div className="flex flex-col items-end leading-tight">
                                      {/* metricValue returns null for an absent key rather than
                                        zero, so a cell scored without an IA says so instead of
                                        borrowing the unweighted figure and reading as the
                                        headline. */}
                                      <span>
                                        {metricValue(cell, PRIMARY_METRIC)?.toFixed(3) ?? "no IA"}
                                        {/* A maximum at an edge of the threshold grid is an edge,
                                          not a maximum: marked here, said in full in the tooltip. */}
                                        <span className="text-amber-700">
                                          {edgeMark(headlineMaximum(cell))}
                                        </span>
                                      </span>
                                      <span className="text-[10px] text-slate-500">
                                        {cellCompanions(cell)}
                                      </span>
                                    </div>
                                  )}
                                </td>
                              );
                            });
                          })}
                        </tr>
                        {/* Provenance sub-row: frame / window / arms / leakage
                          badges for this result. Always rendered (with an
                          explicit "unknown" empty state) so a number is
                          never shown without its scoring context. */}
                        <tr
                          className={`border-b ${isHighlight ? "bg-amber-50" : ""}`}
                        >
                          <td
                            colSpan={4 + CATEGORIES.length * NAMESPACES.length}
                            className="px-3 pb-2 pt-0"
                          >
                            <div className="flex items-center gap-2">
                              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
                                {t("provenanceLabel", {
                                  default: "provenance",
                                })}
                              </span>
                              <EvalProvenanceBadges
                                provenance={{
                                  frame: r.frame,
                                  frame_digest: r.frame_digest,
                                  temporal_window: r.temporal_window,
                                  arms_enabled: r.arms_enabled,
                                  leakage_role: r.leakage_role,
                                }}
                              />
                              {/* The file each category's cells are read from, served by
                                itself, with the sha256 the result recorded for it. */}
                              <CellFiles r={r} label={t("sourcesLabel")} hint={t("sourcesHint")} />
                              <button
                                type="button"
                                onClick={() =>
                                  setOpenStrata((cur) =>
                                    cur === r.id ? null : r.id,
                                  )
                                }
                                className="ml-auto rounded-md px-2 py-0.5 text-[10px] font-medium text-slate-600 ring-1 ring-inset ring-slate-300 hover:bg-slate-50"
                                aria-expanded={
                                  isHighlight || openStrata === r.id
                                }
                              >
                                {isHighlight || openStrata === r.id
                                  ? "hide strata"
                                  : "show strata"}
                              </button>
                            </div>
                          </td>
                        </tr>
                        {/* Strata for the highlighted result only. One fetch per
                          rendered panel, so mounting it on every row would issue
                          a request per result for a table nobody has asked to
                          read yet. */}
                        {(isHighlight || openStrata === r.id) && (
                          <tr className="border-b bg-amber-50">
                            <td
                              colSpan={
                                4 + CATEGORIES.length * NAMESPACES.length
                              }
                              className="px-3 pb-3 pt-0"
                            >
                              {/* What it was measured with, before where it holds. A
                                  reader who opens a row is asking both, and the strata
                                  answer only the second: a per-stratum table of a run
                                  whose K and donor policy are unknown is a
                                  decomposition of an unknown. */}
                              {r.prediction_set_id ? (
                                <div className="mb-3">
                                  <RunReceipt predictionSetId={r.prediction_set_id} />
                                </div>
                              ) : null}
                              <StrataPanel
                                evaluationResultId={r.id}
                                predictionSetId={r.prediction_set_id}
                                evaluationSetId={r.evaluation_set_id}
                                locale={params.locale}
                              />
                            </td>
                          </tr>
                        )}
                      </Fragment>
                    );
                  })}
                </tbody>
              </table>
            )}
          </section>
          {/* Each arm against a baseline, per cell, with a paired interval. */}
          <PairedComparisons
            evaluationSetId={params.id}
            results={sortedResults}
            locale={params.locale}
          />
        </>
      )}
    </main>
  );
}
