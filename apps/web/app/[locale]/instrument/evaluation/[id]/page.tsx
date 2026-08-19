"use client";

import Link from "next/link";
import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { useParams, useSearchParams } from "next/navigation";
import { useTranslations } from "next-intl";
import { baseUrl } from "@/lib/api";
import { EvalProvenanceBadges } from "@/components/EvalProvenanceBadges";
import StrataPanel from "@/components/StrataPanel";

type NsMetrics = {
  fmax: number;
  aupr?: number;
  smin?: number;
  coverage: number;
  n_proteins?: number;
};

type SettingResults = Record<string, NsMetrics>;

type EvaluationResult = {
  id: string;
  evaluation_set_id: string;
  prediction_set_id: string;
  scoring_config_id: string | null;
  reranker_model_id: string | null;
  reranker_config: Record<string, Record<string, string>> | null;
  job_id: string | null;
  created_at: string;
  // Method-surface provenance (slice F-METHOD-EVAL-SURFACE). Optional:
  // null on rows written before the columns existed; the badge strip then
  // renders an explicit "unknown" state.
  frame: string | null;
  temporal_window: string | null;
  arms_enabled: Record<string, boolean> | null;
  leakage_role: string | null;
  results: Record<string, SettingResults>;
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

          <section className="rounded-lg border bg-white shadow-sm">
            <header className="border-b px-4 py-3">
              <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-600">
                {t("resultsTitle", {
                  count: sortedResults.length,
                  default: "Results ({count})",
                })}
              </h2>
            </header>
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
                            {shortId(r.id)}
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
                          </td>
                          <td className="px-3 py-2 font-mono text-xs">
                            {r.scoring_config_id
                              ? shortId(r.scoring_config_id)
                              : "—"}
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
                          {CATEGORIES.flatMap((cat) =>
                            NAMESPACES.map((ns) => {
                              const cell = r.results?.[cat]?.[ns];
                              return (
                                <td
                                  key={`${r.id}-${cat}-${ns}`}
                                  className="px-2 py-2 text-right font-mono tabular-nums"
                                  title={
                                    cell
                                      ? `fmax=${cell.fmax.toFixed(4)} coverage=${cell.coverage.toFixed(3)}`
                                      : "no data"
                                  }
                                >
                                  {cell ? cell.fmax.toFixed(3) : "—"}
                                </td>
                              );
                            }),
                          )}
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
                                  temporal_window: r.temporal_window,
                                  arms_enabled: r.arms_enabled,
                                  leakage_role: r.leakage_role,
                                }}
                              />
                            </div>
                          </td>
                        </tr>
                        {/* Strata for the highlighted result only. One fetch per
                          rendered panel, so mounting it on every row would issue
                          a request per result for a table nobody has asked to
                          read yet. */}
                        {isHighlight && (
                          <tr className="border-b bg-amber-50">
                            <td
                              colSpan={
                                4 + CATEGORIES.length * NAMESPACES.length
                              }
                              className="px-3 pb-3 pt-0"
                            >
                              <StrataPanel evaluationResultId={r.id} />
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
        </>
      )}
    </main>
  );
}
