"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useTranslations } from "next-intl";
import Link from "next/link";
import { getShowcase, type ShowcaseData } from "../../lib/api";
import { AnnotateForm } from "../../components/AnnotateForm";

const ASPECTS = ["MFO", "BPO", "CCO"] as const;
const ASPECT_LABELS: Record<string, string> = {
  MFO: "Molecular Function",
  BPO: "Biological Process",
  CCO: "Cellular Component",
};

const CATEGORIES = ["NK", "LK", "PK"] as const;
const CATEGORY_LABELS: Record<string, string> = {
  NK: "No Knowledge",
  LK: "Limited Knowledge",
  PK: "Partial Knowledge",
};

const STAGE_ICONS: Record<string, string> = {
  sequences: "Aa",
  embeddings: "E",
  predictions: "K",
  reranker_models: "R",
  evaluations: "F",
};

const STAGE_I18N: Record<string, string> = {
  sequences: "stageSequences",
  embeddings: "stageEmbeddings",
  predictions: "stageKnn",
  reranker_models: "stageReranker",
  evaluations: "stageEvaluation",
};

const STAGE_LABELS: Record<string, string> = {
  baseline: "pipelineStageBaseline",
  alignment_weighted: "pipelineStageAlignmentWeighted",
  reranker: "pipelineStageReranker",
};

const STAGE_BADGE: Record<string, string> = {
  baseline: "bg-gray-100 text-gray-700",
  alignment_weighted: "bg-amber-100 text-amber-800",
  reranker: "bg-blue-100 text-blue-800",
};

function formatParamCount(n: number | null): string {
  if (n == null) return "";
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(n >= 10_000_000_000 ? 0 : 1)}B`;
  if (n >= 1_000_000) return `${Math.round(n / 1_000_000)}M`;
  return `${n}`;
}

export default function HomePage() {
  const t = useTranslations("home");
  const router = useRouter();
  const [data, setData] = useState<ShowcaseData | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    getShowcase().then(setData).catch((e) => setError(e.message));
  }, []);

  if (error) {
    return (
      <div className="max-w-5xl mx-auto px-4 sm:px-6 py-12">
        <div className="rounded-lg border border-red-200 bg-red-50 p-6 text-center">
          <p className="text-red-800 text-sm">{error}</p>
          <button
            onClick={() => {
              setError(null);
              getShowcase()
                .then(setData)
                .catch((e) => setError(e.message));
            }}
            className="mt-3 text-sm text-red-600 underline hover:text-red-800"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="max-w-5xl mx-auto px-4 sm:px-6 py-12 space-y-8">
        <div className="h-8 w-96 bg-gray-100 rounded animate-pulse" />
        <div className="h-32 bg-gray-100 rounded-lg animate-pulse" />
        <div className="h-48 bg-gray-100 rounded-lg animate-pulse" />
      </div>
    );
  }

  const best = data.best;
  const paramBadge = best ? formatParamCount(best.embedding.param_count) : "";

  // Derive a per-aspect summary (mean over the 3 categories) from the flat
  // per_cell list the backend returns, so we can show 3 big Fmax tiles without
  // imposing a specific category on the user.
  const perAspect: Record<string, { sum: number; count: number }> = {};
  if (best) {
    for (const cell of best.per_cell) {
      if (!perAspect[cell.aspect]) perAspect[cell.aspect] = { sum: 0, count: 0 };
      perAspect[cell.aspect].sum += cell.fmax;
      perAspect[cell.aspect].count += 1;
    }
  }

  return (
    <div className="max-w-5xl mx-auto px-4 sm:px-6 py-8 space-y-10">
      {/* ── Hero ──────────────────────────────────────────────────── */}
      <section className="text-center space-y-3">
        <h1 className="text-3xl sm:text-4xl font-bold text-gray-900 tracking-tight">
          PROTEA
        </h1>
        <p className="text-lg text-gray-500 max-w-2xl mx-auto">{t("subtitle")}</p>
      </section>

      {/* ── Annotate form ─────────────────────────────────────────── */}
      <AnnotateForm />

      {/* ── Best result spotlight ─────────────────────────────────── */}
      {best ? (
        <section>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider">
              {t("bestOverall")}
            </h2>
            <Link
              href="/benchmark"
              className="text-xs text-blue-600 hover:text-blue-800 underline underline-offset-2"
            >
              {t("viewBenchmark")} →
            </Link>
          </div>

          <div className="rounded-2xl border bg-white p-6 shadow-sm">
            <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-4">
              <div>
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="text-2xl font-bold text-gray-900">
                    {best.embedding.display_name}
                  </span>
                  {paramBadge && (
                    <span className="rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-600 tabular-nums">
                      {paramBadge}
                    </span>
                  )}
                  <span
                    className={`rounded-full px-2 py-0.5 text-xs font-medium ${STAGE_BADGE[best.stage]}`}
                  >
                    {t(STAGE_LABELS[best.stage] as any)}
                  </span>
                </div>
                <div className="text-xs text-gray-400 mt-1 font-mono">
                  {best.embedding.model_name}
                </div>
              </div>

              <div className="text-right">
                <div className="text-4xl font-bold text-gray-900 tabular-nums">
                  {best.avg_fmax.toFixed(3)}
                </div>
                <div className="text-xs text-gray-500 mt-1">{t("avgFmaxAcrossCells")}</div>
              </div>
            </div>

            {/* Per-aspect mini tiles (mean across NK/LK/PK) */}
            <div className="mt-5 grid grid-cols-3 gap-3">
              {ASPECTS.map((aspect) => {
                const agg = perAspect[aspect];
                const value = agg ? agg.sum / agg.count : null;
                return (
                  <div
                    key={aspect}
                    className="rounded-lg border bg-gray-50 p-3 text-center"
                    title={ASPECT_LABELS[aspect]}
                  >
                    <div className="text-xl font-semibold text-gray-900 tabular-nums">
                      {value != null ? value.toFixed(3) : "—"}
                    </div>
                    <div className="text-[10px] uppercase tracking-wide text-gray-500 mt-1">
                      {aspect}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </section>
      ) : (
        <section className="rounded-xl border-2 border-dashed border-gray-200 bg-gray-50 p-8 text-center">
          <p className="text-gray-500">{t("noDataYet")}</p>
          <Link
            href="/proteins"
            className="mt-4 inline-block rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 transition-colors"
          >
            {t("getStarted")}
          </Link>
        </section>
      )}

      {/* ── Pipeline diagram ──────────────────────────────────────── */}
      <section>
        <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">
          {t("pipeline")}
        </h2>
        <div className="flex flex-col sm:flex-row items-center justify-center gap-2 sm:gap-0">
          {data.pipeline_stages.map((stage, i) => (
            <div key={stage.name} className="flex flex-col sm:flex-row items-center">
              {i > 0 && (
                <div className="text-gray-300 text-xl sm:mx-2 rotate-90 sm:rotate-0 my-1 sm:my-0 select-none">
                  &rarr;
                </div>
              )}
              <button
                onClick={() => router.push(stage.href)}
                className="group relative flex flex-col items-center justify-center w-28 h-20 rounded-lg border-2 border-gray-200 bg-white hover:border-blue-400 hover:shadow-md transition-all cursor-pointer"
              >
                <span className="text-xs font-bold text-gray-400 group-hover:text-blue-500 transition-colors">
                  {STAGE_ICONS[stage.name] ?? stage.name.slice(0, 3).toUpperCase()}
                </span>
                <span className="text-xs font-medium text-gray-700 mt-1">
                  {t(STAGE_I18N[stage.name] as any)}
                </span>
                <span className="text-[10px] text-gray-400 tabular-nums mt-0.5">
                  {stage.count.toLocaleString()}
                </span>
              </button>
            </div>
          ))}
          {/* LLM stage (future) */}
          <div className="flex flex-col sm:flex-row items-center">
            <div className="text-gray-300 text-xl sm:mx-2 rotate-90 sm:rotate-0 my-1 sm:my-0 select-none">
              &rarr;
            </div>
            <div className="flex flex-col items-center justify-center w-28 h-20 rounded-lg border-2 border-dashed border-gray-200 bg-gray-50">
              <span className="text-xs font-bold text-gray-300">LLM</span>
              <span className="text-xs font-medium text-gray-400 mt-1">{t("stageLlm")}</span>
              <span className="text-[10px] text-gray-300 mt-0.5">soon</span>
            </div>
          </div>
        </div>
      </section>

      {/* ── Stats bar ─────────────────────────────────────────────── */}
      <section>
        <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">
          {t("stats")}
        </h2>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {(
            [
              ["proteins", data.counts.proteins],
              ["sequences", data.counts.sequences],
              ["embeddings", data.counts.embeddings],
              ["predictions", data.counts.predictions],
            ] as [string, number][]
          ).map(([key, count]) => (
            <div key={key} className="rounded-lg border bg-white p-3 text-center">
              <div className="text-2xl font-bold text-gray-900 tabular-nums">
                {count.toLocaleString()}
              </div>
              <div className="text-xs text-gray-500 mt-1">{t(key as any)}</div>
            </div>
          ))}
        </div>
      </section>

      {/* ── CTAs ──────────────────────────────────────────────────── */}
      <section className="flex flex-col sm:flex-row items-center justify-center gap-3 pt-2">
        <Link
          href="/benchmark"
          className="rounded-md bg-blue-600 px-6 py-2.5 text-sm font-medium text-white hover:bg-blue-700 transition-colors"
        >
          {t("exploreResults")}
        </Link>
        <a
          href="#annotate-form"
          className="rounded-md border border-gray-300 bg-white px-6 py-2.5 text-sm font-medium text-gray-700 hover:bg-gray-50 transition-colors"
        >
          {t("annotateProteins")}
        </a>
      </section>
    </div>
  );
}
