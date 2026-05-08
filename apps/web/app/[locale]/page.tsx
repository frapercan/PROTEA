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

const ASPECT_COLORS: Record<string, { bg: string; text: string; ring: string }> = {
  MFO: { bg: "bg-blue-50", text: "text-blue-700", ring: "ring-blue-100" },
  BPO: { bg: "bg-violet-50", text: "text-violet-700", ring: "ring-violet-100" },
  CCO: { bg: "bg-emerald-50", text: "text-emerald-700", ring: "ring-emerald-100" },
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
  baseline: "bg-slate-100 text-slate-700 ring-1 ring-inset ring-slate-200",
  alignment_weighted: "bg-amber-50 text-amber-800 ring-1 ring-inset ring-amber-200",
  reranker: "bg-blue-50 text-blue-800 ring-1 ring-inset ring-blue-200",
};

const STAT_ACCENTS: Record<string, string> = {
  proteins: "from-blue-500/10 to-blue-500/0 text-blue-700",
  sequences: "from-violet-500/10 to-violet-500/0 text-violet-700",
  embeddings: "from-emerald-500/10 to-emerald-500/0 text-emerald-700",
  predictions: "from-amber-500/10 to-amber-500/0 text-amber-700",
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
      <div className="mx-auto max-w-3xl px-4 sm:px-6 py-12">
        <div className="rounded-2xl border border-red-200 bg-red-50/70 p-8 text-center shadow-sm">
          <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-red-100 text-red-600 text-lg">
            !
          </div>
          <p className="text-red-800 text-sm">{error}</p>
          <button
            onClick={() => {
              setError(null);
              getShowcase()
                .then(setData)
                .catch((e) => setError(e.message));
            }}
            className="mt-4 rounded-lg bg-red-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-red-700 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="mx-auto max-w-6xl px-4 sm:px-6 py-12 space-y-8">
        <div className="h-10 w-2/3 max-w-md bg-slate-200/70 rounded-lg animate-pulse mx-auto" />
        <div className="h-6 w-1/2 max-w-sm bg-slate-200/60 rounded animate-pulse mx-auto" />
        <div className="h-40 bg-white/60 border border-slate-200 rounded-2xl animate-pulse" />
        <div className="h-56 bg-white/60 border border-slate-200 rounded-2xl animate-pulse" />
      </div>
    );
  }

  const best = data.best;
  const paramBadge = best ? formatParamCount(best.embedding.param_count) : "";

  // Derive a per-aspect summary (mean over the 3 categories) from the flat
  // per_cell list the backend returns.
  const perAspect: Record<string, { sum: number; count: number }> = {};
  if (best) {
    for (const cell of best.per_cell) {
      if (!perAspect[cell.aspect]) perAspect[cell.aspect] = { sum: 0, count: 0 };
      perAspect[cell.aspect].sum += cell.fmax;
      perAspect[cell.aspect].count += 1;
    }
  }

  return (
    <div className="space-y-12 lg:space-y-14">
      {/* ── Hero ──────────────────────────────────────────────────── */}
      <section className="relative overflow-hidden rounded-3xl border border-slate-200 bg-white shadow-sm">
        <div className="absolute inset-0 protea-grid-bg opacity-60 pointer-events-none" />
        <div
          aria-hidden
          className="absolute -top-32 -right-24 h-80 w-80 rounded-full bg-gradient-to-br from-blue-200 via-indigo-200 to-violet-200 blur-3xl opacity-50 pointer-events-none"
        />
        <div
          aria-hidden
          className="absolute -bottom-32 -left-24 h-80 w-80 rounded-full bg-gradient-to-tr from-emerald-100 via-cyan-100 to-blue-100 blur-3xl opacity-40 pointer-events-none"
        />
        <div className="relative mx-auto max-w-4xl text-center px-6 py-12 sm:py-16 space-y-5">
          <div className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white/80 px-3 py-1 text-[11px] font-medium text-slate-600 shadow-sm">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-500 animate-pulse" />
            Protein Functional Embedding-based Annotation
          </div>
          <h1 className="text-5xl sm:text-6xl lg:text-7xl font-bold tracking-tight">
            <span className="protea-gradient-text">PROTEA</span>
          </h1>
          <p className="mx-auto max-w-2xl text-lg sm:text-xl leading-relaxed text-slate-600">
            {t("subtitle")}
          </p>
        </div>
      </section>

      {/* ── Annotate form ─────────────────────────────────────────── */}
      {/* `id` powers the "Annotate proteins" CTA below; scroll-mt clears
           the sticky h-16 header so the form lands fully visible. */}
      <section id="annotate-form" className="mx-auto max-w-4xl scroll-mt-24">
        <AnnotateForm />
      </section>

      {/* ── Best result spotlight ─────────────────────────────────── */}
      {best ? (
        <section className="mx-auto max-w-5xl">
          <div className="flex items-end justify-between mb-4">
            <div>
              <h2 className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">
                {t("bestOverall")}
              </h2>
              <p className="text-sm text-slate-500 mt-1">
                Top performing model across NK / LK / PK categories
              </p>
            </div>
            <Link
              href="/benchmark"
              className="text-[13px] font-medium text-blue-600 hover:text-blue-800 transition-colors inline-flex items-center gap-1"
            >
              {t("viewBenchmark")}
              <span aria-hidden>→</span>
            </Link>
          </div>

          <div className="relative overflow-hidden rounded-3xl border border-slate-200 bg-white p-6 sm:p-8 shadow-sm">
            <div
              aria-hidden
              className="absolute right-0 top-0 h-40 w-40 rounded-full bg-gradient-to-br from-blue-100 to-violet-100 blur-3xl opacity-50 pointer-events-none"
            />

            <div className="relative flex flex-col lg:flex-row lg:items-start lg:justify-between gap-6">
              <div className="min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="text-2xl sm:text-3xl font-bold text-slate-900 tracking-tight truncate">
                    {best.embedding.display_name}
                  </span>
                  {paramBadge && (
                    <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700 tabular-nums ring-1 ring-inset ring-slate-200">
                      {paramBadge}
                    </span>
                  )}
                  <span
                    className={`rounded-full px-2.5 py-1 text-xs font-semibold ${STAGE_BADGE[best.stage]}`}
                  >
                    {t(STAGE_LABELS[best.stage] as any)}
                  </span>
                </div>
                <div className="text-[12px] text-slate-400 mt-2 font-mono break-all">
                  {best.embedding.model_name}
                </div>
              </div>

              <div className="flex items-baseline gap-2 lg:flex-col lg:items-end lg:gap-1 lg:text-right shrink-0">
                <div className="text-5xl sm:text-6xl font-bold tabular-nums protea-gradient-text leading-none">
                  {best.avg_fmax.toFixed(3)}
                </div>
                <div className="text-[11px] uppercase tracking-[0.14em] text-slate-500 font-medium">
                  {t("avgFmaxAcrossCells")}
                </div>
              </div>
            </div>

            {/* Per-aspect mini tiles (mean across NK/LK/PK) */}
            <div className="relative mt-7 grid grid-cols-3 gap-3 sm:gap-4">
              {ASPECTS.map((aspect) => {
                const agg = perAspect[aspect];
                const value = agg ? agg.sum / agg.count : null;
                const color = ASPECT_COLORS[aspect];
                return (
                  <div
                    key={aspect}
                    className={`rounded-2xl ${color.bg} p-4 text-center ring-1 ring-inset ${color.ring} transition-transform hover:-translate-y-0.5`}
                    title={ASPECT_LABELS[aspect]}
                  >
                    <div className={`text-2xl sm:text-3xl font-bold tabular-nums ${color.text}`}>
                      {value != null ? value.toFixed(3) : "—"}
                    </div>
                    <div className={`text-[10px] uppercase tracking-[0.14em] mt-1.5 font-semibold ${color.text} opacity-80`}>
                      {aspect}
                    </div>
                    <div className="text-[10px] text-slate-500 mt-0.5">
                      {ASPECT_LABELS[aspect]}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </section>
      ) : (
        <section className="mx-auto max-w-3xl rounded-3xl border-2 border-dashed border-slate-200 bg-white/60 p-10 text-center">
          <p className="text-slate-500 text-base">{t("noDataYet")}</p>
          <Link
            href="/proteins"
            className="mt-5 inline-flex items-center gap-2 rounded-xl bg-blue-600 px-5 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-blue-700 transition-colors"
          >
            {t("getStarted")}
            <span aria-hidden>→</span>
          </Link>
        </section>
      )}

      {/* ── Pipeline diagram ──────────────────────────────────────── */}
      <section className="mx-auto max-w-6xl">
        <div className="mb-4">
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">
            {t("pipeline")}
          </h2>
          <p className="text-sm text-slate-500 mt-1">
            From sequence input to functional annotation
          </p>
        </div>

        <div className="rounded-3xl border border-slate-200 bg-white p-5 sm:p-6 shadow-sm">
          <div className="flex flex-col sm:flex-row items-center justify-center gap-2 sm:gap-1 lg:gap-2">
            {data.pipeline_stages.map((stage, i) => (
              <div key={stage.name} className="flex flex-col sm:flex-row items-center">
                {i > 0 && (
                  <div className="text-slate-300 sm:mx-1 lg:mx-2 my-1 sm:my-0 select-none flex items-center justify-center">
                    <svg className="rotate-90 sm:rotate-0 w-5 h-5" fill="none" viewBox="0 0 20 20" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M5 10h10M11 6l4 4-4 4" />
                    </svg>
                  </div>
                )}
                <button
                  onClick={() => router.push(stage.href)}
                  className="group relative flex flex-col items-center justify-center w-32 sm:w-36 h-28 rounded-2xl border border-slate-200 bg-gradient-to-br from-white to-slate-50 hover:from-blue-50 hover:to-white hover:border-blue-300 hover:shadow-md transition-all cursor-pointer"
                >
                  <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-slate-100 text-slate-500 text-sm font-bold group-hover:bg-blue-100 group-hover:text-blue-600 transition-colors">
                    {STAGE_ICONS[stage.name] ?? stage.name.slice(0, 2).toUpperCase()}
                  </span>
                  <span className="text-[12px] font-semibold text-slate-700 mt-2 group-hover:text-slate-900">
                    {t(STAGE_I18N[stage.name] as any)}
                  </span>
                  <span className="text-[11px] text-slate-400 tabular-nums mt-0.5 font-medium">
                    {stage.count.toLocaleString()}
                  </span>
                </button>
              </div>
            ))}
            {/* LLM stage (future) */}
            <div className="flex flex-col sm:flex-row items-center">
              <div className="text-slate-300 sm:mx-1 lg:mx-2 my-1 sm:my-0 select-none flex items-center justify-center">
                <svg className="rotate-90 sm:rotate-0 w-5 h-5" fill="none" viewBox="0 0 20 20" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M5 10h10M11 6l4 4-4 4" />
                </svg>
              </div>
              <div className="flex flex-col items-center justify-center w-32 sm:w-36 h-28 rounded-2xl border-2 border-dashed border-slate-200 bg-slate-50/40">
                <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-slate-100/70 text-slate-300 text-sm font-bold">
                  LLM
                </span>
                <span className="text-[12px] font-medium text-slate-400 mt-2">{t("stageLlm")}</span>
                <span className="text-[10px] text-slate-300 mt-0.5 font-semibold uppercase tracking-wider">
                  soon
                </span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ── Stats bar ─────────────────────────────────────────────── */}
      <section className="mx-auto max-w-6xl">
        <div className="mb-4">
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">
            {t("stats")}
          </h2>
          <p className="text-sm text-slate-500 mt-1">Live counts across the data layer</p>
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
          {(
            [
              ["proteins", data.counts.proteins],
              ["sequences", data.counts.sequences],
              ["embeddings", data.counts.embeddings],
              ["predictions", data.counts.predictions],
            ] as [string, number][]
          ).map(([key, count]) => {
            const accent = STAT_ACCENTS[key] ?? "from-slate-200 to-transparent text-slate-700";
            return (
              <div
                key={key}
                className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white p-5 shadow-sm transition-shadow hover:shadow-md"
              >
                <div
                  aria-hidden
                  className={`absolute -top-10 -right-10 h-32 w-32 rounded-full bg-gradient-to-br ${accent} blur-2xl opacity-70 pointer-events-none`}
                />
                <div className="relative">
                  <div className="text-3xl sm:text-4xl font-bold text-slate-900 tabular-nums tracking-tight">
                    {count.toLocaleString()}
                  </div>
                  <div className="text-xs uppercase tracking-[0.14em] text-slate-500 mt-2 font-semibold">
                    {t(key as any)}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* ── CTAs ──────────────────────────────────────────────────── */}
      <section className="mx-auto flex max-w-3xl flex-col sm:flex-row items-center justify-center gap-3 pt-2">
        <Link
          href="/benchmark"
          className="w-full sm:w-auto inline-flex items-center justify-center gap-2 rounded-xl bg-blue-600 px-7 py-3 text-sm font-semibold text-white shadow-sm hover:bg-blue-700 transition-colors"
        >
          {t("exploreResults")}
          <span aria-hidden>→</span>
        </Link>
        <a
          href="#annotate-form"
          className="w-full sm:w-auto inline-flex items-center justify-center gap-2 rounded-xl border border-slate-300 bg-white px-7 py-3 text-sm font-semibold text-slate-700 shadow-sm hover:bg-slate-50 hover:border-slate-400 transition-colors"
        >
          {t("annotateProteins")}
        </a>
      </section>
    </div>
  );
}
