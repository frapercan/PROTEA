"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useTranslations } from "next-intl";
import Link from "next/link";
import { getShowcase, type ShowcaseData } from "../../lib/api";
import { AnnotateForm } from "../../components/AnnotateForm";

const ASPECTS = ["MFO", "BPO", "CCO"] as const;
const ASPECT_COLORS: Record<string, string> = {
  MFO: "blue",
  BPO: "green",
  CCO: "purple",
};
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

const METHOD_KEYS: Record<string, string> = {
  knn_baseline: "knnBaseline",
  knn_scored: "knnScored",
  knn_reranker: "knnReranker",
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

export default function HomePage() {
  const t = useTranslations("home");
  const router = useRouter();
  const [data, setData] = useState<ShowcaseData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeCategory, setActiveCategory] = useState<string>("NK");

  useEffect(() => {
    getShowcase().then(setData).catch((e) => setError(e.message));
  }, []);

  if (error) {
    return (
      <div className="max-w-5xl mx-auto px-4 sm:px-6 py-12">
        <div className="rounded-lg border border-red-200 bg-red-50 p-6 text-center">
          <p className="text-red-800 text-sm">{error}</p>
          <button
            onClick={() => { setError(null); getShowcase().then(setData).catch((e) => setError(e.message)); }}
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
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          {[0, 1, 2].map((i) => (
            <div key={i} className="h-32 bg-gray-100 rounded-lg animate-pulse" />
          ))}
        </div>
        <div className="h-48 bg-gray-100 rounded-lg animate-pulse" />
      </div>
    );
  }

  const hasFmax = data.best_fmax && Object.keys(data.best_fmax).length > 0;
  const hasComparison = data.method_comparison && Object.keys(data.method_comparison).length > 0;

  // Available categories (only those with data)
  const availableCategories = CATEGORIES.filter(
    (cat) => data.best_fmax?.[cat] || data.method_comparison?.[cat]
  );

  // Current category data
  const catFmax = data.best_fmax?.[activeCategory] ?? {};
  const catMethods = data.method_comparison?.[activeCategory] ?? [];
  const baseline = catMethods.find((m) => m.method === "knn_baseline");

  return (
    <div className="max-w-5xl mx-auto px-4 sm:px-6 py-8 space-y-10">
      {/* ── Hero ──────────────────────────────────────────────────── */}
      <section className="text-center space-y-3">
        <h1 className="text-3xl sm:text-4xl font-bold text-gray-900 tracking-tight">
          PROTEA
        </h1>
        <p className="text-lg text-gray-500 max-w-2xl mx-auto">
          {t("subtitle")}
        </p>
      </section>

      {/* ── Annotate form ─────────────────────────────────────────── */}
      <AnnotateForm />

      {/* ── Category tabs ─────────────────────────────────────────── */}
      {hasFmax ? (
        <>
          <section>
            <div className="flex items-center gap-4 mb-4">
              <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider">
                {t("bestResults")}
              </h2>
              <div className="flex gap-1 rounded-lg bg-gray-100 p-0.5">
                {availableCategories.map((cat) => (
                  <button
                    key={cat}
                    onClick={() => setActiveCategory(cat)}
                    className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                      activeCategory === cat
                        ? "bg-white text-gray-900 shadow-sm"
                        : "text-gray-500 hover:text-gray-700"
                    }`}
                    title={CATEGORY_LABELS[cat]}
                  >
                    {cat}
                  </button>
                ))}
              </div>
              <span className="text-xs text-gray-400" title={CATEGORY_LABELS[activeCategory]}>
                {CATEGORY_LABELS[activeCategory]}
              </span>
            </div>

            {/* ── Fmax cards ────────────────────────────────────────── */}
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              {ASPECTS.map((aspect) => {
                const d = catFmax[aspect];
                if (!d) return null;
                const color = ASPECT_COLORS[aspect];
                return (
                  <div
                    key={aspect}
                    className={`rounded-xl border-2 p-5 text-center`}
                    style={{
                      borderColor: `var(--color-${color}-200, #bfdbfe)`,
                      backgroundColor: `var(--color-${color}-50, #eff6ff)`,
                    }}
                  >
                    <div className="text-4xl font-bold text-gray-900 tabular-nums">
                      {d.fmax.toFixed(2)}
                    </div>
                    <div className="text-sm font-semibold text-gray-600 mt-1">
                      {t("fmax")} {aspect}
                    </div>
                    <div className="text-xs text-gray-400 mt-1">
                      {ASPECT_LABELS[aspect]}
                    </div>
                    <div className="text-xs text-gray-400 mt-1">
                      {d.method_label}
                    </div>
                  </div>
                );
              })}
            </div>
          </section>

          {/* ── Method comparison table ───────────────────────────── */}
          {catMethods.length > 0 && (
            <section>
              <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">
                {t("methodComparison")}
                <span className="ml-2 text-xs font-normal normal-case text-gray-400">
                  ({activeCategory})
                </span>
              </h2>
              <div className="overflow-x-auto rounded-lg border">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50 text-left">
                      <th className="px-4 py-3 font-medium text-gray-600">{t("method")}</th>
                      {ASPECTS.map((a) => (
                        <th key={a} className="px-4 py-3 font-medium text-gray-600 text-center">
                          {a}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {catMethods.map((row, i) => {
                      const isBest = ASPECTS.some(
                        (a) => catFmax[a]?.method === row.method
                      );
                      return (
                        <tr
                          key={row.method}
                          className={`border-t ${isBest ? "bg-blue-50" : i % 2 === 0 ? "bg-white" : "bg-gray-50/50"}`}
                        >
                          <td className="px-4 py-3 font-medium text-gray-900">
                            {t(METHOD_KEYS[row.method] ?? row.method)}
                            {isBest && (
                              <span className="ml-2 text-xs text-blue-600 font-normal">best</span>
                            )}
                          </td>
                          {ASPECTS.map((aspect) => {
                            const val = (row as any)[aspect]?.fmax;
                            const baseVal = baseline ? (baseline as any)[aspect]?.fmax : null;
                            const delta = val != null && baseVal != null && row.method !== "knn_baseline"
                              ? val - baseVal
                              : null;
                            return (
                              <td key={aspect} className="px-4 py-3 text-center tabular-nums">
                                {val != null ? (
                                  <span>
                                    <span className="font-semibold">{val.toFixed(3)}</span>
                                    {delta != null && (
                                      <span className={`ml-1.5 text-xs ${delta > 0 ? "text-green-600" : delta < 0 ? "text-red-600" : "text-gray-400"}`}>
                                        {delta > 0 ? "+" : ""}{delta.toFixed(3)}
                                      </span>
                                    )}
                                  </span>
                                ) : (
                                  <span className="text-gray-300">&mdash;</span>
                                )}
                              </td>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          )}
        </>
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
          {([
            ["proteins", data.counts.proteins],
            ["sequences", data.counts.sequences],
            ["embeddings", data.counts.embeddings],
            ["predictions", data.counts.predictions],
          ] as [string, number][]).map(([key, count]) => (
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
          href="/evaluation"
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
