import Link from "next/link";
import { getLocale, getTranslations } from "next-intl/server";
import { getShowcase, type PerTaskAggregate, type ShowcaseData } from "@/lib/api";
import { Tooltip } from "@/components/Tooltip";
import { ShareBestLinkButton } from "./ShareBestLinkButton";
import { stageBadgeClass, stageLabelKey } from "@/lib/stageBadge";

/**
 * Async server component that hydrates the homepage best-result
 * spotlight, pipeline diagram, and live stats from a single
 * `getShowcase()` call rendered on the server. Lives inside a
 * <Suspense> boundary in the page route so the static hero streams
 * immediately while this awaits the API.
 *
 * Zero client JS apart from the single ShareBestLinkButton island
 * and the (stateless, CSS-only) Tooltip wrappers around the GO and
 * CAFA-split acronyms.
 */

const ASPECTS = ["MFO", "BPO", "CCO"] as const;

const ASPECT_LABELS: Record<string, string> = {
  MFO: "Molecular Function",
  BPO: "Biological Process",
  CCO: "Cellular Component",
};

// Text shades bumped one step darker (700 -> 800) so the abbreviation
// labels clear WCAG 2.2 AA (4.5:1) against their tinted backgrounds; the
// emerald-700-on-emerald-50 pairing was the lowest at 3.55:1.
const ASPECT_COLORS: Record<string, { bg: string; text: string; ring: string }> = {
  MFO: { bg: "bg-blue-50", text: "text-blue-800", ring: "ring-blue-100" },
  BPO: { bg: "bg-violet-50", text: "text-violet-800", ring: "ring-violet-100" },
  CCO: { bg: "bg-emerald-50", text: "text-emerald-800", ring: "ring-emerald-100" },
};

// Tooltips for the GO aspects, written in plain English. Hover surfaces
// the longer explanation so the short MFO / BPO / CCO acronyms stay
// usable in tight grid cells (P1.2 + P1.9).
const ASPECT_TOOLTIPS: Record<string, string> = {
  MFO: "Molecular Function: what the protein does at the molecular level (binding, catalysis, transport).",
  BPO: "Biological Process: the broader biological program the protein contributes to (cell cycle, signalling, metabolism).",
  CCO: "Cellular Component: where in the cell the protein is active (membrane, nucleus, ribosome, organelle).",
};

// Mirror the CAFA-split tooltips already used on the benchmark page so
// the NK / LK / PK acronyms in the spotlight subtitle are self-
// explaining (P1.9). Source of truth lives on the benchmark page; copy
// is kept short here.
const CATEGORY_TOOLTIPS: Record<string, string> = {
  NK: "No Knowledge: the test protein has no experimental GO annotations of any aspect at the train cutoff. Strictest CAFA split.",
  LK: "Limited Knowledge: the protein is annotated in other aspects but not the one under evaluation.",
  PK: "Partial Knowledge: the protein is already annotated for the same aspect, so scoring only counts newly added terms.",
};

// Definition of the IA-weighted headline metric. f_micro_w is the
// information-accretion-weighted micro-averaged F: the LAFA / CAFA headline
// number, the only one comparable to the external leaderboards (FIX-METRIC-IA).
const FMICRO_TOOLTIP =
  "f_micro_w: the information-accretion-weighted micro-averaged F score. The LAFA / CAFA headline metric, comparable to the external leaderboards. Rare, specific GO terms count more than common ones.";

// Shown when a cell still carries the legacy unweighted Fmax (no real IA
// score yet). Surfaces the gap honestly rather than mixing it into the
// IA-weighted headline.
const NOT_IA_TOOLTIP =
  "Not IA-weighted: this result predates real information-accretion scoring and reports the unweighted Fmax, which is not directly comparable to the LAFA / CAFA leaderboards.";

/** Mean ± 95% CI half-width across all (category, aspect) tasks, pooling the
 *  per-task means. This is the honest headline: the central tendency across
 *  every benchmark task, NOT the single best cell (winner's-curse). The CI is
 *  the mean of the per-task half-widths, a conservative readout of the typical
 *  uncertainty band rather than a re-derived pooled interval. */
function overallPerTask(
  perTask: PerTaskAggregate[],
): { mean: number; ci95: number; nTasks: number } | null {
  if (perTask.length === 0) return null;
  const mean = perTask.reduce((s, t) => s + t.mean, 0) / perTask.length;
  const ci95 = perTask.reduce((s, t) => s + t.ci95, 0) / perTask.length;
  return { mean, ci95, nTasks: perTask.length };
}

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

const STAGE_DESC_I18N: Record<string, string> = {
  sequences: "stageSequencesDesc",
  embeddings: "stageEmbeddingsDesc",
  predictions: "stageKnnDesc",
  reranker_models: "stageRerankerDesc",
  evaluations: "stageEvaluationDesc",
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

// Stage hrefs from the backend are root-relative ("/proteins",
// "/benchmark", ...). The active locale must be prepended or
// next-intl middleware silently redirects to the default locale
// (see fix(locale)/#530 / P0-C).
function withLocale(locale: string, href: string): string {
  if (!href.startsWith("/")) return href;
  if (href.startsWith(`/${locale}/`) || href === `/${locale}`) return href;
  return `/${locale}${href}`;
}

export async function HomeShowcase() {
  const t = await getTranslations("home");
  const locale = await getLocale();
  let data: ShowcaseData;
  // In E2E mode the Playwright fixture swaps the mock response between
  // tests, so the 60s fetch revalidate window would serve stale data
  // from the prior test. Disable the data cache when the build flag
  // is set; prod renders keep the cache.
  const cacheable = process.env.NEXT_PUBLIC_E2E !== "1";
  try {
    data = await getShowcase({ cacheable });
  } catch (e) {
    const message = e instanceof Error ? e.message : "Failed to load showcase";
    // Retry: target this exact route on the active locale so the
    // visitor stays on the homepage. A plain <a> works without client
    // JS (server component constraint); the browser performs a fresh
    // RSC fetch which re-runs `getShowcase()`.
    return (
      <div className="mx-auto max-w-3xl" data-testid="showcase-error">
        <div className="rounded-2xl border border-red-200 bg-red-50/70 p-8 text-center shadow-sm">
          <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-red-100 text-red-600 text-lg">
            !
          </div>
          <p className="text-red-800 text-sm">{message}</p>
          <a
            href={`/${locale}/`}
            className="mt-5 inline-flex items-center gap-2 rounded-xl bg-red-600 px-5 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-red-700 transition-colors"
          >
            Retry
          </a>
        </div>
      </div>
    );
  }

  const best = data.best;
  const paramBadge = best ? formatParamCount(best.embedding.param_count) : "";

  // Honest headline: per-task mean ± 95% CI across every model (the
  // f_micro_w aggregate the backend computes), NOT the best-cell maximum
  // (winner's-curse, FIX-METRIC-IA). The single best embedding below is
  // labelled "best cell", not as the headline number. `per_task` defaults
  // to [] so a backend that predates the aggregate degrades to the
  // best-cell-only view instead of crashing.
  const perTask = data.per_task ?? [];
  const headline = overallPerTask(perTask);

  // Per-aspect mean of the IA-weighted per-task means (mean over the 3
  // categories per aspect) for the mini tiles. Driven by the honest
  // per_task aggregate, not the best-cell per_cell list.
  const perAspect: Record<string, { sum: number; count: number }> = {};
  for (const t of perTask) {
    if (!perAspect[t.aspect]) perAspect[t.aspect] = { sum: 0, count: 0 };
    perAspect[t.aspect].sum += t.mean;
    perAspect[t.aspect].count += 1;
  }

  // True when the spotlight result is still on the legacy unweighted Fmax
  // (one or more cells predate real-IA scoring). Surfaced as a badge so the
  // non-IA number never silently rides the f_micro_w framing.
  const bestNotIaWeighted = best != null && best.primary_metric !== "f_micro_w";

  return (
    <>
      {/* Best result spotlight. id="best-<evaluation_result_id>" anchors
          the share-copy URL so opening the link reproduces this view. */}
      {best ? (
        <section
          id={`best-${best.evaluation_result_id}`}
          className="mx-auto max-w-5xl scroll-mt-24"
        >
          <div className="flex items-end justify-between gap-3 mb-4">
            <div className="min-w-0">
              <h2 className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-600">
                Benchmark headline
              </h2>
              <p className="text-sm text-slate-500 mt-1">
                Per-task mean{" "}
                <Tooltip text={FMICRO_TOOLTIP}>
                  <span className="underline decoration-dotted decoration-slate-400 underline-offset-4 cursor-help font-mono">
                    f_micro_w
                  </span>
                </Tooltip>
                {" "}across all models, with a 95% CI, on the{" "}
                <Tooltip text={CATEGORY_TOOLTIPS.NK}>
                  <span className="underline decoration-dotted decoration-slate-400 underline-offset-4 cursor-help">
                    NK
                  </span>
                </Tooltip>
                {" / "}
                <Tooltip text={CATEGORY_TOOLTIPS.LK}>
                  <span className="underline decoration-dotted decoration-slate-400 underline-offset-4 cursor-help">
                    LK
                  </span>
                </Tooltip>
                {" / "}
                <Tooltip text={CATEGORY_TOOLTIPS.PK}>
                  <span className="underline decoration-dotted decoration-slate-400 underline-offset-4 cursor-help">
                    PK
                  </span>
                </Tooltip>
                {" "}CAFA splits
              </p>
            </div>
            <div className="flex items-center gap-3 shrink-0">
              <ShareBestLinkButton evaluationResultId={best.evaluation_result_id} />
              <Link
                href={`/${locale}/benchmark`}
                className="text-[13px] font-medium text-blue-600 hover:text-blue-800 transition-colors inline-flex items-center gap-1"
              >
                {t("viewBenchmark")}
                <span aria-hidden>→</span>
              </Link>
            </div>
          </div>

          <div className="relative overflow-hidden rounded-3xl border border-slate-200 bg-white p-6 sm:p-8 shadow-sm">
            <div
              aria-hidden
              className="absolute right-0 top-0 h-40 w-40 rounded-full bg-gradient-to-br from-blue-100 to-violet-100 blur-3xl opacity-50 pointer-events-none"
            />

            {/* Headline: honest per-task mean ± 95% CI across every model.
                NOT the best-cell maximum (winner's-curse, FIX-METRIC-IA). */}
            <div className="relative flex flex-col sm:flex-row sm:items-end sm:justify-between gap-4">
              <div className="flex items-baseline gap-2">
                <div className="text-5xl sm:text-6xl font-bold tabular-nums protea-gradient-text leading-none">
                  {headline ? headline.mean.toFixed(3) : "—"}
                </div>
                {headline && (
                  <div className="text-xl sm:text-2xl font-semibold tabular-nums text-slate-500 leading-none">
                    ± {headline.ci95.toFixed(3)}
                  </div>
                )}
              </div>
              <div className="text-[11px] uppercase tracking-[0.14em] text-slate-500 font-medium">
                mean{" "}
                <Tooltip text={FMICRO_TOOLTIP}>
                  <span className="underline decoration-dotted decoration-slate-400 underline-offset-4 cursor-help font-mono normal-case">
                    f_micro_w
                  </span>
                </Tooltip>
                {" "}± 95% CI · {headline ? headline.nTasks : 0} tasks
              </div>
            </div>

            {/* Best-cell spotlight — the single top model on one cell. Clearly
                labelled "best cell" so it is never read as the headline. */}
            <div className="relative mt-6 rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
              <div className="flex items-center gap-2 mb-2 flex-wrap">
                <span className="rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-800 ring-1 ring-inset ring-amber-200">
                  Best cell
                </span>
                {bestNotIaWeighted && (
                  <Tooltip text={NOT_IA_TOOLTIP}>
                    <span className="rounded-full bg-rose-100 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-rose-800 ring-1 ring-inset ring-rose-200 cursor-help">
                      Not IA-weighted
                    </span>
                  </Tooltip>
                )}
                <span className="text-[11px] text-slate-500">
                  single top model on its best (split × aspect) cell · not the headline
                </span>
              </div>
              <div className="flex flex-col lg:flex-row lg:items-start lg:justify-between gap-4">
                <div className="min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-xl sm:text-2xl font-bold text-slate-900 tracking-tight truncate">
                      {best.embedding.display_name}
                    </span>
                    {paramBadge && (
                      <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700 tabular-nums ring-1 ring-inset ring-slate-200">
                        {paramBadge}
                      </span>
                    )}
                    <span
                      className={`rounded-full px-2.5 py-1 text-xs font-semibold ${stageBadgeClass(
                        best.stage,
                      )}`}
                    >
                      {stageLabelKey(best.stage)
                        ? t(stageLabelKey(best.stage) as never)
                        : best.stage}
                    </span>
                  </div>
                  <div className="text-[12px] text-slate-600 mt-2 font-mono break-all">
                    {best.embedding.model_name}
                  </div>
                </div>
                <div className="flex items-baseline gap-2 lg:flex-col lg:items-end lg:gap-1 lg:text-right shrink-0">
                  <div className="text-3xl sm:text-4xl font-bold tabular-nums text-slate-800 leading-none">
                    {typeof best.avg_primary === "number" ? best.avg_primary.toFixed(3) : "—"}
                  </div>
                  <div className="text-[10px] uppercase tracking-[0.14em] text-slate-500 font-medium">
                    best-cell mean{" "}
                    {bestNotIaWeighted ? "Fmax" : "f_micro_w"}
                  </div>
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
                  >
                    <div className={`text-2xl sm:text-3xl font-bold tabular-nums ${color.text}`}>
                      {value != null ? value.toFixed(3) : "—"}
                    </div>
                    <div className={`text-xs uppercase tracking-[0.14em] mt-1.5 font-semibold ${color.text} opacity-80`}>
                      <Tooltip text={ASPECT_TOOLTIPS[aspect]}>
                        <span className="underline decoration-dotted decoration-slate-400 underline-offset-4 cursor-help">
                          {aspect}
                        </span>
                      </Tooltip>
                    </div>
                    <div className="text-xs text-slate-600 mt-0.5 truncate" title={ASPECT_LABELS[aspect]}>
                      {ASPECT_LABELS[aspect]}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </section>
      ) : (
        <section
          data-testid="showcase-empty"
          className="mx-auto max-w-3xl rounded-3xl border-2 border-dashed border-slate-200 bg-white/60 p-10 text-center"
        >
          <p className="text-slate-500 text-base">{t("noDataYet")}</p>
          <Link
            href={`/${locale}/proteins`}
            className="mt-5 inline-flex items-center gap-2 rounded-xl bg-blue-600 px-5 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-blue-700 transition-colors"
          >
            {t("getStarted")}
            <span aria-hidden>→</span>
          </Link>
        </section>
      )}

      {/* Pipeline diagram. Tiles use <Link> so navigation works without
          client JS (the original useRouter().push has no extra value
          here, the targets are plain routes). Each href is locale-
          prefixed per fix(locale)/#530. */}
      <section className="mx-auto max-w-6xl">
        <div className="mb-4">
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-600">
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
                <Link
                  href={withLocale(locale, stage.href)}
                  aria-label={`${t(STAGE_I18N[stage.name] as never)} ${t(STAGE_DESC_I18N[stage.name] as never)}`}
                  className="group relative flex flex-col items-center justify-center w-40 sm:w-44 min-h-[10rem] px-3 py-3 rounded-2xl border border-slate-200 bg-gradient-to-br from-white to-slate-50 hover:from-blue-50 hover:to-white hover:border-blue-300 hover:shadow-md transition-all cursor-pointer text-center"
                >
                  <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-slate-100 text-slate-500 text-sm font-bold group-hover:bg-blue-100 group-hover:text-blue-600 transition-colors">
                    {STAGE_ICONS[stage.name] ?? stage.name.slice(0, 2).toUpperCase()}
                  </span>
                  <span className="text-[12px] font-semibold text-slate-700 mt-2 group-hover:text-slate-900">
                    {t(STAGE_I18N[stage.name] as never)}
                  </span>
                  <span className="text-[11px] text-slate-600 tabular-nums mt-0.5 font-medium">
                    {stage.count.toLocaleString()}
                  </span>
                  <span className="text-[10px] text-slate-500 leading-snug mt-1.5 line-clamp-2">
                    {t(STAGE_DESC_I18N[stage.name] as never)}
                  </span>
                  <span className="text-[10px] font-medium text-blue-600 mt-1.5 inline-flex items-center gap-0.5 group-hover:text-blue-800 transition-colors">
                    {t("stageView" as never)}
                    <span aria-hidden>→</span>
                  </span>
                </Link>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Live stats bar */}
      <section className="mx-auto max-w-6xl">
        <div className="mb-4">
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-600">
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
                    {t(key as never)}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </section>
    </>
  );
}

export function HomeShowcaseSkeleton() {
  return (
    <div className="space-y-12 lg:space-y-14" aria-busy="true" aria-live="polite">
      <div className="mx-auto max-w-5xl">
        <div className="h-4 w-40 bg-slate-200/70 rounded mb-4 animate-pulse" />
        <div className="h-56 rounded-3xl border border-slate-200 bg-white/60 animate-pulse" />
      </div>
      <div className="mx-auto max-w-6xl">
        <div className="h-4 w-40 bg-slate-200/70 rounded mb-4 animate-pulse" />
        <div className="h-44 rounded-3xl border border-slate-200 bg-white/60 animate-pulse" />
      </div>
      <div className="mx-auto max-w-6xl">
        <div className="h-4 w-40 bg-slate-200/70 rounded mb-4 animate-pulse" />
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="h-28 rounded-2xl border border-slate-200 bg-white/60 animate-pulse" />
          ))}
        </div>
      </div>
    </div>
  );
}
