import { Suspense } from "react";
import Link from "next/link";
import { getLocale, getTranslations } from "next-intl/server";
import { AnnotateForm } from "../../components/AnnotateForm";
import { HomeShowcase, HomeShowcaseSkeleton } from "../../components/home/HomeShowcase";

/**
 * Homepage — App Router server component.
 *
 * P0.2 perf migration: the hero + AnnotateForm shell + CTAs render on
 * the server (zero client JS for the LCP region). The heavier
 * best-result + pipeline + stats triad lives behind a <Suspense> wall
 * so it streams in second without blocking the first paint. See
 * components/home/HomeShowcase.tsx for the awaited data flow.
 *
 * Only the AnnotateForm and the ShareBestLinkButton stay as client
 * islands because they own interactive state (form submission and
 * clipboard write respectively); everything else is plain HTML by the
 * time it reaches the browser.
 */
export default async function HomePage() {
  const t = await getTranslations("home");
  const locale = await getLocale();

  return (
    <div className="space-y-12 lg:space-y-14">
      {/* Hero — pure server-rendered HTML, contributes the LCP. */}
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

      {/* Annotate form — client island. `id` powers the "Annotate
          proteins" CTA below; scroll-mt clears the sticky h-16 header
          so the form lands fully visible. */}
      <section id="annotate-form" className="mx-auto max-w-4xl scroll-mt-24">
        <AnnotateForm />
      </section>

      {/* Best-result spotlight + pipeline + stats. Wrapped in Suspense
          so the showcase fetch never blocks the hero LCP. */}
      <Suspense fallback={<HomeShowcaseSkeleton />}>
        <HomeShowcase />
      </Suspense>

      {/* CTAs. /{locale}/benchmark uses <Link> for SPA navigation; the
          in-page #annotate-form jump stays a plain anchor since the
          fragment is local. Locale prefix per fix(locale)/#530. */}
      <section className="mx-auto flex max-w-3xl flex-col sm:flex-row items-center justify-center gap-3 pt-2">
        <Link
          href={`/${locale}/benchmark`}
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
