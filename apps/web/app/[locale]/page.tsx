import { getLocale, getTranslations } from "next-intl/server";
import { AUTHOR, LINKS } from "@/lib/campaign";
import { baseUrl, publicBaseUrl } from "@/lib/api";
import { QuietLink } from "@/components/AwaitingData";
import { AnnotateForm } from "@/components/AnnotateForm";
import { CHANGES, PLANS, inLocale } from "@/lib/news";

/**
 * The front page (the author, 2026-11-24): the instrument first, then what it
 * holds, what has changed and what comes next. A first-time visitor learns what
 * PROTEA does by using it; a returning one reads the log and the plans.
 *
 * Every figure is read live from the API that serves this page and cached for
 * five minutes. A figure that cannot be read shows a dash, never a constant that
 * goes stale.
 */

type ProteinStats = { total?: number; with_go_annotations?: number };
type AnnotationSetRow = { source?: string; source_published_at?: string | null };
type ConfigRow = { embedding_count?: number | null };

async function readLive<T>(path: string): Promise<T | null> {
  try {
    const res = await fetch(`${baseUrl()}${path}`, {
      next: { revalidate: 300 },
      signal: AbortSignal.timeout(2_500),
    });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

const LINK =
  "text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]";
const SECTION_TITLE = "protea-eyebrow text-[12px] uppercase tracking-wide text-[var(--primary)]";

export default async function FrontPage() {
  const [t, tb, tn, locale] = await Promise.all([
    getTranslations("landing"),
    getTranslations("book"),
    getTranslations("nav"),
    getLocale(),
  ]);
  const [stats, sets, configs] = await Promise.all([
    readLive<ProteinStats>("/proteins/stats"),
    readLive<AnnotationSetRow[]>("/annotations/sets/"),
    readLive<ConfigRow[]>("/embeddings/configs/"),
  ]);

  const count = new Intl.NumberFormat(locale);
  const shown = (v: number | null | undefined) => (typeof v === "number" ? count.format(v) : "-");
  const day = new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeZone: "UTC" });

  // The GO annotation releases the store holds, by the date each was published.
  const releases = Array.isArray(sets)
    ? sets
        .filter((s) => s.source === "goa" && s.source_published_at)
        .map((s) => String(s.source_published_at))
        .sort()
    : [];
  // A model counts once it has vectors; a learned method registered as a config has none.
  const models = Array.isArray(configs) ? configs.filter((c) => (c.embedding_count ?? 0) > 0) : null;
  const vectors = models ? models.reduce((sum, c) => sum + (c.embedding_count ?? 0), 0) : null;

  const figures: Array<[string, string]> = [
    [t("statProteins"), shown(stats?.total)],
    [t("statWithGo"), shown(stats?.with_go_annotations)],
    [
      t("statCuts"),
      releases.length
        ? t("statCutsValue", {
            count: releases.length,
            first: releases[0].slice(0, 4),
            last: releases[releases.length - 1].slice(0, 4),
          })
        : "-",
    ],
    [t("statModels"), models ? count.format(models.length) : "-"],
    [t("statVectors"), shown(vectors)],
  ];

  return (
    <div className="mx-auto max-w-6xl px-1 pb-16">
      <header className="max-w-[46rem] pt-2 sm:pt-6">
        <p className={SECTION_TITLE}>{tb("eyebrow")}</p>
        <h1 className="mt-6 font-serif text-[1.7rem] font-normal leading-[1.42] tracking-tight text-[var(--foreground)] sm:text-[2.05rem] sm:leading-[1.4]">
          {tb("welcomeTitle")}
        </h1>
        <p className="mt-6 font-serif text-[17px] leading-relaxed text-[var(--foreground)]">{tb("welcomeBody")}</p>
        <p className="mt-5 text-[14px] leading-relaxed text-[var(--muted)]">
          {tb.rich("byline", {
            name: () => (
              <a href={LINKS.github} target="_blank" rel="noopener noreferrer" className={LINK}>
                {AUTHOR.name}
              </a>
            ),
          })}
          {AUTHOR.orcid ? (
            <>
              {" "}
              <a href={`https://orcid.org/${AUTHOR.orcid}`} target="_blank" rel="noopener noreferrer" className={LINK}>
                ORCID
              </a>
            </>
          ) : null}
        </p>
      </header>

      {/* The tool, at the entrance: the sidebar's call to action and older links point here. */}
      <section id="annotate-form" className="mt-12 scroll-mt-24 border-t border-[var(--border)] pt-10">
        <AnnotateForm />
      </section>

      <section aria-labelledby="front-figures" className="mt-14 border-t border-[var(--border)] pt-10">
        <h2 id="front-figures" className={SECTION_TITLE}>
          {t("statsTitle")}
        </h2>
        <dl className="mt-6 grid grid-cols-1 gap-x-10 gap-y-7 sm:grid-cols-2 lg:grid-cols-3">
          {figures.map(([label, value]) => (
            <div key={label} className="border-l-2 border-[var(--border-strong)] pl-4">
              <dt className="protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--subtle)]">{label}</dt>
              <dd className="mt-1.5 font-serif text-[1.6rem] leading-tight tabular-nums text-[var(--foreground)]">
                {value}
              </dd>
            </div>
          ))}
        </dl>
        <p className="mt-5 text-[13px] text-[var(--subtle)]">{t("statsNote")}</p>
      </section>

      <div className="mt-14 grid grid-cols-1 gap-12 border-t border-[var(--border)] pt-10 lg:grid-cols-2">
        <section aria-labelledby="front-news">
          <h2 id="front-news" className={SECTION_TITLE}>
            {t("newsTitle")}
          </h2>
          <ol className="mt-6 space-y-5">
            {CHANGES.map((entry) => (
              <li key={`${entry.date} ${entry.text.en}`} className="grid grid-cols-[7.5rem_minmax(0,1fr)] gap-4">
                <time dateTime={entry.date} className="pt-0.5 text-[13px] tabular-nums text-[var(--subtle)]">
                  {day.format(new Date(`${entry.date}T00:00:00Z`))}
                </time>
                <p className="font-serif text-[15.5px] leading-relaxed text-[var(--foreground)]">
                  {inLocale(entry.text, locale)}
                </p>
              </li>
            ))}
          </ol>
        </section>
        <section aria-labelledby="front-plans">
          <h2 id="front-plans" className={SECTION_TITLE}>
            {t("plansTitle")}
          </h2>
          <ul className="mt-6 space-y-5">
            {PLANS.map((plan) => (
              <li
                key={plan.text.en}
                className="border-l-2 border-[var(--border-strong)] pl-4 font-serif text-[15.5px] leading-relaxed text-[var(--foreground)]"
              >
                {inLocale(plan.text, locale)}
              </li>
            ))}
          </ul>
        </section>
      </div>

      <nav aria-label={t("linksLabel")} className="mt-14 flex flex-wrap gap-x-7 gap-y-3 border-t border-[var(--border)] pt-8">
        <QuietLink href={`/${locale}/instrument`}>{tb("openInstrument")}</QuietLink>
        <QuietLink href={`/${locale}/instrument/evaluation`}>{t("evaluation")}</QuietLink>
        <QuietLink href="/sphinx/" external>
          {tn("sphinx")}
        </QuietLink>
        <QuietLink href={`${publicBaseUrl()}/docs`} external>
          {tn("swagger")}
        </QuietLink>
        <QuietLink href={LINKS.repo} external>
          {tb("readTheCode")}
        </QuietLink>
        <QuietLink href="/thesis.pdf">{tb("thesisPdf")}</QuietLink>
        <QuietLink href={`/${locale}?policy=1`}>{tb("termsAndPrivacy")}</QuietLink>
      </nav>
    </div>
  );
}
