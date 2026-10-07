import Link from "next/link";
import { getLocale, getTranslations } from "next-intl/server";
import { ARGUMENT_RECORD, AUTHOR, CAMPAIGN_STATUS, EXTERNAL_RESULT, LINKS, PLATFORM } from "@/lib/campaign";
import { baseUrl } from "@/lib/api";
import { QuietLink } from "@/components/AwaitingData";
import { AnnotateForm } from "@/components/AnnotateForm";
import { NineCellGrid } from "@/components/book/NineCellGrid";
import { ReceiptFootnote } from "@/components/book/ReceiptFootnote";
import { CHAPTER_ZERO, HEADLINE, PILLARS, THESIS_SENTENCE } from "@/lib/book";

/**
 * The front door is the argument, not a dashboard.
 *
 * `/` opens with one sentence stating what PROTEA is and what it achieved, sets
 * the sealed board as the hero (leading, deliberately, with the two cells it does
 * not win), and offers the four pillars as chapters. The instrument still lives,
 * one level in, reachable from the sidebar and from the quiet footer here; this
 * page simply stops being a control panel and becomes the thesis it serves.
 *
 * Server component: the only client island is the pull-a-footnote apparatus.
 */
/**
 * The live corpus size, or null when it cannot be read.
 *
 * An external audit on 2026-10-07 found four different protein counts on
 * four pages within the same minute: 745,421, 715,602, 713,318 and
 * 754673. They were not contradictory, they were unlabelled: the API
 * reports `total` (canonical entries plus isoforms) and `canonical`
 * separately, and this page was quoting a hand-written snapshot of the
 * total that had already gone stale. On a page about data, four numbers
 * for one quantity reads worse than any of them being wrong.
 *
 * Read at request time, with a short timeout, and the dated constant as
 * the fallback: this figure is the only genuinely live thing on the page
 * and it should not be the reason it fails to render.
 */
async function liveProteinTotal(): Promise<number | null> {
  try {
    const res = await fetch(`${baseUrl()}/proteins/stats`, {
      cache: "no-store",
      signal: AbortSignal.timeout(2_000),
    });
    if (!res.ok) return null;
    const stats = (await res.json()) as { total?: unknown };
    return typeof stats.total === "number" ? stats.total : null;
  } catch {
    return null;
  }
}

export default async function ArgumentPage() {
  const t = await getTranslations("book");
  const locale = await getLocale();
  const liveTotal = await liveProteinTotal();

  // The board gets its own window, and it is the only one this page states.
  //
  // The sealed board is LAFA's evaluation of a submitted container over Sep
  // 2025 to Mar 2026. Beside it this page used to print a second window read
  // live from the campaign, on the reasoning that a value read from the
  // database cannot drift. It drifted anyway, in the one way that reasoning
  // does not cover: the reading itself outlived what it read. Its source was
  // the retired ladder surface, which derived a window from jobs whose
  // evaluation results had been deleted, so the caption named a period no
  // surviving result was scored over.
  //
  // Both the campaign caption and the section that followed it are gone with
  // that surface. The experiment graph at /v1/graph replaces them, and until
  // it has a page this page states only what the sealed board settled.
  const frameCaption = [HEADLINE.metric, HEADLINE.frame, `validation ${HEADLINE.validation}`]
    .filter(Boolean)
    .join(" · ");

  // The page is wider than its prose on purpose. On a 1830px screen the
  // old max-w-3xl left roughly 370px of dead gutter on each side of a
  // 768px column. Widening the text would have been the wrong fix: about
  // 65 characters is the measure that reads well, and the h1 and the
  // argument keep it. What the extra room buys is somewhere to PUT
  // things, so the counted figures, the links and the campaign note move
  // into a rail beside the opening instead of sitting below the fold.
  // Every section below the header is pinned back to the reading measure.
  return (
    <div className="mx-auto max-w-6xl px-1 pb-16">
      {/* What this is, in the reader's own words, before any of ours.
          The thesis sentence used to be the h1, which meant the first
          thing a visitor read was "a taxonomy of orthogonal evidence
          combined by a calibrated fusion". It is the right sentence for
          the argument and the wrong one for an opening. It keeps every
          word, one section down.

          Three things the first draft of this block got wrong, all of
          them visible the moment it was on screen:

          - The measure was inconsistent. The h1 and the rule spanned the
            container while every paragraph stopped at max-w-prose, so
            the body looked narrower than everything around it. One
            measure now, the container's.
          - The external result was buried third, with less weight than
            the campaign note below it. It is the strongest and most
            checkable claim on the page, so it leads.
          - The engineering figures were inside prose, where they cannot
            be scanned. They are a row now.

          Typeset in this page's own language: stone family, serif body,
          and the understated underlined link it uses everywhere. No
          filled buttons, and no --muted background: --muted is #57534E,
          a TEXT colour. */}
      <header className="grid grid-cols-1 gap-x-14 gap-y-10 pt-2 sm:pt-6 xl:grid-cols-[minmax(0,1fr)_300px]">
        <div className="max-w-[46rem]">
        <p className="protea-eyebrow text-[12px] uppercase tracking-wide text-[var(--primary)]">
          {t("eyebrow")}
        </p>
        <h1 className="mt-6 font-serif text-[1.7rem] font-normal leading-[1.42] tracking-tight text-[var(--foreground)] sm:text-[2.05rem] sm:leading-[1.4]">
          {t("welcomeTitle")}
        </h1>
        <p className="mt-5 text-[14px] leading-relaxed text-[var(--muted)]">
          {t.rich("byline", {
            name: () => (
              <a
                href={LINKS.github}
                target="_blank"
                rel="noopener noreferrer"
                className="text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
              >
                {AUTHOR.name}
              </a>
            ),
          })}
          {AUTHOR.orcid ? (
            <>
              {" "}
              <a
                href={`https://orcid.org/${AUTHOR.orcid}`}
                target="_blank"
                rel="noopener noreferrer"
                className="text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
              >
                ORCID
              </a>
            </>
          ) : null}
        </p>
        <p className="mt-6 font-serif text-[17px] leading-relaxed text-[var(--foreground)]">
          {t("welcomeBody")}
        </p>

        {/* The result, given the weight it earns: external, dated,
            scored by someone else, and checkable in one click. */}
        <figure className="mt-9 border-t border-[var(--border)] pt-6">
          <figcaption className="protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--subtle)]">
            {t("resultLabel")}
          </figcaption>
          <p className="mt-3 font-serif text-[1.35rem] leading-snug text-[var(--foreground)] sm:text-[1.5rem]">
            {t.rich("welcomeResult", {
              rank: EXTERNAL_RESULT.rank,
              teams: EXTERNAL_RESULT.teams,
              competition: EXTERNAL_RESULT.competition,
              cafa: (chunks) => (
                <a
                  href={LINKS.cafaCompetition}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
                >
                  {chunks}
                </a>
              ),
            })}
          </p>
          <p className="mt-4 text-[14.5px] leading-relaxed text-[var(--muted)]">
            {t.rich("welcomeValidation", {
              lafa: (chunks) => (
                <a
                  href={LINKS.lafa}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
                >
                  {chunks}
                </a>
              ),
              evaluator: (chunks) => (
                <a
                  href={LINKS.evaluator}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
                >
                  {chunks}
                </a>
              ),
              upstream: (chunks) => (
                <a
                  href={LINKS.evaluatorUpstream}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
                >
                  {chunks}
                </a>
              ),
            })}
          </p>
        </figure>

        </div>

        {/* The rail: counted figures, where to go, and what is running.
            One column on a phone, beside the opening on a wide screen. */}
        <aside className="flex flex-col gap-7 xl:border-l xl:border-[var(--border)] xl:pl-10">
        <dl className="grid grid-cols-1 gap-x-8 gap-y-5 sm:grid-cols-3 xl:grid-cols-1">
          {[
            [t("statApi"), t("statApiValue", { ops: PLATFORM.apiOperations, routes: PLATFORM.apiRoutes })],
            [t("statTests"), t("statTestsValue", { tests: PLATFORM.backendTests })],
            [t("statData"), t("statDataValue", { gigabytes: CAMPAIGN_STATUS.gafGigabytes })],
          ].map(([label, value]) => (
            <div key={label}>
              <dt className="protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--subtle)]">
                {label}
              </dt>
              <dd className="mt-1.5 font-serif text-[18px] text-[var(--foreground)]">{value}</dd>
            </div>
          ))}
        </dl>

        <div className="flex flex-wrap gap-x-7 gap-y-3 border-t border-[var(--border)] pt-6 xl:flex-col xl:gap-y-3">
          <QuietLink href={`/${locale}/instrument`}>{t("openInstrument")}</QuietLink>
          <QuietLink href={`/${locale}/annotate`}>{t("annotate")}</QuietLink>
          <QuietLink href={LINKS.repo} external>{t("readTheCode")}</QuietLink>
          <QuietLink href="/thesis.pdf">{t("thesisPdf")}</QuietLink>
          <QuietLink href={`/${locale}?policy=1`}>{t("termsAndPrivacy")}</QuietLink>
        </div>

        <p className="border-l-2 border-[var(--border-strong)] pl-4 text-[13.5px] leading-relaxed text-[var(--subtle)]">
          {t("welcomeCampaign", {
            asOf: CAMPAIGN_STATUS.asOf,
            read: CAMPAIGN_STATUS.releasesRead,
            releases: CAMPAIGN_STATUS.goaReleases,
            gigabytes: CAMPAIGN_STATUS.gafGigabytes,
            proteins: liveTotal ?? CAMPAIGN_STATUS.proteinsAdmitted,
          })}
        </p>
        </aside>
      </header>

      {/* The tool, at the entrance, because that is what this is.
          The sidebar's call to action pointed at `/#annotate-form` and
          that anchor did not exist here any more, which is the fossil of
          this block having lived on the home page before. It reads
          better here than the sealed board did: a visitor who does not
          know what a protein annotation is learns more from the form
          than from a table of withdrawn figures.

          The form renders itself disabled, with its own explanation,
          while the annotation corpus is rebuilt. Showing it disabled
          shows the tool; hiding it hid the tool. */}
      <section id="annotate-form" className="mt-14 scroll-mt-24 border-t border-[var(--border)] pt-10">
        <AnnotateForm />
      </section>

      {/* The argument, published in full and dated. Not a word of the
          prose is edited: it states the LAFA board in the present tense
          and that was true when written, so the honest move is to say
          what it was measured against, not to rewrite a researcher's
          sentences or to make published work read as withheld. */}
      <section aria-labelledby="argument-heading" className="max-w-3xl mt-14 border-t border-[var(--border)] pt-10">
        <p className="max-w-prose text-[13px] leading-relaxed text-[var(--subtle)]">
          {t("argumentRecord", { board: ARGUMENT_RECORD.board, frame: ARGUMENT_RECORD.frame })}
        </p>
        <h2
          id="argument-heading"
          className="mt-5 font-serif text-[1.55rem] font-normal leading-[1.42] tracking-tight text-[var(--foreground)] sm:text-[1.85rem] sm:leading-[1.4]"
        >
          {THESIS_SENTENCE}
        </h2>
      </section>

      {/* Chapter zero: the whole argument, end to end, for a reader barely initiated. */}
      <section aria-labelledby="ch0-heading" className="max-w-3xl mt-12 border-t border-[var(--border)] pt-10">
        <h2 id="ch0-heading" className="sr-only">
          The argument, end to end
        </h2>
        <div className="space-y-7">
          {CHAPTER_ZERO.map((m, i) => (
            <div key={i}>
              <p className="font-serif text-[17px] leading-relaxed text-[var(--foreground)]">
                <span className="font-semibold">{m.lead}</span> {m.body}
              </p>
              {m.link ? (
                <Link
                  href={`/${locale}/${m.link.to}`}
                  className="group mt-2 inline-flex items-baseline gap-1.5 text-[14px] text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
                >
                  {m.link.label}
                  <span aria-hidden className="transition-transform group-hover:translate-x-0.5">
                    →
                  </span>
                </Link>
              ) : null}
            </div>
          ))}
        </div>
      </section>

      {/* The hero: the sealed board, typeset as a table. */}
      <section aria-labelledby="board-heading" className="max-w-3xl mt-14 border-t border-[var(--border)] pt-10">
        <h2 id="board-heading" className="sr-only">
          {t("boardHeading")}
        </h2>
        <NineCellGrid frameCaption={frameCaption} italicLine={t("nineCellItalic")} />

        <p className="mt-8 font-serif text-[17px] leading-relaxed text-[var(--foreground)]">
          {t.rich("headlineSentence", {
            metric: () => <span className="font-mono text-[15px] text-[var(--foreground)]">{HEADLINE.metric}</span>,
            // Withdrawn while the campaign recomputes: the sentence keeps its
            // shape and names the figure as absent, so a reader is told the
            // state of the work rather than shown a number this repository has
            // retracted.
            value: () => (
              <span className="font-semibold italic text-[var(--muted)]">
                {HEADLINE.value ?? t("headlineBeingRecomputed")}
              </span>
            ),
            note: (chunks) => (
              <>
                {chunks}
                <ReceiptFootnote
                  marker="R"
                  receipt={{
                    artifact: "storage/feature_necessity/gain_report.json",
                    script: "The sealed board is immutable; regenerated numbers are candidates until reviewed.",
                  }}
                  operation={{
                    kind: "job",
                    operation: "run_cafa_evaluation",
                    payload: { prediction_set_id: "<sealed>", metric: "f_micro_w", frame: "v227-v230" },
                    note: "The published figure is withdrawn while the campaign recomputes. Dispatching this operation is how a new one is produced, which is the point: a claim you cannot regenerate is not a claim.",
                  }}
                />
              </>
            ),
          })}
        </p>
      </section>

      {/* The four pillars, as chapters. */}
      <section aria-labelledby="chapters-heading" className="max-w-3xl mt-16 border-t border-[var(--border)] pt-10">
        <h2
          id="chapters-heading"
          className="protea-eyebrow text-[12px] uppercase tracking-wide text-[var(--muted)]"
        >
          {t("chaptersHeading")}
        </h2>
        <ol className="mt-6 divide-y divide-[var(--border)]">
          {PILLARS.map((p) => (
            <li key={p.n}>
              <Link
                href={`/${locale}/pillar/${p.n}`}
                className="group grid grid-cols-[2.5rem_1fr_auto] items-baseline gap-x-4 py-6 sm:gap-x-6"
              >
                <span className="font-serif text-2xl text-[var(--subtle)] tabular-nums group-hover:text-[var(--primary)]">
                  {p.n}
                </span>
                <span className="min-w-0">
                  <span className="block font-serif text-xl leading-snug text-[var(--foreground)] group-hover:text-[var(--primary)]">
                    {p.title}
                  </span>
                  <span className="mt-1.5 block text-[14px] leading-relaxed text-[var(--muted)]">
                    {p.teaser}
                  </span>
                </span>
                <span
                  aria-hidden
                  className="self-center text-[var(--subtle)] transition-transform group-hover:translate-x-0.5 group-hover:text-[var(--primary)]"
                >
                  →
                </span>
              </Link>
            </li>
          ))}
        </ol>
      </section>

      {/* Quiet footer: the instrument is a tab, not the entrance. */}
      <footer className="mt-14 border-t border-[var(--border)] pt-6">
        <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-[14px]">
          <Link
            href={`/${locale}/instrument/benchmark`}
            className="text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
          >
            {t("openInstrument")}
          </Link>
          <Link
            href={`/${locale}/annotate`}
            className="text-[var(--muted)] underline decoration-[var(--border)] decoration-1 underline-offset-2 hover:text-[var(--foreground)]"
          >
            {t("annotate")}
          </Link>
          <a
            href="/thesis.pdf"
            className="text-[var(--muted)] underline decoration-[var(--border)] decoration-1 underline-offset-2 hover:text-[var(--foreground)]"
          >
            {t("thesisPdf")}
          </a>
        </div>
      </footer>
    </div>
  );
}
