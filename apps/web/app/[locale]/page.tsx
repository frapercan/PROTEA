import Link from "next/link";
import { getLocale, getTranslations } from "next-intl/server";
import { ARGUMENT_RECORD, CAMPAIGN_STATUS, EXTERNAL_RESULT } from "@/lib/campaign";
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
export default async function ArgumentPage() {
  const t = await getTranslations("book");
  const locale = await getLocale();

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

  return (
    <div className="mx-auto max-w-3xl px-1 pb-16">
      {/* What this is, in the reader's own words, before any of ours.
          The thesis sentence used to be the h1, which meant the first
          thing a visitor read was "a taxonomy of orthogonal evidence
          combined by a calibrated fusion". It is the right sentence for
          the argument and the wrong one for an opening. It keeps every
          word, one section down. */}
      <header className="pt-2 sm:pt-6">
        <p className="protea-eyebrow text-[12px] uppercase tracking-wide text-[var(--primary)]">
          {t("eyebrow")}
        </p>
        <h1 className="mt-6 text-[1.65rem] font-semibold leading-[1.25] tracking-tight text-[var(--foreground)] sm:text-[2.1rem]">
          {t("welcomeTitle")}
        </h1>
        <p className="mt-5 max-w-prose text-[16px] leading-relaxed text-[var(--foreground)]">
          {t("welcomeBody")}
        </p>
        <p className="mt-4 max-w-prose text-[15px] leading-relaxed text-[var(--muted-foreground)]">
          {t("welcomeResult", {
            rank: EXTERNAL_RESULT.rank,
            teams: EXTERNAL_RESULT.teams,
            competition: EXTERNAL_RESULT.competition,
          })}
        </p>
        <p className="mt-4 max-w-prose rounded-lg border border-[var(--border)] bg-[var(--muted)] px-4 py-3 text-[14px] leading-relaxed text-[var(--muted-foreground)]">
          {t("welcomeCampaign", {
            asOf: CAMPAIGN_STATUS.asOf,
            releases: CAMPAIGN_STATUS.goaReleases,
            gigabytes: CAMPAIGN_STATUS.gafGigabytes,
            proteins: CAMPAIGN_STATUS.proteinsAdmitted,
          })}
        </p>
        <div className="mt-6 flex flex-wrap items-center gap-3">
          <Link
            href={`/${locale}/instrument/benchmark`}
            className="rounded-lg bg-[var(--primary)] px-4 py-2.5 text-[14px] font-semibold text-white hover:opacity-90"
          >
            {t("openInstrument")}
          </Link>
          <Link
            href="/thesis.pdf"
            className="rounded-lg border border-[var(--border-strong)] px-4 py-2.5 text-[14px] font-semibold text-[var(--foreground)] hover:bg-[var(--muted)]"
          >
            {t("thesisPdf")}
          </Link>
        </div>
      </header>

      {/* The argument, scoped by the board and window it was written
          against. Not a word of the prose is edited: the figures inside
          it are stated in the present tense and were true then, so the
          honest fix is to date the whole record rather than rewrite a
          researcher's sentences. */}
      <section aria-labelledby="argument-heading" className="mt-14 border-t border-[var(--border)] pt-10">
        <p className="max-w-prose text-[13px] leading-relaxed text-[var(--muted-foreground)]">
          {t("argumentRecord", { board: ARGUMENT_RECORD.board, frame: ARGUMENT_RECORD.frame })}
        </p>
        <h2
          id="argument-heading"
          className="mt-6 font-serif text-[1.7rem] font-normal leading-[1.42] tracking-tight text-[var(--foreground)] sm:text-[2.05rem] sm:leading-[1.4]"
        >
          {THESIS_SENTENCE}
        </h2>
      </section>

      {/* Chapter zero: the whole argument, end to end, for a reader barely initiated. */}
      <section aria-labelledby="ch0-heading" className="mt-12 border-t border-[var(--border)] pt-10">
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
      <section aria-labelledby="board-heading" className="mt-14 border-t border-[var(--border)] pt-10">
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
                {HEADLINE.value ?? "being recomputed"}
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
      <section aria-labelledby="chapters-heading" className="mt-16 border-t border-[var(--border)] pt-10">
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
