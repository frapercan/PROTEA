import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { getTranslations } from "next-intl/server";
import { EvidenceTable } from "@/components/book/EvidenceTable";
import { ReceiptFootnote } from "@/components/book/ReceiptFootnote";
import { pillarByNumber } from "@/lib/book";

/**
 * One page per pillar, in the same five parts, in this order: the claim in prose,
 * the board-faithful evidence, the receipt and the operation that regenerates it
 * (both carried by the pulled footnote, the way an edition carries its notes), and
 * the caveats. Rule five says we state our caveats plainly, so they close the page
 * as an open block, in frontier rose, never hidden behind a toggle.
 *
 * Server component: the pulled footnote is the only client island.
 */

type Params = { locale: string; n: string };

function parseN(raw: string): 1 | 2 | 3 | 4 | null {
  const n = Number(raw);
  return n === 1 || n === 2 || n === 3 || n === 4 ? n : null;
}

export async function generateMetadata({ params }: { params: Promise<Params> }): Promise<Metadata> {
  const { n } = await params;
  const pillar = parseN(n) ? pillarByNumber(Number(n)) : undefined;
  if (!pillar) return { title: "PROTEA" };
  return { title: pillar.title };
}

export default async function PillarPage({ params }: { params: Promise<Params> }) {
  const { locale, n } = await params;
  const num = parseN(n);
  if (!num) notFound();
  const pillar = pillarByNumber(num)!;
  const t = await getTranslations("book");

  const prev = pillar.n > 1 ? pillar.n - 1 : null;
  const next = pillar.n < 4 ? pillar.n + 1 : null;

  return (
    <article className="mx-auto max-w-3xl px-1 pb-16">
      {/* Return to the argument. */}
      <Link
        href={`/${locale}`}
        className="protea-eyebrow inline-flex items-center gap-1.5 text-[12px] uppercase tracking-wide text-[var(--muted)] hover:text-[var(--primary)]"
      >
        <span aria-hidden>←</span>
        {t("backToArgument")}
      </Link>

      <header className="mt-8">
        <p className="protea-eyebrow text-[12px] uppercase tracking-wide text-[var(--primary)]">
          {pillar.eyebrow}
        </p>
        <h1 className="mt-3 font-serif text-[2rem] font-normal leading-tight tracking-tight text-[var(--foreground)] sm:text-[2.4rem]">
          {pillar.title}
        </h1>
      </header>

      {/* 1. The claim, in prose, closing on the pulled receipt. */}
      <section className="mt-9">
        <h2 className="protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--muted)]">
          {t("claim")}
        </h2>
        <p className="mt-3 font-serif text-[18px] leading-[1.72] text-[var(--foreground)]">
          {pillar.claim}{" "}
          <ReceiptFootnote
            marker="R"
            receipt={pillar.receipt}
            receiptSecondary={pillar.receiptSecondary}
            operation={pillar.operation}
          />
        </p>
        <p className="mt-3 text-[13px] text-[var(--subtle)]">{t("pullHint")}</p>
      </section>

      {/* 2. The evidence, board-faithful. */}
      <section className="mt-12 border-t border-[var(--border)] pt-8">
        <h2 className="protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--muted)]">
          {t("evidence")}
        </h2>
        <div className="mt-4">
          <EvidenceTable data={pillar.evidence} />
        </div>
        {pillar.evidenceLabel && (
          <p className="mt-4 border-l-2 border-[var(--danger)] pl-3 text-[13px] leading-relaxed text-[var(--danger)]">
            {pillar.evidenceLabel}
          </p>
        )}
      </section>

      {/* 5. The caveats, ours, stated first before anyone states them for us. */}
      <section className="mt-12 border-t border-[var(--border)] pt-8">
        <h2 className="protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--danger)]">
          {t("caveats")}
        </h2>
        <p className="mt-3 text-[13px] italic text-[var(--muted)]">{t("caveatsIntro")}</p>
        <ul className="mt-4 space-y-3">
          {pillar.caveats.map((c, i) => (
            <li key={i} className="grid grid-cols-[1rem_1fr] gap-x-3">
              <span aria-hidden className="select-none font-mono text-[var(--danger)]">
                ·
              </span>
              <span className="text-[15px] leading-relaxed text-[var(--foreground)]">{c}</span>
            </li>
          ))}
        </ul>
      </section>

      {/* Chapter navigation. */}
      <nav
        aria-label={t("chapterNav")}
        className="mt-14 flex items-center justify-between border-t border-[var(--border)] pt-6 text-[14px]"
      >
        {prev ? (
          <Link
            href={`/${locale}/pillar/${prev}`}
            className="text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
          >
            ← {pillarByNumber(prev)!.title}
          </Link>
        ) : (
          <span />
        )}
        {next ? (
          <Link
            href={`/${locale}/pillar/${next}`}
            className="text-right text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
          >
            {pillarByNumber(next)!.title} →
          </Link>
        ) : (
          <span />
        )}
      </nav>
    </article>
  );
}
