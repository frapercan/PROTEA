"use client";

import { useId, useState } from "react";
import Link from "next/link";
import { useLocale, useTranslations } from "next-intl";
import type { Operation, Receipt } from "@/lib/book";

/**
 * The signature of the book: a footnote you pull.
 *
 * A claim carries a small superscript marker set in evidence blue. Activating it
 * expands the apparatus in place, the way a scholarly edition carries its notes:
 * the receipt filename, the script that produced it, and the operation that
 * regenerates the number. It is a footnote you pull, not a call-to-action button;
 * filled buttons belong to the instrument, one level in.
 *
 * The control is a real <button> so it is reachable and operable from the
 * keyboard, and it announces its expanded state. The expansion is a plain
 * conditional render, so `prefers-reduced-motion` has nothing to suppress.
 */

function ApparatusRow({ term, children }: { term: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[7rem_1fr] gap-x-3 gap-y-1 py-1.5">
      <dt className="protea-eyebrow text-[11px] uppercase text-[var(--muted)]">{term}</dt>
      <dd className="min-w-0 text-[13px] leading-relaxed text-[var(--foreground)]">{children}</dd>
    </div>
  );
}

export function ReceiptFootnote({
  marker = "R",
  receipt,
  receiptSecondary,
  operation,
}: {
  marker?: string;
  receipt: Receipt;
  receiptSecondary?: Receipt;
  operation: Operation;
}) {
  const t = useTranslations("book");
  const locale = useLocale();
  const [open, setOpen] = useState(false);
  const panelId = useId();

  const renderReceipt = (r: Receipt, key: string) => (
    <ApparatusRow term={key === "primary" ? t("receipt") : t("receiptAlso")} key={key}>
      <code className="block break-all font-mono text-[12px] text-[var(--foreground)]">{r.artifact}</code>
      {r.script && (
        <code className="mt-0.5 block break-all font-mono text-[12px] text-[var(--muted)]">{r.script}</code>
      )}
      {r.sha && <span className="font-mono text-[11px] text-[var(--muted)]">sha {r.sha}</span>}
      {r.pending && <span className="mt-0.5 block text-[12px] text-[var(--danger)]">{t("receiptPending")}</span>}
    </ApparatusRow>
  );

  return (
    <span className="whitespace-nowrap">
      <button
        type="button"
        aria-expanded={open}
        aria-controls={panelId}
        aria-label={t("pullReceipt")}
        onClick={() => setOpen((v) => !v)}
        className="align-super text-[0.62em] font-mono font-semibold text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)] focus-visible:outline-2"
        title={t("pullReceipt")}
      >
        {marker}
      </button>
      {open && (
        <span
          id={panelId}
          className="book-apparatus mt-3 mb-2 block whitespace-normal rounded-r-md border-l-2 border-[var(--primary)] bg-[var(--primary-soft)] px-4 py-3"
        >
          <dl className="divide-y divide-[var(--border)]">
            {renderReceipt(receipt, "primary")}
            {receiptSecondary && renderReceipt(receiptSecondary, "secondary")}
            <ApparatusRow term={t("reproduce")}>
              {operation.kind === "job" ? (
                <>
                  <code className="block break-all font-mono text-[12px] text-[var(--foreground)]">
                    POST /jobs
                  </code>
                  <pre className="mt-1 overflow-x-auto whitespace-pre-wrap rounded bg-[var(--surface)] px-2.5 py-2 font-mono text-[11.5px] leading-relaxed text-[var(--foreground)] ring-1 ring-[var(--border)]">
{JSON.stringify({ operation: operation.operation, payload: operation.payload }, null, 2)}
                  </pre>
                  {operation.note && (
                    <span className="mt-1 block text-[12px] text-[var(--muted)]">{operation.note}</span>
                  )}
                  <Link
                    href={`/${locale}/jobs`}
                    className="mt-2 inline-block font-medium text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
                  >
                    {t("regenerate")}
                  </Link>
                </>
              ) : (
                <>
                  <code className="block break-all font-mono text-[12px] text-[var(--foreground)]">
                    {operation.script}
                  </code>
                  {operation.note && (
                    <span className="mt-1 block text-[12px] text-[var(--muted)]">{operation.note}</span>
                  )}
                </>
              )}
            </ApparatusRow>
          </dl>
        </span>
      )}
    </span>
  );
}
