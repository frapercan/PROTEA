"use client";

import { useTranslations } from "next-intl";

type Status = "queued" | "running" | "succeeded" | "failed" | "cancelled" | string;

const STYLES: Record<string, string> = {
  queued: "bg-yellow-100 text-yellow-800 border-yellow-200",
  running: "bg-blue-100 text-blue-800 border-blue-200",
  succeeded: "bg-green-100 text-green-800 border-green-200",
  failed: "bg-red-100 text-red-800 border-red-200",
  cancelled: "bg-slate-100 text-slate-600 border-slate-200",
};

const KNOWN_STATUSES = ["queued", "running", "succeeded", "failed", "cancelled"] as const;
type KnownStatus = typeof KNOWN_STATUSES[number];

/**
 * Leading glyph per status. Communicates the state via shape, redundant
 * with color so colorblind users (and tiny-screen readers) still parse
 * the badge at a glance.
 */
function StatusGlyph({ status }: { status: string }) {
  switch (status) {
    case "queued":
      return (
        <svg className="h-3 w-3" aria-hidden fill="none" viewBox="0 0 12 12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="6" cy="6" r="4.5" />
          <path d="M6 3.5v2.7l1.7 1.1" />
        </svg>
      );
    case "running":
      return (
        <span aria-hidden className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-blue-500" />
      );
    case "succeeded":
      return (
        <svg className="h-3 w-3" aria-hidden fill="none" viewBox="0 0 12 12" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M2.5 6.5l2.5 2.5L9.5 3.5" />
        </svg>
      );
    case "failed":
      return (
        <svg className="h-3 w-3" aria-hidden fill="none" viewBox="0 0 12 12" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M3 3l6 6M9 3l-6 6" />
        </svg>
      );
    case "cancelled":
      return (
        <svg className="h-3 w-3" aria-hidden fill="none" viewBox="0 0 12 12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="6" cy="6" r="4.5" />
          <path d="M3 9L9 3" />
        </svg>
      );
    default:
      return null;
  }
}

export function StatusBadge({ status }: { status: Status }) {
  const t = useTranslations("components.statusBadge");
  const key = status.toLowerCase();
  const cls = STYLES[key] ?? "bg-slate-100 text-slate-600 border-slate-200";
  const isKnown = KNOWN_STATUSES.includes(key as KnownStatus);
  const label = isKnown ? t(key as KnownStatus) : status.toUpperCase();
  return (
    <span
      role="status"
      aria-label={`Status: ${label}`}
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium ${cls}`}
    >
      <StatusGlyph status={key} />
      {label}
    </span>
  );
}
