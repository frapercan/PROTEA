"use client";

import { useState } from "react";
import { useTranslations } from "next-intl";

/**
 * Client island for the "copy share link" affordance on the homepage
 * best-result spotlight. Extracted from the home page so the rest of
 * the spotlight (a server component) ships zero JS.
 *
 * Copies a deep link with the evaluation_result_id as a hash so the URL
 * reproduces the same view and scrolls to the section. Falls back to a
 * no-op when navigator.clipboard is unavailable (older browsers / non
 * HTTPS); the URL bar still works as a manual fallback.
 */
export function ShareBestLinkButton({
  evaluationResultId,
}: {
  evaluationResultId: string;
}) {
  const t = useTranslations("home");
  const [copied, setCopied] = useState(false);

  function handleCopy() {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.hash = `best-${evaluationResultId}`;
    const value = url.toString();
    const flash = () => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    };
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(value).then(flash).catch(() => {
        // ignore; user can still copy from the URL bar
      });
    } else {
      flash();
    }
  }

  return (
    <button
      type="button"
      onClick={handleCopy}
      aria-live="polite"
      className="inline-flex min-h-[40px] items-center gap-1.5 rounded-md px-3 py-2 text-[13px] font-medium text-slate-700 ring-1 ring-inset ring-slate-200 bg-white hover:bg-slate-50 hover:ring-slate-300 transition-colors"
    >
      <svg
        aria-hidden
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.75"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="w-4 h-4"
      >
        <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
        <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
      </svg>
      {copied ? t("shareCopied" as never) : t("shareCopy" as never)}
    </button>
  );
}
