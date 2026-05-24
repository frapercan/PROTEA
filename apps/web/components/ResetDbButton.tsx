"use client";

import { useCallback, useId, useState } from "react";
import { useTranslations } from "next-intl";
import { authHeaders } from "@/lib/auth";
import { useHasRole } from "@/lib/useRole";
import { useFocusTrap } from "@/lib/useFocusTrap";

export function ResetDbButton() {
  const t = useTranslations("components.resetDbButton");
  const [showConfirm, setShowConfirm] = useState(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<"ok" | "error" | null>(null);
  const titleId = useId();
  const canAdmin = useHasRole("admin");

  const closeConfirm = useCallback(() => {
    if (!loading) setShowConfirm(false);
  }, [loading]);

  const dialogRef = useFocusTrap<HTMLDivElement>(showConfirm, closeConfirm);

  async function handleReset() {
    setLoading(true);
    setResult(null);
    try {
      const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/admin/reset-db`, {
        method: "POST",
        headers: { ...authHeaders() },
      });
      const data = await res.json();
      setResult(data.ok ? "ok" : "error");
    } catch {
      setResult("error");
    } finally {
      setLoading(false);
      setShowConfirm(false);
    }
  }

  // FEAT-AUTH: hide the destructive control from non-admin sessions.
  // The backend gate still enforces; this is purely affordance hygiene
  // so viewers don't see a button that will only ever 403.
  if (!canAdmin) return null;

  return (
    <>
      <button
        onClick={() => { setResult(null); setShowConfirm(true); }}
        aria-label={t("button")}
        className="inline-flex h-9 items-center gap-1.5 rounded-lg border border-red-200 bg-red-50 px-3 text-[12px] font-semibold text-red-600 hover:bg-red-100 hover:border-red-300 transition-colors"
      >
        <svg aria-hidden="true" className="w-3.5 h-3.5" fill="none" viewBox="0 0 14 14" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M3 4h8M5.5 4v-1a1 1 0 011-1h1a1 1 0 011 1v1M4 4l.5 8a1 1 0 001 1h3a1 1 0 001-1L10 4" />
        </svg>
        {t("button")}
      </button>

      {result === "ok" && (
        <span className="ml-2 text-xs text-green-600 font-medium">{t("resetOk")}</span>
      )}
      {result === "error" && (
        <span className="ml-2 text-xs text-red-600 font-medium">{t("error")}</span>
      )}

      {showConfirm && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
          onClick={closeConfirm}
        >
          <div
            ref={dialogRef}
            role="dialog"
            aria-modal="true"
            aria-labelledby={titleId}
            className="w-full max-w-sm rounded-xl border bg-white shadow-xl p-6"
            onClick={(e) => e.stopPropagation()}
          >
            <h2 id={titleId} className="text-base font-semibold text-slate-900">{t("confirmTitle")}</h2>
            <p className="mt-2 text-sm text-slate-500">{t("confirmMessage")}</p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                onClick={() => setShowConfirm(false)}
                disabled={loading}
                className="rounded-md border px-4 py-2 text-sm hover:bg-slate-50 disabled:opacity-50"
              >
                {t("cancel")}
              </button>
              <button
                onClick={handleReset}
                disabled={loading}
                className="rounded-md bg-red-600 px-4 py-2 text-sm text-white hover:bg-red-700 disabled:opacity-50"
              >
                {loading ? t("confirming") : t("confirm")}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
