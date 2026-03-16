"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { baseUrl } from "@/lib/api";
import { useTranslations } from "next-intl";

type SupportData = {
  count: number;
  comments: { id: string; comment: string; created_at: string }[];
};

export function SupportButton() {
  const t = useTranslations("components.supportButton");
  const [data, setData] = useState<SupportData | null>(null);
  const [open, setOpen] = useState(false);
  const [comment, setComment] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetch(`${baseUrl()}/support`, { cache: "no-store" })
      .then((r) => r.json())
      .then(setData)
      .catch(() => {});
  }, []);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  async function handleSubmit() {
    setSubmitting(true);
    try {
      const res = await fetch(`${baseUrl()}/support`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ comment: comment.trim() || null }),
      });
      const json = await res.json();
      setData((prev) => prev ? { ...prev, count: json.count } : { count: json.count, comments: [] });
      setSubmitted(true);
      setComment("");
      setTimeout(() => setOpen(false), 1500);
    } finally {
      setSubmitting(false);
    }
  }

  const count = data?.count ?? null;

  return (
    <div ref={ref} className="relative">
      {/* Trigger button with tooltip */}
      <div className="group relative inline-block">
        <button
          onClick={() => { setOpen((v) => !v); setSubmitted(false); }}
          className="flex items-center gap-1.5 rounded-full border border-gray-200 bg-white px-2.5 sm:px-3 py-1.5 text-sm text-gray-600 hover:border-blue-300 hover:text-blue-600 transition-colors shadow-sm"
        >
          <span className="text-base leading-none">👍</span>
          <span className="font-medium hidden sm:inline">{t("support")}</span>
          {count !== null && (
            <span className="rounded-full bg-blue-50 px-1.5 py-0.5 text-xs font-semibold text-blue-600">
              {count.toLocaleString()}
            </span>
          )}
        </button>
        <span className="pointer-events-none absolute bottom-full right-0 mb-2 z-20 hidden group-hover:block w-64 rounded-md border border-gray-200 bg-white px-3 py-2 text-xs text-gray-500 shadow-lg leading-relaxed">
          {t("tooltip")}
        </span>
      </div>

      {/* Popover */}
      {open && (
        <div className="absolute right-0 top-full mt-2 z-30 w-80 max-w-[calc(100vw-2rem)] rounded-xl border border-gray-200 bg-white shadow-xl">
          <div className="p-4 space-y-3">
            {submitted ? (
              <p className="text-center text-sm font-medium text-green-600 py-2">
                {t("thanks")}
              </p>
            ) : (
              <>
                <p className="text-sm font-semibold text-gray-800">{t("projectSupport")}</p>
                <textarea
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                  placeholder={t("commentPlaceholder")}
                  maxLength={500}
                  rows={3}
                  className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm resize-none focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <p className="text-xs text-gray-400">
                  {t("publicNote")}
                </p>
                <button
                  onClick={handleSubmit}
                  disabled={submitting}
                  className="w-full rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 transition-colors"
                >
                  {submitting ? t("sending") : t("sendThumbsUp")}
                </button>
              </>
            )}
          </div>

          {/* Recent comments */}
          {data && data.comments?.length > 0 && (
            <div className="border-t border-gray-100 px-4 py-3 space-y-2 max-h-48 overflow-y-auto">
              <div className="flex items-center justify-between">
                <p className="text-xs font-medium text-gray-400 uppercase tracking-wide">{t("recentComments")}</p>
                <Link href="/support" className="text-xs text-blue-500 hover:underline" onClick={() => setOpen(false)}>
                  {t("viewAll")}
                </Link>
              </div>
              {data.comments.map((c) => (
                <div key={c.id} className="text-xs text-gray-600 leading-relaxed">
                  <span className="text-gray-400 mr-1">{new Date(c.created_at).toLocaleDateString()}</span>
                  {c.comment}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
