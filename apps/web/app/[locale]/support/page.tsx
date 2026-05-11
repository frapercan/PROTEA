"use client";

import { useEffect, useState } from "react";
import { baseUrl } from "@/lib/api";
import { Skeleton } from "@/components/Skeleton";
import { useTranslations } from "next-intl";

type Comment = { id: string; comment: string; created_at: string };
type SupportData = { count: number; comments: Comment[] };

function timeAgo(iso: string): string {
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

export default function SupportPage() {
  const t = useTranslations("support");
  const [data, setData] = useState<SupportData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${baseUrl()}/support?all_comments=true`, { cache: "no-store" })
      .then((r) => r.json())
      .then(setData)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="max-w-2xl space-y-10">
        <div className="rounded-2xl border border-blue-100 bg-blue-50 px-8 py-10 text-center space-y-3">
          <Skeleton className="h-14 w-14 mx-auto" />
          <Skeleton className="h-12 w-32 mx-auto" />
          <Skeleton className="h-4 w-48 mx-auto" />
          <Skeleton className="h-3 w-40 mx-auto" />
        </div>
        <div className="space-y-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="rounded-xl border border-slate-100 bg-white px-5 py-4 shadow-sm space-y-2">
              <Skeleton className="h-3 w-3/4" />
              <Skeleton className="h-3 w-1/2" />
              <Skeleton className="h-3 w-16" />
            </div>
          ))}
        </div>
      </div>
    );
  }
  if (!data) return <div className="p-8 text-sm text-red-500">Could not load support data.</div>;

  const withComments = data.comments.length;
  const anonymous = data.count - withComments;

  return (
    <div className="max-w-2xl space-y-10">

      {/* Hero */}
      <div className="rounded-2xl border border-blue-100 bg-blue-50 px-8 py-10 text-center space-y-3">
        <div className="text-6xl">👍</div>
        <div className="text-5xl font-bold text-blue-700">{data.count.toLocaleString()}</div>
        <div className="text-base text-blue-500 font-medium">{t("hero.supportCount", { count: data.count })}</div>
        <div className="text-xs text-blue-400 pt-1">
          {t("hero.withComments", { count: withComments })} · {t("hero.anonymous", { count: anonymous })}
        </div>
      </div>

      {/* Comments */}
      {data.comments.length > 0 && (
        <section className="space-y-3">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-600">
            {t("commentsSection.heading")}
          </h2>
          <div className="space-y-3">
            {data.comments.map((c) => (
              <div
                key={c.id}
                className="rounded-xl border border-slate-100 bg-white px-5 py-4 shadow-sm"
              >
                <p className="text-sm text-slate-700 leading-relaxed">{c.comment}</p>
                <p className="mt-2 text-xs text-slate-600">{timeAgo(c.created_at)}</p>
              </div>
            ))}
          </div>
        </section>
      )}

      {data.comments.length === 0 && (
        <p className="text-center text-sm text-slate-600">{t("commentsSection.noComments")}</p>
      )}
    </div>
  );
}
