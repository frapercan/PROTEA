"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { listJobs, Job } from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";
import { SkeletonTableRow } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";
import { useTranslations } from "next-intl";
import { useUrlParam } from "@/lib/useUrlParam";

const STATUS_OPTIONS = ["", "queued", "running", "succeeded", "failed", "cancelled"];

function formatDate(iso?: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
}

function InlineProgress({
  current, total,
}: {
  current?: number | null;
  total?: number | null;
}) {
  if (!current && !total) return null;
  if (!total) {
    return (
      <div className="mt-1 flex items-center gap-2">
        <div className="h-1.5 w-24 overflow-hidden rounded-full bg-slate-100">
          <div className="h-1.5 w-8 rounded-full bg-blue-400 animate-pulse" />
        </div>
        <span className="text-xs text-slate-600">{(current ?? 0).toLocaleString()}</span>
      </div>
    );
  }
  const pct = Math.min(100, Math.round(((current ?? 0) / total) * 100));
  return (
    <div className="mt-1 flex items-center gap-2">
      <div className="h-1.5 w-24 overflow-hidden rounded-full bg-slate-100">
        <div
          className="h-1.5 rounded-full bg-blue-400 transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-xs text-slate-600">{pct}%</span>
    </div>
  );
}

export default function JobsPage() {
  const t = useTranslations("jobs");
  const toast = useToast();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [statusFilterRaw, setStatusFilterRaw] = useUrlParam("status", "");
  const statusFilter = statusFilterRaw ?? "";
  const setStatusFilter = (v: string) => setStatusFilterRaw(v === "" ? null : v);
  const [error, setError] = useState("");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  async function refresh(status = statusFilter, showLoader = false) {
    if (!showLoader && typeof document !== "undefined" && document.visibilityState === "hidden") return;
    if (showLoader) setLoading(true);
    try {
      setError("");
      const all = await listJobs({ limit: 500, status: status || undefined });
      setJobs(all.filter((j) => !j.parent_job_id && j.queue_name !== "protea.embeddings.batch"));
    } catch (e: any) {
      const msg = String(e);
      setError(msg);
      toast(msg, "error");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh(statusFilter, true);
  }, [statusFilter]);

  // Auto-refresh: faster when there are active jobs, slower otherwise.
  // Pauses automatically when the tab is hidden (refresh() checks
  // document.visibilityState) and forces a refresh on visibilitychange.
  useEffect(() => {
    if (!autoRefresh) {
      if (intervalRef.current) clearInterval(intervalRef.current);
      return;
    }
    function schedule() {
      const hasActive = jobs.some((j) => j.status === "running" || j.status === "queued");
      return hasActive ? 3000 : 8000;
    }
    intervalRef.current = setInterval(() => refresh(), schedule());
    const onVisibility = () => {
      if (document.visibilityState === "visible") refresh();
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [autoRefresh, statusFilter, jobs]);

  const activeCount = jobs.filter((j) => j.status === "running" || j.status === "queued").length;

  return (
    <>
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-xl font-semibold">{t("title")}</h1>
        {activeCount > 0 && (
          <span className="flex items-center gap-1.5 rounded-full bg-blue-50 px-2.5 py-0.5 text-xs font-medium text-blue-700 border border-blue-100">
            <span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-blue-500" />
            {t("activeJobs", { count: activeCount })}
          </span>
        )}

        <div className="ml-auto flex flex-wrap items-center gap-2">
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="rounded-md border bg-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {STATUS_OPTIONS.map((s) => (
              <option key={s} value={s}>{s ? s.charAt(0).toUpperCase() + s.slice(1) : t("allStatuses")}</option>
            ))}
          </select>

          <label className="flex items-center gap-1.5 text-sm text-slate-600 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(e) => setAutoRefresh(e.target.checked)}
              className="rounded"
            />
            {t("autoRefresh")}
            {autoRefresh && <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-green-500" />}
          </label>

          <button
            onClick={() => refresh(statusFilter)}
            className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-slate-50"
          >
            {t("refresh")}
          </button>
        </div>
      </div>

      {error && (
        <pre className="mt-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </pre>
      )}

      {/* Mobile card list */}
      <div className="mt-4 lg:hidden space-y-2">
        {loading && Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="rounded-lg border bg-white p-3 shadow-sm animate-pulse space-y-2">
            <div className="h-4 bg-slate-200 rounded w-24" />
            <div className="h-3 bg-slate-100 rounded w-40" />
          </div>
        ))}
        {!loading && jobs.length === 0 && (
          <div className="rounded-lg border bg-white px-4 py-8 text-center text-sm text-slate-600 shadow-sm">{t("noJobsFound")}</div>
        )}
        {!loading && jobs.map((j) => (
          <Link key={j.id} href={`/jobs/${j.id}`} className="block rounded-lg border bg-white p-3 shadow-sm hover:border-blue-200 hover:bg-blue-50 transition-colors">
            <div className="flex items-start justify-between gap-2">
              <StatusBadge status={j.status} />
              <span className="text-xs text-slate-600">{formatDate(j.created_at)}</span>
            </div>
            <p className="mt-1.5 text-sm font-medium text-slate-800">{j.operation}</p>
            {j.operation_description && (
              <p className="text-xs text-slate-500 leading-snug">{j.operation_description}</p>
            )}
            {j.operation_summary && (
              <p className="mt-1 text-xs font-mono text-slate-700 break-words">{j.operation_summary}</p>
            )}
            <InlineProgress current={j.progress_current} total={j.progress_total} />
            <p className="mt-1 font-mono text-xs text-slate-600 truncate">{j.id}</p>
          </Link>
        ))}
      </div>

      {/* Desktop table */}
      <div className="mt-4 hidden lg:block overflow-hidden rounded-lg border bg-white shadow-sm">
        <div className="grid grid-cols-[140px_220px_1fr_180px] gap-2 border-b bg-slate-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-500">
          <div>{t("status")}</div>
          <div>{t("operation")}</div>
          <div>{t("operationContext")}</div>
          <div>{t("created")}</div>
        </div>

        {loading && Array.from({ length: 5 }).map((_, i) => (
          <SkeletonTableRow key={i} cols={4} />
        ))}

        {!loading && jobs.map((j) => (
          <Link
            key={j.id}
            href={`/jobs/${j.id}`}
            className="grid grid-cols-[140px_220px_1fr_180px] gap-2 border-b px-4 py-3 text-sm hover:bg-blue-50 transition-colors last:border-0 items-start"
          >
            <div><StatusBadge status={j.status} /></div>
            <div>
              <span className="text-slate-700 font-medium block truncate">{j.operation}</span>
              {j.operation_description && (
                <span className="text-xs text-slate-500 leading-snug line-clamp-2 block">{j.operation_description}</span>
              )}
              <InlineProgress current={j.progress_current} total={j.progress_total} />
            </div>
            <div className="space-y-0.5">
              {j.operation_summary ? (
                <span className="text-xs font-mono text-slate-700 break-words block">{j.operation_summary}</span>
              ) : (
                <span className="text-xs text-slate-300">—</span>
              )}
              <span className="font-mono text-[10px] text-slate-600 truncate block">{j.id}</span>
            </div>
            <div className="text-slate-500 text-xs">{formatDate(j.created_at)}</div>
          </Link>
        ))}

        {!loading && jobs.length === 0 && (
          <div className="px-4 py-8 text-center text-sm text-slate-600">
            {t("noJobsFound")}
          </div>
        )}
      </div>

      <p className="mt-2 text-xs text-slate-600">{jobs.length} job{jobs.length !== 1 ? "s" : ""} shown</p>
    </>
  );
}
