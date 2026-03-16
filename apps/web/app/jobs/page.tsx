"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { listJobs, Job } from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";
import { SkeletonTableRow } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";
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
        <div className="h-1.5 w-24 overflow-hidden rounded-full bg-gray-100">
          <div className="h-1.5 w-8 rounded-full bg-blue-400 animate-pulse" />
        </div>
        <span className="text-xs text-gray-400">{(current ?? 0).toLocaleString()}</span>
      </div>
    );
  }
  const pct = Math.min(100, Math.round(((current ?? 0) / total) * 100));
  return (
    <div className="mt-1 flex items-center gap-2">
      <div className="h-1.5 w-24 overflow-hidden rounded-full bg-gray-100">
        <div
          className="h-1.5 rounded-full bg-blue-400 transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-xs text-gray-400">{pct}%</span>
    </div>
  );
}

export default function JobsPage() {
  const toast = useToast();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState("");
  const [error, setError] = useState("");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  async function refresh(status = statusFilter, showLoader = false) {
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

  // Auto-refresh: faster when there are active jobs, slower otherwise
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
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh, statusFilter, jobs]);

  const activeCount = jobs.filter((j) => j.status === "running" || j.status === "queued").length;

  return (
    <>
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-xl font-semibold">Jobs</h1>
        {activeCount > 0 && (
          <span className="flex items-center gap-1.5 rounded-full bg-blue-50 px-2.5 py-0.5 text-xs font-medium text-blue-700 border border-blue-100">
            <span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-blue-500" />
            {activeCount} active
          </span>
        )}

        <div className="ml-auto flex flex-wrap items-center gap-2">
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="rounded-md border bg-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {STATUS_OPTIONS.map((s) => (
              <option key={s} value={s}>{s ? s.charAt(0).toUpperCase() + s.slice(1) : "All statuses"}</option>
            ))}
          </select>

          <label className="flex items-center gap-1.5 text-sm text-gray-600 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(e) => setAutoRefresh(e.target.checked)}
              className="rounded"
            />
            Auto-refresh
            {autoRefresh && <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-green-500" />}
          </label>

          <button
            onClick={() => refresh(statusFilter)}
            className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50"
          >
            Refresh
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
            <div className="h-4 bg-gray-200 rounded w-24" />
            <div className="h-3 bg-gray-100 rounded w-40" />
          </div>
        ))}
        {!loading && jobs.length === 0 && (
          <div className="rounded-lg border bg-white px-4 py-8 text-center text-sm text-gray-400 shadow-sm">No jobs found.</div>
        )}
        {!loading && jobs.map((j) => (
          <Link key={j.id} href={`/jobs/${j.id}`} className="block rounded-lg border bg-white p-3 shadow-sm hover:border-blue-200 hover:bg-blue-50 transition-colors">
            <div className="flex items-start justify-between gap-2">
              <StatusBadge status={j.status} />
              <span className="text-xs text-gray-400">{formatDate(j.created_at)}</span>
            </div>
            <p className="mt-1.5 text-sm font-medium text-gray-800">{j.operation}</p>
            <InlineProgress current={j.progress_current} total={j.progress_total} />
            <p className="mt-1 font-mono text-xs text-gray-400 truncate">{j.id}</p>
          </Link>
        ))}
      </div>

      {/* Desktop table */}
      <div className="mt-4 hidden lg:block overflow-hidden rounded-lg border bg-white shadow-sm">
        <div className="grid grid-cols-[140px_180px_1fr_180px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
          <div>Status</div>
          <div>Operation</div>
          <div>Job ID</div>
          <div>Created</div>
        </div>

        {loading && Array.from({ length: 5 }).map((_, i) => (
          <SkeletonTableRow key={i} cols={4} />
        ))}

        {!loading && jobs.map((j) => (
          <Link
            key={j.id}
            href={`/jobs/${j.id}`}
            className="grid grid-cols-[140px_180px_1fr_180px] gap-2 border-b px-4 py-3 text-sm hover:bg-blue-50 transition-colors last:border-0 items-start"
          >
            <div><StatusBadge status={j.status} /></div>
            <div>
              <span className="text-gray-700 truncate block">{j.operation}</span>
              <InlineProgress current={j.progress_current} total={j.progress_total} />
            </div>
            <div className="font-mono text-xs text-gray-400 truncate">{j.id}</div>
            <div className="text-gray-500 text-xs">{formatDate(j.created_at)}</div>
          </Link>
        ))}

        {!loading && jobs.length === 0 && (
          <div className="px-4 py-8 text-center text-sm text-gray-400">
            No jobs found.
          </div>
        )}
      </div>

      <p className="mt-2 text-xs text-gray-400">{jobs.length} job{jobs.length !== 1 ? "s" : ""} shown</p>
    </>
  );
}
