"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { listJobs, Job } from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";
import { JobForm } from "@/components/JobForm";
import { useRouter } from "next/navigation";

const STATUS_OPTIONS = ["", "queued", "running", "succeeded", "failed", "cancelled"];

function formatDate(iso?: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
}

export default function JobsPage() {
  const router = useRouter();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [statusFilter, setStatusFilter] = useState("");
  const [error, setError] = useState("");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  async function refresh(status = statusFilter) {
    try {
      setError("");
      setJobs(await listJobs({ limit: 100, status: status || undefined }));
    } catch (e: any) {
      setError(String(e));
    }
  }

  useEffect(() => {
    refresh();
  }, [statusFilter]);

  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(() => refresh(), 3000);
    } else {
      if (intervalRef.current) clearInterval(intervalRef.current);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh, statusFilter]);

  function onJobCreated(id: string) {
    setShowForm(false);
    router.push(`/jobs/${id}`);
  }

  return (
    <>
      {showForm && <JobForm onCreated={onJobCreated} onClose={() => setShowForm(false)} />}

      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-xl font-semibold">Jobs</h1>

        <div className="ml-auto flex flex-wrap items-center gap-2">
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="rounded-md border bg-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {STATUS_OPTIONS.map((s) => (
              <option key={s} value={s}>{s ? s.toUpperCase() : "All statuses"}</option>
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
            onClick={() => refresh()}
            className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50"
          >
            Refresh
          </button>

          <button
            onClick={() => setShowForm(true)}
            className="rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700"
          >
            + New Job
          </button>
        </div>
      </div>

      {error && (
        <pre className="mt-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </pre>
      )}

      <div className="mt-4 overflow-hidden rounded-lg border bg-white shadow-sm">
        <div className="grid grid-cols-[140px_160px_1fr_180px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
          <div>Status</div>
          <div>Operation</div>
          <div>Job ID</div>
          <div>Created</div>
        </div>

        {jobs.map((j) => (
          <Link
            key={j.id}
            href={`/jobs/${j.id}`}
            className="grid grid-cols-[140px_160px_1fr_180px] gap-2 border-b px-4 py-3 text-sm hover:bg-blue-50 transition-colors last:border-0"
          >
            <div><StatusBadge status={j.status} /></div>
            <div className="text-gray-700 truncate">{j.operation}</div>
            <div className="font-mono text-xs text-gray-400 truncate">{j.id}</div>
            <div className="text-gray-500 text-xs">{formatDate(j.created_at)}</div>
          </Link>
        ))}

        {jobs.length === 0 && (
          <div className="px-4 py-8 text-center text-sm text-gray-400">
            No jobs found.{" "}
            <button onClick={() => setShowForm(true)} className="text-blue-600 underline">
              Create one
            </button>
          </div>
        )}
      </div>

      <p className="mt-2 text-xs text-gray-400">{jobs.length} job{jobs.length !== 1 ? "s" : ""} shown</p>
    </>
  );
}
