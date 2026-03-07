"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { use, useEffect, useRef, useState } from "react";
import { cancelJob, deleteJob, getJob, getJobEvents, JobEvent } from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";
import { EventTimeline } from "@/components/EventTimeline";

const TERMINAL = ["succeeded", "failed", "cancelled"];

function formatDate(iso?: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
}

function ProgressBar({ current, total }: { current?: number | null; total?: number | null }) {
  if (!total) return null;
  const pct = Math.round(((current ?? 0) / total) * 100);
  return (
    <div className="mt-2">
      <div className="flex justify-between text-xs text-gray-500 mb-1">
        <span>Progress</span>
        <span>{current ?? 0} / {total} ({pct}%)</span>
      </div>
      <div className="h-2 w-full overflow-hidden rounded-full bg-gray-100">
        <div className="h-2 rounded-full bg-blue-500 transition-all" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

export default function JobDetail({ params }: { params: Promise<{ id: string }> }) {
  const { id: jobId } = use(params);
  const [job, setJob] = useState<any>(null);
  const [events, setEvents] = useState<JobEvent[]>([]);
  const [error, setError] = useState("");
  const router = useRouter();
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  async function refresh() {
    try {
      setError("");
      const [j, ev] = await Promise.all([getJob(jobId), getJobEvents(jobId, 200)]);
      setJob(j);
      setEvents([...ev].reverse()); // chronological
    } catch (e: any) {
      setError(String(e));
    }
  }

  useEffect(() => {
    refresh();
  }, [jobId]);

  // Auto-refresh while job is active
  useEffect(() => {
    if (!job) return;
    const isTerminal = TERMINAL.includes(String(job.status).toLowerCase());
    if (!isTerminal) {
      intervalRef.current = setInterval(refresh, 2000);
    } else {
      if (intervalRef.current) clearInterval(intervalRef.current);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [job?.status]);

  async function onDelete() {
    if (!confirm("Delete this job?")) return;
    try {
      setError("");
      await deleteJob(jobId);
      router.push("/jobs");
    } catch (e: any) {
      setError(String(e));
    }
  }

  async function onCancel() {
    try {
      setError("");
      await cancelJob(jobId);
      await refresh();
    } catch (e: any) {
      setError(String(e));
    }
  }

  const isTerminal = job && TERMINAL.includes(String(job.status).toLowerCase());
  const isLive = job && !isTerminal;

  return (
    <div>
      {/* Header */}
      <div className="flex flex-wrap items-center gap-3">
        <Link href="/jobs" className="text-sm text-gray-500 hover:text-gray-800">← Jobs</Link>
        <h1 className="text-xl font-semibold">Job Detail</h1>
        {isLive && (
          <span className="flex items-center gap-1 text-xs text-blue-600">
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-blue-500" />
            Live
          </span>
        )}
        <div className="ml-auto flex gap-2">
          <button onClick={refresh} className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
            Refresh
          </button>
          <button
            onClick={onCancel}
            disabled={isTerminal}
            className="rounded-md border px-3 py-1.5 text-sm hover:bg-gray-50 disabled:opacity-40"
          >
            Cancel
          </button>
          <button
            onClick={onDelete}
            disabled={isLive}
            className="rounded-md border border-red-200 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 disabled:opacity-40"
          >
            Delete
          </button>
        </div>
      </div>

      {error && (
        <pre className="mt-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </pre>
      )}

      {/* Job card */}
      {job && (
        <div className="mt-4 rounded-lg border bg-white p-4 shadow-sm space-y-3">
          <div className="flex items-center gap-3">
            <StatusBadge status={job.status} />
            <span className="font-semibold text-gray-800">{job.operation}</span>
            <span className="font-mono text-xs text-gray-400">{jobId}</span>
          </div>

          <div className="grid grid-cols-2 gap-x-6 gap-y-1 text-sm">
            <div><span className="text-gray-500">Queue:</span> <span className="font-mono text-xs">{job.queue_name}</span></div>
            <div><span className="text-gray-500">Created:</span> {formatDate(job.created_at)}</div>
            <div><span className="text-gray-500">Started:</span> {formatDate(job.started_at)}</div>
            <div><span className="text-gray-500">Finished:</span> {formatDate(job.finished_at)}</div>
            {job.error_code && (
              <div className="col-span-2 text-red-600">
                <span className="font-medium">{job.error_code}:</span> {job.error_message}
              </div>
            )}
          </div>

          <ProgressBar current={job.progress_current} total={job.progress_total} />

          {job.payload && Object.keys(job.payload).length > 0 && (
            <details className="text-sm">
              <summary className="cursor-pointer text-gray-500 hover:text-gray-700">Payload</summary>
              <pre className="mt-1 rounded bg-gray-50 p-2 text-xs overflow-auto">{JSON.stringify(job.payload, null, 2)}</pre>
            </details>
          )}
        </div>
      )}

      {/* Events */}
      <div className="mt-6">
        <h2 className="mb-3 text-base font-semibold">
          Events <span className="text-xs font-normal text-gray-400">({events.length})</span>
        </h2>
        <EventTimeline events={events} />
      </div>
    </div>
  );
}
