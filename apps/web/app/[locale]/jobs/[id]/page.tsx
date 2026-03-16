"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { use, useEffect, useRef, useState } from "react";
import { cancelJob, deleteJob, getJob, getJobEvents, listJobs, JobEvent, Job } from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";
import { EventTimeline } from "@/components/EventTimeline";
import { useToast } from "@/components/Toast";
import { useTranslations } from "next-intl";

const TERMINAL = ["succeeded", "failed", "cancelled"];

function formatDate(iso?: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
}

function formatEta(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function ProgressBar({
  current, total, unit = "items",
}: {
  current?: number | null;
  total?: number | null;
  unit?: string;
}) {
  if (!current && !total) return null;
  if (!total) {
    return (
      <div className="mt-2 space-y-1">
        <div className="text-xs text-gray-500">
          <span className="font-medium">{(current ?? 0).toLocaleString()} {unit} processed</span>
        </div>
        <div className="h-2.5 w-full overflow-hidden rounded-full bg-gray-100">
          <div className="h-2.5 w-1/3 rounded-full bg-blue-400 animate-pulse" />
        </div>
      </div>
    );
  }
  const pct = Math.min(100, Math.round(((current ?? 0) / total) * 100));
  return (
    <div className="mt-2 space-y-1">
      <div className="flex justify-between text-xs text-gray-500">
        <span className="font-medium">{(current ?? 0).toLocaleString()} / {total.toLocaleString()} {unit} ({pct}%)</span>
      </div>
      <div className="h-2.5 w-full overflow-hidden rounded-full bg-gray-100">
        <div
          className="h-2.5 rounded-full bg-blue-500 transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

export default function JobDetail({ params }: { params: Promise<{ id: string }> }) {
  const { id: jobId } = use(params);
  const t = useTranslations("jobs");
  const [job, setJob] = useState<any>(null);
  const [events, setEvents] = useState<JobEvent[]>([]);
  const [children, setChildren] = useState<Job[]>([]);
  const [error, setError] = useState("");
  const router = useRouter();
  const toast = useToast();
  const prevStatusRef = useRef<string | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  async function refresh() {
    try {
      setError("");
      const [j, ev, ch] = await Promise.all([
        getJob(jobId),
        getJobEvents(jobId, 200),
        listJobs({ limit: 500 }),
      ]);
      setJob(j);
      setEvents([...ev].reverse()); // chronological
      setChildren(ch.filter((c) => c.parent_job_id === jobId));
      // Notify when job reaches a terminal state
      if (prevStatusRef.current && prevStatusRef.current !== j.status) {
        if (j.status === "succeeded") toast("Job succeeded", "success");
        else if (j.status === "failed") toast("Job failed", "error");
        else if (j.status === "cancelled") toast("Job cancelled", "info");
      }
      prevStatusRef.current = j.status;
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
    if (!confirm(t("jobDetail.deleteConfirm"))) return;
    try {
      setError("");
      await deleteJob(jobId);
      toast("Job deleted", "info");
      router.push("/jobs");
    } catch (e: any) {
      setError(String(e));
      toast(String(e), "error");
    }
  }

  async function onCancel() {
    try {
      setError("");
      await cancelJob(jobId);
      toast("Job cancelled", "info");
      await refresh();
    } catch (e: any) {
      setError(String(e));
      toast(String(e), "error");
    }
  }

  const isTerminal = job && TERMINAL.includes(String(job.status).toLowerCase());
  const isLive = job && !isTerminal;

  return (
    <div>
      {/* Header */}
      <div className="flex flex-wrap items-center gap-3">
        <Link href="/jobs" className="text-sm text-gray-500 hover:text-gray-800">{t("jobDetail.backToJobs")}</Link>
        <h1 className="text-xl font-semibold">{t("jobDetail.title")}</h1>
        {isLive && (
          <span className="flex items-center gap-1 text-xs text-blue-600">
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-blue-500" />
            {t("jobDetail.live")}
          </span>
        )}
        <div className="ml-auto flex gap-2">
          <button onClick={refresh} className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
            {t("refresh")}
          </button>
          <button
            onClick={onCancel}
            disabled={isTerminal}
            className="rounded-md border px-3 py-1.5 text-sm hover:bg-gray-50 disabled:opacity-40"
          >
            {t("jobDetail.cancel")}
          </button>
          <button
            onClick={onDelete}
            disabled={isLive}
            className="rounded-md border border-red-200 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 disabled:opacity-40"
          >
            {t("jobDetail.delete")}
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
            <div><span className="text-gray-500">{t("jobDetail.queue")}</span> <span className="font-mono text-xs">{job.queue_name}</span></div>
            <div><span className="text-gray-500">{t("jobDetail.created")}</span> {formatDate(job.created_at)}</div>
            <div><span className="text-gray-500">{t("jobDetail.started")}</span> {formatDate(job.started_at)}</div>
            <div><span className="text-gray-500">{t("jobDetail.finished")}</span> {formatDate(job.finished_at)}</div>
            {job.error_code && (
              <div className="col-span-2 text-red-600">
                <span className="font-medium">{job.error_code}:</span> {job.error_message}
              </div>
            )}
          </div>

          <ProgressBar
            current={job.progress_current}
            total={job.progress_total}
            unit={
              job.operation === "insert_proteins" || job.operation === "fetch_uniprot_metadata"
                ? "proteins"
                : job.operation === "load_quickgo_annotations"
                ? "accession batches"
                : "batches"
            }
          />

          {job.payload && Object.keys(job.payload).length > 0 && (
            <details className="text-sm">
              <summary className="cursor-pointer text-gray-500 hover:text-gray-700">{t("jobDetail.payloadLabel")}</summary>
              <pre className="mt-1 rounded bg-gray-50 p-2 text-xs overflow-auto">{JSON.stringify(job.payload, null, 2)}</pre>
            </details>
          )}
        </div>
      )}

      {/* Child jobs */}
      {children.length > 0 && (
        <div className="mt-6">
          <div className="mb-3 flex items-center gap-4 flex-wrap">
            <h2 className="text-base font-semibold">
              {t("jobDetail.childJobsTitle")} <span className="text-xs font-normal text-gray-400">{t("jobDetail.childJobsCount", { count: children.length })}</span>
            </h2>
            {(["running", "queued", "succeeded", "failed", "cancelled"] as const).map((s) => {
              const n = children.filter((c) => c.status === s).length;
              if (!n) return null;
              return <span key={s} className="text-xs text-gray-500">{s}: <strong>{n}</strong></span>;
            })}
          </div>
          <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[120px_1fr_160px] gap-2 border-b bg-gray-50 px-4 py-2 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>{t("status")}</div>
              <div>{t("jobId")}</div>
              <div>{t("jobDetail.finished")}</div>
            </div>
            {[...children]
              .sort((a, b) => {
                const order: Record<string, number> = { running: 0, queued: 1, succeeded: 2, failed: 3, cancelled: 4 };
                return (order[a.status] ?? 5) - (order[b.status] ?? 5);
              })
              .map((c) => (
              <Link
                key={c.id}
                href={`/jobs/${c.id}`}
                className="grid grid-cols-[120px_1fr_160px] gap-2 border-b px-4 py-2.5 text-sm hover:bg-blue-50 transition-colors last:border-0"
              >
                <div><StatusBadge status={c.status} /></div>
                <div className="font-mono text-xs text-gray-400 truncate">{c.id}</div>
                <div className="text-xs text-gray-400">{formatDate(c.finished_at)}</div>
              </Link>
            ))}
          </div>
        </div>
      )}

      {/* Events */}
      <div className="mt-6">
        <h2 className="mb-3 text-base font-semibold">
          {t("jobDetail.eventsTitle")} <span className="text-xs font-normal text-gray-400">{t("jobDetail.eventsCount", { count: events.length })}</span>
        </h2>
        <EventTimeline events={events} />
      </div>
    </div>
  );
}
