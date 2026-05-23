"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  listJobs,
  Job,
  bulkCancelJobs,
  bulkRetryJobs,
  BulkJobResult,
} from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";
import { SkeletonTableRow } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";
import { Tooltip } from "@/components/Tooltip";
import { useTranslations } from "next-intl";
import { useUrlParam } from "@/lib/useUrlParam";

const STATUS_OPTIONS = ["", "queued", "running", "succeeded", "failed", "cancelled"];

// Statuses a bulk-cancel can act on. Mirrors `cancel_job` server-side
// (cancel is a no-op on terminal jobs and would just spam errors).
const CANCELLABLE = new Set(["queued", "running"]);
// Statuses a bulk-retry can act on. Retry only makes sense on a terminal
// failure; the API decides for sure (so this is a UX guard, not enforcement).
const RETRYABLE = new Set(["failed", "cancelled"]);

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
  // Two visible filter chips replacing the previous hard-coded
  // `!parent_job_id && queue_name !== "protea.embeddings.batch"` filter.
  // Default OFF so the page lands on the exact same view as before, but the
  // hidden universe (child jobs, batch queue) is now reachable from the UI
  // instead of needing a raw curl call.
  const [showChildRaw, setShowChildRaw] = useUrlParam("children", "");
  const showChildJobs = showChildRaw === "1";
  const setShowChildJobs = (v: boolean) => setShowChildRaw(v ? "1" : null);
  const [showBatchRaw, setShowBatchRaw] = useUrlParam("batch", "");
  const showBatchJobs = showBatchRaw === "1";
  const setShowBatchJobs = (v: boolean) => setShowBatchRaw(v ? "1" : null);

  const [error, setError] = useState("");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Selection set (job ids that the operator checked). Reset on every
  // filter change so a stale tick can't accidentally cancel a job that
  // dropped out of the current view between two refreshes.
  const [selected, setSelected] = useState<Set<string>>(new Set());
  // Active bulk operation, drives the sticky progress bar + button lockout.
  const [bulk, setBulk] = useState<
    | null
    | { kind: "cancel" | "retry"; total: number; done: number; ok: number; failed: number }
  >(null);

  async function refresh(status = statusFilter, showLoader = false) {
    if (!showLoader && typeof document !== "undefined" && document.visibilityState === "hidden") return;
    if (showLoader) setLoading(true);
    try {
      setError("");
      const all = await listJobs({ limit: 500, status: status || undefined });
      // Filter is now visible-state driven (chips below). The previous
      // hard-coded mask hid two entire universes; keep the default view
      // identical by defaulting both chips to false.
      const filtered = all.filter((j) => {
        if (!showChildJobs && j.parent_job_id) return false;
        if (!showBatchJobs && j.queue_name === "protea.embeddings.batch") return false;
        return true;
      });
      setJobs(filtered);
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
    // Reset selection on any filter change so checkboxes can't survive
    // a view shift and surprise the operator.
    setSelected(new Set());
  }, [statusFilter, showChildJobs, showBatchJobs]);

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

  // Derived selection counts so the action bar can decide which buttons to
  // enable. We count *eligible* selections (not just total) because cancel
  // on a succeeded job is wasted work and noisy in the toast log.
  const selectedJobs = useMemo(() => jobs.filter((j) => selected.has(j.id)), [jobs, selected]);
  const selectableCancel = useMemo(
    () => selectedJobs.filter((j) => CANCELLABLE.has(String(j.status).toLowerCase())),
    [selectedJobs],
  );
  const selectableRetry = useMemo(
    () => selectedJobs.filter((j) => RETRYABLE.has(String(j.status).toLowerCase())),
    [selectedJobs],
  );

  function toggleOne(id: string, checked: boolean) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (checked) next.add(id); else next.delete(id);
      return next;
    });
  }

  function toggleAll(checked: boolean) {
    // Select-all is scoped to the *current filter* (= what the operator
    // can actually see), not the unfiltered universe. This matches the
    // ux-reviewer expectation and prevents a "ghost selection" footgun.
    if (!checked) { setSelected(new Set()); return; }
    setSelected(new Set(jobs.map((j) => j.id)));
  }

  const allOnPageChecked = jobs.length > 0 && jobs.every((j) => selected.has(j.id));
  const someOnPageChecked = !allOnPageChecked && jobs.some((j) => selected.has(j.id));

  async function runBulk(kind: "cancel" | "retry") {
    const targets = kind === "cancel" ? selectableCancel : selectableRetry;
    if (targets.length === 0) return;
    const ids = targets.map((j) => j.id);
    const label = kind === "cancel" ? t("bulk.cancelling") : t("bulk.retrying");
    toast(t("bulk.starting", { count: ids.length, action: label }), "info");
    setBulk({ kind, total: ids.length, done: 0, ok: 0, failed: 0 });
    const fn = kind === "cancel" ? bulkCancelJobs : bulkRetryJobs;
    const results: BulkJobResult[] = await fn(ids, (done, total, last) => {
      setBulk((prev) => prev && ({
        ...prev,
        done,
        ok: prev.ok + (last.ok ? 1 : 0),
        failed: prev.failed + (last.ok ? 0 : 1),
      }));
      // Per-step toasts get noisy fast (1-of-14, 2-of-14, …) so we only
      // surface failures inline. Successes are aggregated in the sticky bar
      // and announced once at the end.
      if (!last.ok) {
        toast(t("bulk.itemFailed", { id: last.id.slice(0, 8), error: last.error ?? "?" }), "error");
      }
    });
    const okCount = results.filter((r) => r.ok).length;
    const failCount = results.length - okCount;
    if (failCount === 0) {
      toast(t("bulk.allOk", { count: okCount, action: label }), "success");
    } else {
      toast(t("bulk.someFailed", { ok: okCount, failed: failCount }), "error");
    }
    setBulk(null);
    setSelected(new Set());
    await refresh();
  }

  // Same row interaction primitive used for both the desktop table and the
  // mobile card list so the click target stays obvious + the row link
  // stays keyboard-accessible. Stops propagation on the checkbox so a
  // ticked row doesn't also navigate.
  function RowCheckbox({ id, label }: { id: string; label: string }) {
    return (
      <input
        type="checkbox"
        aria-label={label}
        checked={selected.has(id)}
        onChange={(e) => toggleOne(id, e.target.checked)}
        onClick={(e) => e.stopPropagation()}
        className="rounded border-slate-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
      />
    );
  }

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

      {/* Visibility chips. These replace the silent hard-coded mask that
          used to hide child jobs and the protea.embeddings.batch queue.
          OFF by default so the default view is identical to the old page;
          ON exposes the hidden universe the conductor previously had to
          reach with a raw curl. */}
      <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
        <span className="text-slate-500">{t("filters.show")}</span>
        <Tooltip text={t("filters.childJobsHelp")}>
          <button
            type="button"
            aria-pressed={showChildJobs}
            onClick={() => setShowChildJobs(!showChildJobs)}
            className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 transition-colors ${
              showChildJobs
                ? "border-blue-300 bg-blue-50 text-blue-800"
                : "border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
            }`}
          >
            <span className={`inline-block h-1.5 w-1.5 rounded-full ${showChildJobs ? "bg-blue-500" : "bg-slate-300"}`} />
            {t("filters.childJobs")}
          </button>
        </Tooltip>
        <Tooltip text={t("filters.batchJobsHelp")}>
          <button
            type="button"
            aria-pressed={showBatchJobs}
            onClick={() => setShowBatchJobs(!showBatchJobs)}
            className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 transition-colors ${
              showBatchJobs
                ? "border-blue-300 bg-blue-50 text-blue-800"
                : "border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
            }`}
          >
            <span className={`inline-block h-1.5 w-1.5 rounded-full ${showBatchJobs ? "bg-blue-500" : "bg-slate-300"}`} />
            {t("filters.batchJobs")}
          </button>
        </Tooltip>
      </div>

      {error && (
        <pre className="mt-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </pre>
      )}

      {/* Sticky bulk-action bar. Appears the moment 1+ rows are selected
          (or a batch is in flight). Stays visible across mobile + desktop
          so the operator can confirm at a glance how many jobs they're
          about to touch and what the eligible subset is. */}
      {(selected.size > 0 || bulk) && (
        <div
          role="region"
          aria-label={t("bulk.regionLabel")}
          className="sticky top-2 z-30 mt-4 rounded-lg border border-blue-200 bg-blue-50/95 px-4 py-3 shadow-md backdrop-blur"
        >
          <div className="flex flex-wrap items-center gap-3">
            <div className="text-sm font-medium text-blue-900">
              {bulk
                ? t("bulk.inflight", {
                    done: bulk.done,
                    total: bulk.total,
                    action: bulk.kind === "cancel" ? t("bulk.cancelling") : t("bulk.retrying"),
                  })
                : t("bulk.selectedCount", { count: selected.size })}
            </div>

            {!bulk && (
              <div className="flex flex-wrap items-center gap-1.5 text-xs text-blue-800/80">
                <span>{t("bulk.eligible", { cancel: selectableCancel.length, retry: selectableRetry.length })}</span>
              </div>
            )}

            <div className="ml-auto flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={() => runBulk("cancel")}
                disabled={!!bulk || selectableCancel.length === 0}
                className="rounded-md border border-red-300 bg-white px-3 py-1.5 text-sm font-medium text-red-700 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-40"
              >
                {t("bulk.cancelButton", { count: selectableCancel.length })}
              </button>
              <button
                type="button"
                onClick={() => runBulk("retry")}
                disabled={!!bulk || selectableRetry.length === 0}
                className="rounded-md border border-amber-300 bg-white px-3 py-1.5 text-sm font-medium text-amber-700 hover:bg-amber-50 disabled:cursor-not-allowed disabled:opacity-40"
              >
                {t("bulk.retryButton", { count: selectableRetry.length })}
              </button>
              <button
                type="button"
                onClick={() => setSelected(new Set())}
                disabled={!!bulk || selected.size === 0}
                className="rounded-md border bg-white px-3 py-1.5 text-sm text-slate-600 hover:bg-slate-50 disabled:opacity-40"
              >
                {t("bulk.clear")}
              </button>
            </div>
          </div>

          {bulk && (
            <div className="mt-2">
              <div className="h-1.5 w-full overflow-hidden rounded-full bg-blue-100">
                <div
                  className="h-1.5 rounded-full bg-blue-500 transition-all duration-300"
                  style={{ width: `${Math.round((bulk.done / Math.max(1, bulk.total)) * 100)}%` }}
                />
              </div>
              <div className="mt-1 flex items-center justify-between text-[11px] text-blue-900/80">
                <span>{t("bulk.okFailed", { ok: bulk.ok, failed: bulk.failed })}</span>
                <span>{Math.round((bulk.done / Math.max(1, bulk.total)) * 100)}%</span>
              </div>
            </div>
          )}
        </div>
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
          <div key={j.id} className="rounded-lg border bg-white p-3 shadow-sm hover:border-blue-200 transition-colors">
            <div className="flex items-start gap-2.5">
              <span className="mt-1 shrink-0">
                <RowCheckbox id={j.id} label={t("bulk.selectRowLabel", { id: j.id.slice(0, 8) })} />
              </span>
              <Link href={`/jobs/${j.id}`} className="flex-1 min-w-0 block">
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
            </div>
          </div>
        ))}
      </div>

      {/* Desktop table */}
      <div className="mt-4 hidden lg:block overflow-hidden rounded-lg border bg-white shadow-sm">
        <div className="grid grid-cols-[36px_140px_220px_1fr_180px] gap-2 border-b bg-slate-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-500">
          <div className="flex items-center">
            <input
              type="checkbox"
              aria-label={t("bulk.selectAllLabel")}
              checked={allOnPageChecked}
              ref={(el) => { if (el) el.indeterminate = someOnPageChecked; }}
              onChange={(e) => toggleAll(e.target.checked)}
              disabled={jobs.length === 0 || !!bulk}
              className="rounded border-slate-300 text-blue-600 focus:ring-blue-500 cursor-pointer disabled:cursor-not-allowed"
            />
          </div>
          <div>{t("status")}</div>
          <div>{t("operation")}</div>
          <div>{t("operationContext")}</div>
          <div>{t("created")}</div>
        </div>

        {loading && Array.from({ length: 5 }).map((_, i) => (
          <SkeletonTableRow key={i} cols={5} />
        ))}

        {!loading && jobs.map((j) => (
          <div
            key={j.id}
            className={`grid grid-cols-[36px_140px_220px_1fr_180px] gap-2 border-b px-4 py-3 text-sm transition-colors last:border-0 items-start ${
              selected.has(j.id) ? "bg-blue-50/60" : "hover:bg-blue-50"
            }`}
          >
            <div className="flex items-center pt-1">
              <RowCheckbox id={j.id} label={t("bulk.selectRowLabel", { id: j.id.slice(0, 8) })} />
            </div>
            <Link href={`/jobs/${j.id}`} className="contents">
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
          </div>
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
