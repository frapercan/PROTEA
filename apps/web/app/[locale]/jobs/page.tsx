"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { bulkCancelJobs, listJobs, Job } from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";
import { SkeletonTableRow } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";
import { useLocale, useTranslations } from "next-intl";
import { useUrlParam } from "@/lib/useUrlParam";

const STATUS_OPTIONS = ["", "queued", "running", "succeeded", "failed", "cancelled"];
const BATCH_QUEUE = "protea.embeddings.batch";
const CANCELLABLE_STATUSES = new Set(["queued", "running"]);

function formatDate(iso?: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
}

function InlineProgress({
  current, total, status,
}: {
  current?: number | null;
  total?: number | null;
  status?: string;
}) {
  // A SUCCEEDED job is 100% done by definition. Coordinators that finalize in
  // deferred child batches can leave progress_current at a stale sub-total
  // (e.g. 24/26), which used to render a misleading <100% bar on a job that
  // actually finished. Always show a full bar for a terminal success.
  if (String(status ?? "").toLowerCase() === "succeeded") {
    return (
      <div className="mt-1 flex items-center gap-2">
        <div className="h-1.5 w-24 overflow-hidden rounded-full bg-emerald-100">
          <div className="h-1.5 w-full rounded-full bg-emerald-500" />
        </div>
        <span className="text-xs text-slate-600">100%</span>
      </div>
    );
  }
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

// "Operator-or-above" gate. The backend's RBAC story is bearer-or-apiKey
// (see AuthChip + protea/api/security.py). We do not show the destructive
// bulk-cancel action to anonymous viewers; the API would 401/403 anyway,
// but failing in the UI is friendlier.
//
// When PR #456 lands its useRole/useHasRole hook, replace `isOperator`
// with `useHasRole("operator")` and delete the localStorage probe below.
function useIsAuthenticated(): boolean {
  const [authed, setAuthed] = useState(false);
  useEffect(() => {
    function read() {
      try {
        const bearer = window.localStorage.getItem("protea.bearer");
        const apiKey = window.localStorage.getItem("protea.apiKey");
        setAuthed(Boolean((bearer && bearer.length > 16) || (apiKey && apiKey.length > 8)));
      } catch {
        setAuthed(false);
      }
    }
    read();
    const onStorage = (e: StorageEvent) => {
      if (!e.key || e.key.startsWith("protea.")) read();
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);
  return authed;
}

export default function JobsPage() {
  const t = useTranslations("jobs");
  const locale = useLocale();
  const toast = useToast();
  const isAuthed = useIsAuthenticated();

  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [statusFilterRaw, setStatusFilterRaw] = useUrlParam("status", "");
  const statusFilter = statusFilterRaw ?? "";
  const setStatusFilter = (v: string) => setStatusFilterRaw(v === "" ? null : v);

  // FILTER CHIPS — replace the silent hard-coded filter. Both default to
  // false to keep the historical jobs view (no child jobs, no batch
  // jobs) but expose the hidden universe one click away.
  const [showChildRaw, setShowChildRaw] = useUrlParam("children", "");
  const [showBatchRaw, setShowBatchRaw] = useUrlParam("batch", "");
  const showChild = showChildRaw === "1";
  const showBatch = showBatchRaw === "1";
  const setShowChild = (v: boolean) => setShowChildRaw(v ? "1" : null);
  const setShowBatch = (v: boolean) => setShowBatchRaw(v ? "1" : null);

  const [error, setError] = useState("");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Selection state. Held as a Set of job ids; pruned every render to
  // drop ids that have scrolled off the current filter.
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [cancelling, setCancelling] = useState(false);

  async function refresh(status = statusFilter, showLoader = false) {
    if (!showLoader && typeof document !== "undefined" && document.visibilityState === "hidden") return;
    if (showLoader) setLoading(true);
    try {
      setError("");
      const all = await listJobs({ limit: 500, status: status || undefined });
      setJobs(all);
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

  // Apply the filter chips client-side so the backend listJobs() query
  // shape stays unchanged.
  const visibleJobs = useMemo(
    () =>
      jobs.filter((j) => {
        if (!showChild && j.parent_job_id) return false;
        if (!showBatch && j.queue_name === BATCH_QUEUE) return false;
        return true;
      }),
    [jobs, showChild, showBatch],
  );

  // Prune the selection to ids that are still visible AND cancellable.
  // Otherwise the action bar would claim to cancel jobs the user can
  // no longer see (e.g. after toggling off a filter chip).
  useEffect(() => {
    setSelected((prev) => {
      if (prev.size === 0) return prev;
      const visibleCancellable = new Set(
        visibleJobs.filter((j) => CANCELLABLE_STATUSES.has(j.status)).map((j) => j.id),
      );
      let changed = false;
      const next = new Set<string>();
      for (const id of prev) {
        if (visibleCancellable.has(id)) next.add(id);
        else changed = true;
      }
      return changed ? next : prev;
    });
  }, [visibleJobs]);

  const cancellableVisible = useMemo(
    () => visibleJobs.filter((j) => CANCELLABLE_STATUSES.has(j.status)),
    [visibleJobs],
  );
  const allCancellableSelected =
    cancellableVisible.length > 0 && cancellableVisible.every((j) => selected.has(j.id));
  const someCancellableSelected =
    !allCancellableSelected && cancellableVisible.some((j) => selected.has(j.id));

  function toggleRow(id: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleAllVisibleCancellable() {
    if (allCancellableSelected) {
      // Drop only the visible cancellable ids; preserve any selections
      // outside the current filter (defensive, normally pruned above).
      setSelected((prev) => {
        const next = new Set(prev);
        for (const j of cancellableVisible) next.delete(j.id);
        return next;
      });
    } else {
      setSelected((prev) => {
        const next = new Set(prev);
        for (const j of cancellableVisible) next.add(j.id);
        return next;
      });
    }
  }

  async function onCancelSelected() {
    const ids = Array.from(selected);
    if (ids.length === 0) return;
    if (typeof window !== "undefined") {
      const confirmMsg = t("cancelConfirm", { count: ids.length });
      if (!window.confirm(confirmMsg)) return;
    }
    setCancelling(true);
    toast(t("cancelInProgress", { done: 0, total: ids.length }), "info");
    try {
      const { ok, failed } = await bulkCancelJobs(ids, (done, total, _lastId, lastError) => {
        // Surface per-error tail without aborting (toast queue caps at
        // ~3.5s lifetime; rapid bursts are fine for short batches).
        if (lastError) toast(`${_lastId.slice(0, 8)}…: ${lastError}`, "error");
        if (done < total && (done === Math.floor(total / 2) || done === total - 1)) {
          toast(t("cancelInProgress", { done, total }), "info");
        }
      });
      if (failed.length === 0) {
        toast(t("cancelDone", { ok: ok.length, total: ids.length }), "success");
      } else {
        toast(
          t("cancelDoneWithFailures", { ok: ok.length, total: ids.length, failed: failed.length }),
          failed.length === ids.length ? "error" : "info",
        );
      }
      setSelected(new Set());
      // Refresh so the new statuses (cancelled / cancellation-pending)
      // land in the table immediately.
      refresh(statusFilter);
    } finally {
      setCancelling(false);
    }
  }

  const activeCount = visibleJobs.filter((j) => j.status === "running" || j.status === "queued").length;
  const selectedCount = selected.size;

  const cancelDisabledReason = !isAuthed
    ? t("cancelSignInHint")
    : selectedCount === 0
      ? t("cancelOnlyActiveHint")
      : null;

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

      {/* Filter chips. Replaces the previous hard-coded
          `!parent_job_id && queue !== batch` exclusion (visible
          history was lying about coverage). Both default to off so the
          familiar view is unchanged unless the user opts in. */}
      <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
        <span className="text-slate-500">{t("status")}:</span>
        <FilterChip
          label={t("showChildJobs")}
          active={showChild}
          onClick={() => setShowChild(!showChild)}
        />
        <FilterChip
          label={t("showBatchJobs")}
          active={showBatch}
          onClick={() => setShowBatch(!showBatch)}
        />
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
        {!loading && visibleJobs.length === 0 && (
          <div className="rounded-lg border bg-white px-4 py-8 text-center text-sm text-slate-600 shadow-sm">{t("noJobsFound")}</div>
        )}
        {!loading && visibleJobs.map((j) => {
          const canSelect = CANCELLABLE_STATUSES.has(j.status);
          const isSelected = selected.has(j.id);
          return (
            <div key={j.id} className={`rounded-lg border bg-white p-3 shadow-sm transition-colors ${isSelected ? "border-blue-300 ring-1 ring-blue-200" : ""}`}>
              <div className="flex items-start gap-3">
                <div className="pt-0.5">
                  <input
                    type="checkbox"
                    aria-label={t("selectRow")}
                    checked={isSelected}
                    disabled={!canSelect}
                    onChange={() => toggleRow(j.id)}
                    className="h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500 disabled:opacity-30 disabled:cursor-not-allowed"
                  />
                </div>
                <Link href={`/${locale}/jobs/${j.id}`} className="flex-1 min-w-0 -mt-0.5 -mr-1 -mb-1 rounded-md p-1 hover:bg-blue-50">
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
                  <InlineProgress current={j.progress_current} total={j.progress_total} status={j.status} />
                  <p className="mt-1 font-mono text-xs text-slate-600 truncate">{j.id}</p>
                  {(j.parent_job_id || j.queue_name === BATCH_QUEUE) && (
                    <div className="mt-1 flex flex-wrap gap-1">
                      {j.parent_job_id && (
                        <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-[10px] text-slate-600">child</span>
                      )}
                      {j.queue_name === BATCH_QUEUE && (
                        <span className="inline-flex items-center rounded-full bg-violet-100 px-2 py-0.5 text-[10px] text-violet-700">batch</span>
                      )}
                    </div>
                  )}
                </Link>
              </div>
            </div>
          );
        })}
      </div>

      {/* Desktop table — checkbox column added in front. The header
          checkbox controls all currently-visible CANCELLABLE rows so it
          never makes a no-op promise about terminal rows. */}
      <div className="mt-4 hidden lg:block overflow-hidden rounded-lg border bg-white shadow-sm">
        <div className="grid grid-cols-[40px_140px_220px_1fr_180px] gap-2 border-b bg-slate-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-500">
          <div className="flex items-center">
            <input
              ref={(el) => {
                if (el) el.indeterminate = someCancellableSelected;
              }}
              type="checkbox"
              aria-label={t("selectAll")}
              checked={allCancellableSelected}
              disabled={cancellableVisible.length === 0}
              onChange={toggleAllVisibleCancellable}
              className="h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500 disabled:opacity-30 disabled:cursor-not-allowed"
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

        {!loading && visibleJobs.map((j) => {
          const canSelect = CANCELLABLE_STATUSES.has(j.status);
          const isSelected = selected.has(j.id);
          return (
            <div
              key={j.id}
              className={`grid grid-cols-[40px_140px_220px_1fr_180px] gap-2 border-b px-4 py-3 text-sm transition-colors last:border-0 items-start ${isSelected ? "bg-blue-50/60" : "hover:bg-blue-50"}`}
            >
              <div className="pt-0.5">
                <input
                  type="checkbox"
                  aria-label={t("selectRow")}
                  checked={isSelected}
                  disabled={!canSelect}
                  onChange={() => toggleRow(j.id)}
                  className="h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500 disabled:opacity-30 disabled:cursor-not-allowed"
                />
              </div>
              <Link href={`/${locale}/jobs/${j.id}`} className="contents">
                <div><StatusBadge status={j.status} /></div>
                <div>
                  <span className="text-slate-700 font-medium block truncate">{j.operation}</span>
                  {j.operation_description && (
                    <span className="text-xs text-slate-500 leading-snug line-clamp-2 block">{j.operation_description}</span>
                  )}
                  <InlineProgress current={j.progress_current} total={j.progress_total} status={j.status} />
                  {(j.parent_job_id || j.queue_name === BATCH_QUEUE) && (
                    <div className="mt-1 flex flex-wrap gap-1">
                      {j.parent_job_id && (
                        <span className="inline-flex items-center rounded-full bg-slate-100 px-1.5 py-0 text-[10px] text-slate-600">child</span>
                      )}
                      {j.queue_name === BATCH_QUEUE && (
                        <span className="inline-flex items-center rounded-full bg-violet-100 px-1.5 py-0 text-[10px] text-violet-700">batch</span>
                      )}
                    </div>
                  )}
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
          );
        })}

        {!loading && visibleJobs.length === 0 && (
          <div className="px-4 py-8 text-center text-sm text-slate-600">
            {t("noJobsFound")}
          </div>
        )}
      </div>

      <p className="mt-2 text-xs text-slate-600">
        {visibleJobs.length} job{visibleJobs.length !== 1 ? "s" : ""} shown
        {visibleJobs.length !== jobs.length && (
          <span className="text-slate-400"> ({jobs.length - visibleJobs.length} hidden by filter chips)</span>
        )}
      </p>

      {/* Sticky bulk action bar. Renders only when something is
          selected, so it never steals real estate. Cancel-only by
          design — see bulkCancelJobs comment in lib/api.ts for why
          there's no Retry button here yet. */}
      {selectedCount > 0 && (
        <div className="fixed inset-x-0 bottom-0 z-40 border-t border-slate-200 bg-white/95 px-4 py-3 shadow-[0_-4px_10px_rgba(15,23,42,0.06)] backdrop-blur supports-[backdrop-filter]:bg-white/80">
          <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-3">
            <span className="inline-flex items-center gap-2 rounded-full bg-blue-50 px-3 py-1 text-sm font-medium text-blue-800 border border-blue-100">
              <span className="inline-block h-1.5 w-1.5 rounded-full bg-blue-500" />
              {t("selectedCount", { count: selectedCount })}
            </span>
            <button
              type="button"
              onClick={() => setSelected(new Set())}
              className="text-sm text-slate-600 underline-offset-2 hover:underline"
            >
              {t("clearSelection")}
            </button>
            <div className="ml-auto flex items-center gap-3">
              {cancelDisabledReason && (
                <span className="text-xs text-slate-500">{cancelDisabledReason}</span>
              )}
              <button
                type="button"
                onClick={onCancelSelected}
                disabled={!isAuthed || cancelling || selectedCount === 0}
                aria-label={t("cancelSelectedAria", { count: selectedCount })}
                title={cancelDisabledReason ?? undefined}
                className="inline-flex items-center gap-2 rounded-md border border-red-200 bg-red-50 px-4 py-2 text-sm font-medium text-red-700 hover:bg-red-100 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {cancelling && (
                  <span className="inline-block h-3 w-3 animate-spin rounded-full border-2 border-red-300 border-t-red-700" />
                )}
                {t("cancelSelected")}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

function FilterChip({
  label, active, onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium transition-colors ${
        active
          ? "border-blue-200 bg-blue-50 text-blue-800"
          : "border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
      }`}
    >
      <span className={`inline-block h-1.5 w-1.5 rounded-full ${active ? "bg-blue-500" : "bg-slate-300"}`} />
      {label}
    </button>
  );
}
