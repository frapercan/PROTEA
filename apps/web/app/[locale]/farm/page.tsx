"use client";

// FARM-UI.2 list view. Renders the agent-farm task catalog (sourced
// from the farm-api FastAPI sidecar, FARM-UI.1) with three filter
// surfaces:
//   - status: multi-select chip strip (queued / running / succeeded /
//     failed / cancelled / killed / crashed). Multi-select would
//     require client-side intersection across calls, which the sidecar
//     does not support yet; v1 treats the chip strip as exclusive
//     single-select to keep the network round-trips honest. The chip
//     UI hints at the future multi-select via a leading "all" chip.
//   - agent: free-text input (a multi-select chip strip would need a
//     /agents endpoint that the sidecar does not yet expose; the input
//     forwards to /tasks?agent=... which is an exact match).
//   - time window: 24h / 7d radio.
// Filter state is round-tripped through the URL via useUrlParam so the
// dashboard is bookmarkable.
//
// The whole page is a client component because all three filter chips
// need useUrlParam, which depends on Next's client-side router. The
// jobs page is a client component for the same reason; we follow that
// convention.

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { listFarmTasks, FarmTask } from "@/lib/farmApi";
import { StatusBadge } from "@/components/StatusBadge";
import { SkeletonTableRow } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";
import { useTranslations } from "next-intl";
import { useUrlParam } from "@/lib/useUrlParam";

const STATUS_CHIPS = [
  "queued",
  "running",
  "succeeded",
  "failed",
  "cancelled",
  "killed",
  "crashed",
] as const;

const WINDOWS = [
  { key: "24h", since: "24h" },
  { key: "7d", since: "7d" },
] as const;

function formatRelative(iso?: string | null) {
  if (!iso) return "-";
  try {
    const ts = new Date(iso).getTime();
    const diff = Date.now() - ts;
    if (diff < 60_000) return `${Math.max(0, Math.floor(diff / 1000))}s`;
    if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m`;
    if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h`;
    return `${Math.floor(diff / 86_400_000)}d`;
  } catch {
    return iso;
  }
}

function formatDuration(start?: string | null, end?: string | null): string {
  if (!start) return "-";
  const s = new Date(start).getTime();
  const e = end ? new Date(end).getTime() : Date.now();
  const sec = Math.max(0, Math.floor((e - s) / 1000));
  if (sec < 60) return `${sec}s`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ${sec % 60}s`;
  return `${Math.floor(sec / 3600)}h ${Math.floor((sec % 3600) / 60)}m`;
}

export default function FarmListPage() {
  const t = useTranslations("farm");
  const toast = useToast();
  const [tasks, setTasks] = useState<FarmTask[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [statusFilterRaw, setStatusFilterRaw] = useUrlParam("status", "");
  const statusFilter = statusFilterRaw ?? "";
  const setStatusFilter = (v: string) => setStatusFilterRaw(v === "" ? null : v);

  const [agentFilterRaw, setAgentFilterRaw] = useUrlParam("agent", "");
  const agentFilter = agentFilterRaw ?? "";
  const setAgentFilter = (v: string) => setAgentFilterRaw(v === "" ? null : v);

  const [windowRaw, setWindowRaw] = useUrlParam("window", "24h");
  const windowKey = (windowRaw ?? "24h") as (typeof WINDOWS)[number]["key"];
  const setWindow = (v: string) => setWindowRaw(v === "24h" ? null : v);

  const sinceParam = useMemo(
    () => WINDOWS.find((w) => w.key === windowKey)?.since ?? "24h",
    [windowKey],
  );

  async function refresh() {
    setLoading(true);
    try {
      setError("");
      const rows = await listFarmTasks({
        status: statusFilter || undefined,
        agent: agentFilter || undefined,
        since: sinceParam,
        limit: 500,
      });
      setTasks(rows);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      toast(msg, "error");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statusFilter, agentFilter, sinceParam]);

  return (
    <>
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-xl font-semibold">{t("title")}</h1>
        <span className="text-xs text-slate-600">{t("subtitle")}</span>
        <button
          onClick={refresh}
          className="ml-auto rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-slate-50"
        >
          {t("refresh")}
        </button>
      </div>

      {/* Filter row */}
      <div className="mt-4 space-y-3 rounded-lg border bg-white p-3 shadow-sm">
        {/* Status chips */}
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-xs font-medium text-slate-600 mr-1">
            {t("filters.statusLabel")}:
          </span>
          <button
            onClick={() => setStatusFilter("")}
            className={`inline-flex items-center min-h-[44px] sm:min-h-0 rounded-full border px-3 py-1.5 sm:py-0.5 text-sm sm:text-xs font-medium transition-colors ${
              statusFilter === ""
                ? "bg-slate-800 text-white border-slate-800"
                : "bg-white text-slate-600 border-slate-200 hover:bg-slate-50"
            }`}
          >
            {t("filters.statusAll")}
          </button>
          {STATUS_CHIPS.map((s) => (
            <button
              key={s}
              onClick={() => setStatusFilter(s)}
              className={`inline-flex items-center min-h-[44px] sm:min-h-0 rounded-full border px-3 py-1.5 sm:py-0.5 text-sm sm:text-xs font-medium transition-colors ${
                statusFilter === s
                  ? "bg-blue-600 text-white border-blue-600"
                  : "bg-white text-slate-600 border-slate-200 hover:bg-slate-50"
              }`}
              aria-pressed={statusFilter === s}
              data-testid={`farm-status-chip-${s}`}
            >
              {t(`status.${s}`)}
            </button>
          ))}
        </div>

        {/* Agent input + window radio */}
        <div className="flex flex-wrap items-center gap-3">
          <label className="flex items-center gap-2 text-xs text-slate-600">
            {t("filters.agentLabel")}:
            <input
              type="text"
              value={agentFilter}
              onChange={(e) => setAgentFilter(e.target.value)}
              placeholder={t("filters.agentPlaceholder")}
              className="rounded-md border bg-white px-2.5 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </label>
          <div className="flex items-center gap-1.5">
            <span className="text-xs font-medium text-slate-600 mr-1">
              {t("filters.windowLabel")}:
            </span>
            {WINDOWS.map((w) => (
              <button
                key={w.key}
                onClick={() => setWindow(w.key)}
                className={`inline-flex items-center min-h-[44px] sm:min-h-0 rounded-full border px-3 py-1.5 sm:py-0.5 text-sm sm:text-xs font-medium transition-colors ${
                  windowKey === w.key
                    ? "bg-blue-600 text-white border-blue-600"
                    : "bg-white text-slate-600 border-slate-200 hover:bg-slate-50"
                }`}
                aria-pressed={windowKey === w.key}
              >
                {t(`windows.${w.key}`)}
              </button>
            ))}
          </div>
        </div>
      </div>

      {error && (
        <pre
          data-testid="farm-error"
          className="mt-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700"
        >
          {error}
        </pre>
      )}

      {/* Mobile card list. P1.7 reserve min-height matching ~5 skeleton
          rows so async data load does not shift the page (CLS). The
          desktop table reserves space via its own skeleton table-row
          structure. */}
      <div
        className="mt-4 lg:hidden space-y-2 min-h-[420px]"
        data-testid="farm-list-mobile"
      >
        {loading &&
          Array.from({ length: 5 }).map((_, i) => (
            <div
              key={i}
              className="rounded-lg border bg-white p-3 shadow-sm animate-pulse space-y-2"
            >
              <div className="h-4 bg-slate-200 rounded w-32" />
              <div className="h-3 bg-slate-100 rounded w-48" />
            </div>
          ))}
        {!loading && tasks.length === 0 && (
          <div className="rounded-lg border bg-white px-4 py-8 text-center text-sm text-slate-600 shadow-sm">
            {t("emptyState")}
          </div>
        )}
        {!loading &&
          tasks.map((task) => (
            <Link
              key={task.id}
              href={`/farm/${task.id}`}
              className="block rounded-lg border bg-white p-3 shadow-sm hover:border-blue-200 hover:bg-blue-50 transition-colors"
              data-testid="farm-row"
            >
              <div className="flex items-start justify-between gap-2">
                <StatusBadge status={task.status} />
                <span className="text-xs text-slate-600">
                  {formatRelative(task.started_at ?? task.created_at)}
                </span>
              </div>
              <p className="mt-1.5 text-sm font-medium text-slate-800 truncate">
                {task.id}
              </p>
              <p className="text-xs text-slate-600">
                {task.agent_name}
                {task.model ? ` · ${task.model}` : ""}
              </p>
              <p className="mt-0.5 text-xs text-slate-600">
                {t("table.duration")}:{" "}
                {formatDuration(task.started_at, task.ended_at)}
              </p>
            </Link>
          ))}
      </div>

      {/* Desktop table. P1.7 reserve min-height to hold header + ~5
          rows so skeleton-to-data swap does not shift content below. */}
      <div
        className="mt-4 hidden lg:block overflow-hidden rounded-lg border bg-white shadow-sm min-h-[320px]"
        data-testid="farm-list-desktop"
      >
        <div className="grid grid-cols-[1fr_180px_140px_140px_140px_180px] gap-2 border-b bg-slate-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-600">
          <div>{t("table.taskId")}</div>
          <div>{t("table.agent")}</div>
          <div>{t("table.status")}</div>
          <div>{t("table.started")}</div>
          <div>{t("table.duration")}</div>
          <div>{t("table.model")}</div>
        </div>

        {loading &&
          Array.from({ length: 5 }).map((_, i) => (
            <SkeletonTableRow key={i} cols={6} />
          ))}

        {!loading &&
          tasks.map((task) => (
            <Link
              key={task.id}
              href={`/farm/${task.id}`}
              className="grid grid-cols-[1fr_180px_140px_140px_140px_180px] gap-2 border-b px-4 py-3 text-sm hover:bg-blue-50 transition-colors last:border-0 items-center"
              data-testid="farm-row"
            >
              <div className="font-mono text-xs text-slate-700 truncate">
                {task.id}
              </div>
              <div className="text-slate-700 truncate">{task.agent_name}</div>
              <div>
                <StatusBadge status={task.status} />
              </div>
              <div className="text-slate-600 text-xs">
                {formatRelative(task.started_at ?? task.created_at)}
              </div>
              <div className="text-slate-600 text-xs">
                {formatDuration(task.started_at, task.ended_at)}
              </div>
              <div className="text-slate-600 text-xs truncate">
                {task.model ?? "-"}
              </div>
            </Link>
          ))}

        {!loading && tasks.length === 0 && (
          <div className="px-4 py-8 text-center text-sm text-slate-600">
            {t("emptyState")}
          </div>
        )}
      </div>

      <p className="mt-2 text-xs text-slate-600">
        {t("rowCount", { count: tasks.length })}
      </p>
    </>
  );
}
