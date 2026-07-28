"use client";

// FARM-UI.3 sticky-header pill for the agent-farm scope. Mirrors the
// shape of SystemStatusPill but exposes farm-specific signals from the
// FastAPI sidecar (apps/farm-api, FARM-UI.1):
//
//   - health dot color:
//       red    : at least one task in the last hour ended in a failure
//                state (failed / killed / crashed)
//       amber  : sidecar reachable but no failures and zero running
//                tasks AND the sidecar reports degraded; or warning
//                heartbeats (future: we approximate via degraded fetch)
//       green  : sidecar reachable, no failures in the last hour
//       slate  : sidecar unreachable (unknown)
//   - running count: number of tasks with status=running
//   - recent error count: failure-state tasks in the last hour
//
// The component never blocks the chrome when the sidecar is offline; it
// degrades silently to the unknown state and keeps polling every 10s.

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { useLocale, useTranslations } from "next-intl";
import { listFarmTasks, type FarmTask } from "@/lib/farmApi";

const POLL_INTERVAL = 10_000;
const ERROR_WINDOW_MS = 60 * 60 * 1000;
const ERROR_STATES = new Set(["failed", "killed", "crashed"]);

type FarmHealth = "healthy" | "warning" | "error" | "unknown";

function deriveHealth(
  running: FarmTask[],
  recentErrors: FarmTask[],
  reachable: boolean,
): FarmHealth {
  if (!reachable) return "unknown";
  if (recentErrors.length > 0) return "error";
  // Reserved for a future heartbeats-warning surface: today the sidecar
  // does not expose a /heartbeats?level=warning aggregator so we only
  // promote to amber when the queue is empty AND the sidecar is
  // reachable but reports no rows of any kind. That state is rare and
  // not actionable on its own; keep at healthy for now and let the
  // running count carry the signal.
  void running;
  return "healthy";
}

export function FarmStatusPill() {
  const t = useTranslations("farm.pill");
  const locale = useLocale();
  const [running, setRunning] = useState<FarmTask[]>([]);
  const [recentErrors, setRecentErrors] = useState<FarmTask[]>([]);
  const [reachable, setReachable] = useState<boolean>(false);

  const poll = useCallback(async () => {
    if (typeof document !== "undefined" && document.visibilityState === "hidden") return;
    try {
      const [runningRows, recentRows] = await Promise.all([
        listFarmTasks({ status: "running", limit: 50 }),
        listFarmTasks({ since: "1h", limit: 100 }),
      ]);
      setRunning(runningRows);
      const cutoff = Date.now() - ERROR_WINDOW_MS;
      const errors = recentRows.filter((row) => {
        if (!ERROR_STATES.has(row.status)) return false;
        const endedRaw = row.ended_at ?? row.started_at ?? row.created_at;
        if (!endedRaw) return false;
        const ended = new Date(endedRaw).getTime();
        return Number.isFinite(ended) && ended >= cutoff;
      });
      setRecentErrors(errors);
      setReachable(true);
    } catch {
      setReachable(false);
    }
  }, []);

  useEffect(() => {
    poll();
    const id = setInterval(poll, POLL_INTERVAL);
    const onVisibility = () => {
      if (document.visibilityState === "visible") poll();
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      clearInterval(id);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [poll]);

  const health = deriveHealth(running, recentErrors, reachable);

  const dotColor =
    health === "error"
      ? "bg-rose-500"
      : health === "warning"
        ? "bg-amber-500"
        : health === "healthy"
          ? "bg-emerald-500"
          : "bg-slate-400";
  const ringColor =
    health === "error"
      ? "ring-rose-200"
      : health === "warning"
        ? "ring-amber-200"
        : health === "healthy"
          ? "ring-emerald-200"
          : "ring-slate-200";
  const label =
    health === "error"
      ? t("error", { count: recentErrors.length })
      : health === "unknown"
        ? t("unknown")
        : t("running", { count: running.length });
  const showPulse = running.length > 0 && health !== "unknown";

  return (
    <Link
      href={`/${locale}/instrument/farm`}
      title={
        reachable
          ? `${running.length} running · ${recentErrors.length} errors (1h)`
          : "Farm sidecar unreachable"
      }
      className="group inline-flex items-center gap-2 h-9 rounded-full bg-slate-50/80 hover:bg-slate-100 ring-1 ring-inset ring-slate-200 px-2.5 text-[12px] font-medium text-slate-700 transition-colors"
      aria-label={label}
      data-testid="farm-status-pill"
    >
      <span className="relative flex h-2 w-2 shrink-0">
        {showPulse && (
          <span
            className={`absolute inline-flex h-full w-full rounded-full ${dotColor} opacity-60 animate-ping`}
          />
        )}
        <span
          className={`relative inline-flex h-2 w-2 rounded-full ${dotColor} ring-2 ${ringColor}`}
          data-testid="farm-status-dot"
          data-health={health}
        />
      </span>
      <span className="tabular-nums whitespace-nowrap">{label}</span>
      {recentErrors.length > 0 && (
        <span
          className="hidden xl:inline-flex font-mono text-[10px] text-rose-600 border-l border-slate-200 pl-2 ml-0.5"
          aria-label={t("recentErrorsAria", { count: recentErrors.length })}
          data-testid="farm-status-error-count"
        >
          {recentErrors.length} err
        </span>
      )}
    </Link>
  );
}
