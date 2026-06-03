"use client";

// FARM-UI.3 floating widget mirroring FloatingJobsWidget but bound to the
// agent-farm FastAPI sidecar (apps/farm-api, FARM-UI.1). The component
// polls /tasks?status=running every 10s, pauses when the document is
// hidden, and surfaces the count + a per-row drilldown to /farm/<task_id>.
//
// The shape is deliberately kept identical to FloatingJobsWidget (button
// + expandable panel, fixed bottom-right) so the farm scope feels like
// a sibling of the platform's normal job queue, not a third paradigm.
//
// Visibility on the page is controlled by the locale layout: the widget
// is only mounted inside /[locale]/farm/* routes so the farm pill never
// leaks into the rest of the chrome.

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { useLocale } from "next-intl";
import { listFarmTasks, type FarmTask } from "@/lib/farmApi";

const POLL_INTERVAL = 10_000;

export function FarmJobsWidget() {
  const locale = useLocale();
  const [tasks, setTasks] = useState<FarmTask[]>([]);
  const [expanded, setExpanded] = useState(false);

  const poll = useCallback(async () => {
    if (typeof document !== "undefined" && document.visibilityState === "hidden") return;
    try {
      const running = await listFarmTasks({ status: "running", limit: 50 });
      setTasks(running);
    } catch {
      // Sidecar transient errors are ignored so the widget never blocks
      // the chrome. The status pill exposes the degraded state.
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

  if (tasks.length === 0) return null;

  // Stack above the platform-wide FloatingJobsWidget when both are
  // mounted: the regular widget sits at bottom-5; we offset upward by
  // ~3.5rem so the two pill buttons never overlap when running jobs +
  // running farm tasks coexist on a /farm/* route.
  return (
    <div
      className="fixed bottom-20 right-5 sm:bottom-[5.5rem] sm:right-6 z-50"
      data-testid="farm-jobs-widget"
    >
      {expanded && (
        <div className="mb-3 w-80 max-w-[calc(100vw-2.5rem)] rounded-2xl border border-slate-200 bg-white shadow-2xl overflow-hidden">
          <div className="flex items-center justify-between bg-gradient-to-r from-violet-50 to-white px-4 py-3 border-b border-slate-100">
            <div className="flex items-center gap-2">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-violet-400 opacity-60" />
                <span className="relative inline-flex h-2 w-2 rounded-full bg-violet-600" />
              </span>
              <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-600">
                Farm tasks
              </span>
              <span className="rounded-full bg-violet-100 px-2 py-0.5 text-[11px] font-bold text-violet-700 tabular-nums">
                {tasks.length}
              </span>
            </div>
            <button
              onClick={() => setExpanded(false)}
              className="flex h-7 w-7 items-center justify-center rounded-md text-slate-600 hover:bg-slate-100 hover:text-slate-700 transition-colors"
              aria-label="Close"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 16 16" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                <path d="M4 4l8 8M12 4l-8 8" />
              </svg>
            </button>
          </div>
          <div className="divide-y divide-slate-100 max-h-72 overflow-y-auto" data-testid="farm-jobs-widget-list">
            {tasks.map((task) => (
              <Link
                key={task.id}
                href={`/${locale}/farm/${task.id}`}
                className="block px-4 py-3 hover:bg-slate-50 transition-colors"
                data-testid="farm-jobs-widget-row"
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="text-[13px] font-medium text-slate-800 truncate">
                    {task.agent_name}
                  </span>
                  {task.model && (
                    <span className="rounded-md bg-violet-50 px-1.5 py-0.5 text-[11px] font-semibold text-violet-700 tabular-nums shrink-0">
                      {task.model.replace(/^claude-/, "")}
                    </span>
                  )}
                </div>
                <div className="text-[11px] text-slate-600 mt-1 font-mono truncate">
                  {task.id}
                </div>
              </Link>
            ))}
          </div>
        </div>
      )}

      <button
        onClick={() => setExpanded((v) => !v)}
        className="flex items-center gap-2.5 rounded-full bg-gradient-to-r from-violet-600 to-fuchsia-600 text-white pl-3.5 pr-4 py-2.5 text-sm font-semibold shadow-lg shadow-violet-600/30 hover:shadow-xl hover:shadow-violet-600/40 hover:-translate-y-0.5 transition-all"
        aria-expanded={expanded}
        aria-label={`${tasks.length} running farm tasks`}
        data-testid="farm-jobs-widget-toggle"
      >
        <span className="relative flex h-2.5 w-2.5">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-white opacity-70" />
          <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-white" />
        </span>
        <span className="tabular-nums">{tasks.length}</span>
        <span className="opacity-90">farm</span>
      </button>
    </div>
  );
}
