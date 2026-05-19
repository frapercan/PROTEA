"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import { listJobs, type Job } from "@/lib/api";

const POLL_INTERVAL = 10_000;

export function FloatingJobsWidget() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [expanded, setExpanded] = useState(false);

  const poll = useCallback(async () => {
    if (document.visibilityState === "hidden") return;
    try {
      const running = await listJobs({ limit: 50, status: "running" });
      setJobs(running);
    } catch {
      // ignore transient errors
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

  if (jobs.length === 0) return null;

  return (
    <div className="fixed bottom-5 right-5 sm:bottom-6 sm:right-6 z-50">
      {expanded && (
        <div className="mb-3 w-80 max-w-[calc(100vw-2.5rem)] rounded-2xl border border-slate-200 bg-white shadow-2xl overflow-hidden">
          <div className="flex items-center justify-between bg-gradient-to-r from-slate-50 to-white px-4 py-3 border-b border-slate-100">
            <div className="flex items-center gap-2">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-60" />
                <span className="relative inline-flex h-2 w-2 rounded-full bg-blue-600" />
              </span>
              <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-600">
                Running Jobs
              </span>
              <span className="rounded-full bg-blue-100 px-2 py-0.5 text-[11px] font-bold text-blue-700 tabular-nums">
                {jobs.length}
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
          <div className="divide-y divide-slate-100 max-h-72 overflow-y-auto">
            {jobs.map((job) => {
              const pct = job.progress_total && job.progress_current
                ? Math.round((job.progress_current / job.progress_total) * 100)
                : null;
              return (
                <Link
                  key={job.id}
                  href={`/jobs/${job.id}`}
                  className="block px-4 py-3 hover:bg-slate-50 transition-colors"
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-[13px] font-medium text-slate-800 truncate">
                      {job.operation}
                    </span>
                    {pct != null && (
                      <span className="rounded-md bg-blue-50 px-1.5 py-0.5 text-[11px] font-semibold text-blue-700 tabular-nums shrink-0">
                        {pct}%
                      </span>
                    )}
                  </div>
                  {pct != null && (
                    <div className="mt-2 h-1.5 rounded-full bg-slate-100 overflow-hidden">
                      <div
                        className="h-full rounded-full bg-gradient-to-r from-blue-500 to-indigo-500 transition-all"
                        style={{ width: `${pct}%` }}
                      />
                    </div>
                  )}
                  <div className="text-[11px] text-slate-600 mt-1 font-mono truncate">
                    {job.id.slice(0, 8)}…
                  </div>
                </Link>
              );
            })}
          </div>
        </div>
      )}

      <button
        onClick={() => setExpanded((v) => !v)}
        className="flex items-center gap-2.5 rounded-full bg-gradient-to-r from-blue-600 to-indigo-600 text-white pl-3.5 pr-4 py-2.5 text-sm font-semibold shadow-lg shadow-blue-600/30 hover:shadow-xl hover:shadow-blue-600/40 hover:-translate-y-0.5 transition-all"
        aria-expanded={expanded}
      >
        <span className="relative flex h-2.5 w-2.5" aria-hidden="true">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-white opacity-70" />
          <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-white" />
        </span>
        <span className="tabular-nums">{jobs.length}</span>
        <span className="opacity-90">running</span>
      </button>
    </div>
  );
}
