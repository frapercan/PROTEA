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
      const running = await listJobs({ limit: 5, status: "running" });
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
    <div className="fixed bottom-4 right-4 z-50">
      {expanded && (
        <div className="mb-2 w-72 rounded-lg border bg-white shadow-xl overflow-hidden">
          <div className="bg-gray-50 px-3 py-2 text-xs font-semibold text-gray-600 uppercase tracking-wide flex items-center justify-between">
            <span>Running Jobs</span>
            <button onClick={() => setExpanded(false)} className="text-gray-400 hover:text-gray-600 text-sm">
              &times;
            </button>
          </div>
          <div className="divide-y max-h-60 overflow-y-auto">
            {jobs.map((job) => {
              const pct = job.progress_total && job.progress_current
                ? Math.round((job.progress_current / job.progress_total) * 100)
                : null;
              return (
                <Link
                  key={job.id}
                  href={`/jobs/${job.id}`}
                  className="block px-3 py-2.5 hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-medium text-gray-800 truncate">
                      {job.operation}
                    </span>
                    {pct != null && (
                      <span className="text-[10px] text-gray-400 tabular-nums ml-2">{pct}%</span>
                    )}
                  </div>
                  {pct != null && (
                    <div className="mt-1 h-1 rounded-full bg-gray-100 overflow-hidden">
                      <div
                        className="h-full rounded-full bg-blue-500 transition-all"
                        style={{ width: `${pct}%` }}
                      />
                    </div>
                  )}
                  <div className="text-[10px] text-gray-400 mt-0.5 font-mono truncate">
                    {job.id.slice(0, 8)}...
                  </div>
                </Link>
              );
            })}
          </div>
        </div>
      )}

      <button
        onClick={() => setExpanded((v) => !v)}
        className="flex items-center gap-1.5 rounded-full bg-blue-600 text-white px-3.5 py-2 text-sm font-medium shadow-lg hover:bg-blue-700 transition-colors"
      >
        <span className="relative flex h-2 w-2">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-300 opacity-75" />
          <span className="relative inline-flex rounded-full h-2 w-2 bg-white" />
        </span>
        {jobs.length} running
      </button>
    </div>
  );
}
