"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import { useLocale, useTranslations } from "next-intl";
import { listJobs, baseUrl } from "@/lib/api";

/**
 * Compact, sticky-header status pill.
 *
 * Surfaces three pieces of operational information that previously lived
 * nowhere on the chrome:
 *   - count of running jobs (polled every 10s, visibility-aware)
 *   - reachability of the API (green dot = healthy, amber = degraded)
 *   - short build identifier (commit SHA, when the backend exposes one)
 *
 * Designed to take ~ 8rem on desktop and collapse to a dot+number on
 * narrow viewports. Clicking it routes to /jobs.
 *
 * The component is read-only and side-effect free; it never blocks the
 * UI when the API is unreachable.
 */

const POLL_INTERVAL = 10_000;

type Health = "healthy" | "degraded" | "unknown";

async function fetchHealth(): Promise<{ ok: boolean; sha: string | null }> {
  try {
    const res = await fetch(`${baseUrl()}/health`, { cache: "no-store" });
    if (!res.ok) return { ok: false, sha: null };
    let sha: string | null = null;
    try {
      const json = await res.json();
      // The PROTEA /health endpoint returns a string map. We accept any
      // of the common shapes without crashing if absent.
      sha =
        (typeof json?.sha === "string" && json.sha) ||
        (typeof json?.commit === "string" && json.commit) ||
        (typeof json?.version === "string" && json.version) ||
        null;
      if (sha) sha = sha.slice(0, 7);
    } catch {
      sha = null;
    }
    return { ok: true, sha };
  } catch {
    return { ok: false, sha: null };
  }
}

export function SystemStatusPill() {
  const t = useTranslations("nav");
  const locale = useLocale();
  const [running, setRunning] = useState<number>(0);
  const [health, setHealth] = useState<Health>("unknown");
  const [sha, setSha] = useState<string | null>(null);

  const poll = useCallback(async () => {
    if (typeof document !== "undefined" && document.visibilityState === "hidden") return;
    try {
      const jobs = await listJobs({ limit: 50, status: "running" });
      setRunning(jobs.length);
      setHealth("healthy");
    } catch {
      setHealth("degraded");
    }
    const h = await fetchHealth();
    setHealth(h.ok ? "healthy" : "degraded");
    if (h.sha) setSha(h.sha);
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

  const isRunning = running > 0;
  const isDegraded = health === "degraded";

  // Color and label resolve from health first, then queue state.
  const dotColor = isDegraded
    ? "bg-amber-500"
    : isRunning
      ? "bg-blue-500"
      : "bg-emerald-500";
  const ringColor = isDegraded
    ? "ring-amber-200"
    : isRunning
      ? "ring-blue-200"
      : "ring-emerald-200";
  const label = isDegraded
    ? t("statusDegraded")
    : isRunning
      ? t("statusRunning", { count: running })
      : t("statusIdle");

  return (
    <Link
      href={`/${locale}/jobs`}
      title={sha ? `${label} · build ${sha}` : label}
      className="group hidden md:inline-flex items-center gap-2 h-9 rounded-full bg-slate-50/80 hover:bg-slate-100 ring-1 ring-inset ring-slate-200 px-2.5 text-[12px] font-medium text-slate-700 transition-colors"
      aria-label={label}
    >
      <span className="relative flex h-2 w-2 shrink-0">
        {isRunning && !isDegraded && (
          <span className={`absolute inline-flex h-full w-full rounded-full ${dotColor} opacity-60 animate-ping`} />
        )}
        <span className={`relative inline-flex h-2 w-2 rounded-full ${dotColor} ring-2 ${ringColor}`} />
      </span>
      <span className="tabular-nums whitespace-nowrap">{label}</span>
      {sha && (
        <span
          className="hidden xl:inline-flex font-mono text-[10px] text-slate-500 border-l border-slate-200 pl-2 ml-0.5"
          aria-label={t("statusBuild")}
        >
          {sha}
        </span>
      )}
    </Link>
  );
}
