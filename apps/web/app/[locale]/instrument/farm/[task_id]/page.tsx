"use client";

// FARM-UI.2 detail view. One page per agent-farm task, sourced from the
// farm-api sidecar. Surfaces the four bits the operator usually wants:
//   1. Status + identity (badge, agent name, kind, worktree path).
//   2. Diff range (sha_before / sha_after) so the operator can jump to
//      GitHub for review when the task touched a known repo.
//   3. Last 50 heartbeats via the shared EventTimeline component. The
//      heartbeat row shape (id, ts, level, message) is adapted to
//      EventTimeline's JobEvent shape (event = "heartbeat", fields =
//      empty) so we get the same chronological column for free.
//   4. results.summary (rendered as preformatted text; the sidecar
//      stores arbitrary executor markdown that we do not try to
//      sanitise client-side).

import Link from "next/link";
import { use, useEffect, useState } from "react";
import {
  getFarmHeartbeats,
  getFarmResults,
  getFarmTask,
  FarmHeartbeat,
  FarmResult,
  FarmTask,
} from "@/lib/farmApi";
import type { JobEvent } from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";
import { EventTimeline } from "@/components/EventTimeline";
import { Skeleton } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";
import { useLocale, useTranslations } from "next-intl";
import { useHasRole } from "@/lib/useRole";

function formatDate(iso?: string | null) {
  if (!iso) return "-";
  try {
    return new Date(iso).toLocaleString([], {
      dateStyle: "short",
      timeStyle: "medium",
    });
  } catch {
    return iso;
  }
}

function shortSha(sha?: string | null): string {
  if (!sha) return "-";
  return sha.length >= 7 ? sha.slice(0, 7) : sha;
}

// Heuristic owner/repo extractor from worktree_owner_repo or the
// worktree path. The sidecar's Task model carries the owner_repo column
// directly, so this only acts as a fallback for older rows.
function ghCommitUrl(ownerRepo: string | null | undefined, sha: string | null | undefined): string | null {
  if (!ownerRepo || !sha) return null;
  if (!/^[a-z0-9.-]+\/[a-z0-9._-]+$/i.test(ownerRepo)) return null;
  return `https://github.com/${ownerRepo}/commit/${sha}`;
}

function heartbeatToEvent(hb: FarmHeartbeat): JobEvent {
  // Use a synthetic "heartbeat" event name so EventTimeline's monospace
  // column is consistent and the operator can scan the level column
  // (info/warning/error) at the same eye position used by the jobs
  // page. The level field on heartbeats is a free-text string; we
  // narrow to the three EventTimeline-supported levels.
  const lvl = ["info", "warning", "error"].includes(hb.level)
    ? (hb.level as JobEvent["level"])
    : "info";
  return {
    id: hb.id,
    ts: hb.ts,
    level: lvl,
    event: "heartbeat",
    message: hb.message,
    fields: {},
  };
}

export default function FarmTaskDetail({
  params,
}: {
  params: Promise<{ task_id: string }>;
}) {
  const { task_id: taskId } = use(params);
  const t = useTranslations("farm");
  const locale = useLocale();
  const toast = useToast();
  // UX-ADMIN-AUDIT P0-FRM-1: spawn_args may contain prompt content or
  // file paths; only admins see the raw payload, others get a redacted
  // placeholder. The auth gate at FarmLayout already blocks anonymous
  // visitors entirely, so the role check here only differentiates
  // signed-in viewer/operator vs admin.
  const isAdmin = useHasRole("admin");
  const [task, setTask] = useState<FarmTask | null>(null);
  const [heartbeats, setHeartbeats] = useState<FarmHeartbeat[]>([]);
  const [result, setResult] = useState<FarmResult | null>(null);
  const [resultMissing, setResultMissing] = useState(false);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  async function refresh() {
    setLoading(true);
    setError("");
    try {
      const [taskRow, hbs] = await Promise.all([
        getFarmTask(taskId),
        getFarmHeartbeats(taskId, 50),
      ]);
      setTask(taskRow);
      // Heartbeats arrive newest-first from the sidecar; flip to
      // chronological order so EventTimeline reads top-down like a log.
      setHeartbeats([...hbs].reverse());

      // results may legitimately be 404 if the task never reached
      // finalize. Catch only that case; bubble other errors up.
      try {
        const res = await getFarmResults(taskId);
        setResult(res);
        setResultMissing(false);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes("no results row") || msg.includes("404")) {
          setResult(null);
          setResultMissing(true);
        } else {
          throw e;
        }
      }
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
  }, [taskId]);

  const ghBefore = ghCommitUrl(task?.worktree_owner_repo, task?.spawn_args ? null : null) ?? null;
  void ghBefore;
  const ghBeforeUrl = result
    ? ghCommitUrl(task?.worktree_owner_repo, result.sha_before)
    : null;
  const ghAfterUrl = result
    ? ghCommitUrl(task?.worktree_owner_repo, result.sha_after)
    : null;

  return (
    <div>
      <div className="flex flex-wrap items-center gap-3">
        <Link
          href={`/${locale}/instrument/farm`}
          className="text-sm text-blue-600 hover:underline"
          data-testid="farm-detail-back-link"
        >
          {t("detail.backLink")}
        </Link>
        <h1 className="text-xl font-semibold">{t("detail.title")}</h1>
        <button
          onClick={refresh}
          className="ml-auto rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-slate-50"
        >
          {t("refresh")}
        </button>
      </div>

      {error && (
        <pre
          data-testid="farm-detail-error"
          className="mt-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700"
        >
          {error}
        </pre>
      )}

      {loading && !task && (
        <div className="mt-4 rounded-lg border bg-white p-4 shadow-sm space-y-3">
          <div className="flex items-center gap-3">
            <Skeleton className="h-5 w-20" />
            <Skeleton className="h-4 w-40" />
            <Skeleton className="h-3 w-60" />
          </div>
          <Skeleton className="h-3 w-3/4" />
          <Skeleton className="h-3 w-1/2" />
        </div>
      )}

      {task && (
        <div
          className="mt-4 rounded-lg border bg-white p-4 shadow-sm space-y-3"
          data-testid="farm-task-card"
        >
          <div className="flex flex-wrap items-center gap-3">
            <StatusBadge status={task.status} />
            <span className="font-mono text-xs text-slate-600">{task.id}</span>
            <span className="text-sm text-slate-600">
              {task.agent_name} · {task.kind}
            </span>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-1 text-sm">
            <div>
              <span className="text-slate-600">{t("detail.created")}:</span>{" "}
              {formatDate(task.created_at)}
            </div>
            <div>
              <span className="text-slate-600">{t("detail.started")}:</span>{" "}
              {formatDate(task.started_at)}
            </div>
            <div>
              <span className="text-slate-600">{t("detail.ended")}:</span>{" "}
              {formatDate(task.ended_at)}
            </div>
            <div>
              <span className="text-slate-600">{t("detail.model")}:</span>{" "}
              {task.model ?? "-"}
            </div>
            {task.exit_code != null && (
              <div>
                <span className="text-slate-600">{t("detail.exitCode")}:</span>{" "}
                <span className="font-mono">{task.exit_code}</span>
              </div>
            )}
            {task.pid != null && (
              <div>
                <span className="text-slate-600">{t("detail.pid")}:</span>{" "}
                <span className="font-mono">{task.pid}</span>
              </div>
            )}
            {task.worktree && (
              <div className="md:col-span-2">
                <span className="text-slate-600">{t("detail.worktree")}:</span>{" "}
                <span className="font-mono text-xs text-slate-700 break-all">
                  {task.worktree}
                </span>
              </div>
            )}
            {task.worktree_owner_repo && (
              <div className="md:col-span-2">
                <span className="text-slate-600">{t("detail.ownerRepo")}:</span>{" "}
                <span className="font-mono text-xs text-slate-700">
                  {task.worktree_owner_repo}
                </span>
              </div>
            )}
          </div>

          {task.spawn_args && (
            isAdmin ? (
              <details className="text-sm" data-testid="farm-spawn-args">
                <summary className="cursor-pointer text-slate-600 hover:text-slate-700">
                  {t("detail.spawnArgsAdmin")}
                </summary>
                <pre className="mt-1 rounded bg-slate-50 p-2 text-xs overflow-auto whitespace-pre-wrap break-words">
                  {task.spawn_args}
                </pre>
              </details>
            ) : (
              <p
                className="text-xs text-slate-500"
                data-testid="farm-spawn-args-redacted"
              >
                {t("detail.spawnArgsRedacted")}
              </p>
            )
          )}
        </div>
      )}

      {/* sha diff range */}
      {result && (result.sha_before || result.sha_after) && (
        <div className="mt-4 rounded-lg border bg-white p-4 shadow-sm">
          <h2 className="mb-2 text-base font-semibold">
            {t("detail.diffRange")}
          </h2>
          <div className="flex flex-wrap items-center gap-2 text-sm">
            <span className="text-slate-600">{t("detail.shaBefore")}:</span>
            {ghBeforeUrl ? (
              <a
                href={ghBeforeUrl}
                target="_blank"
                rel="noreferrer"
                className="font-mono text-blue-600 hover:underline"
              >
                {shortSha(result.sha_before)}
              </a>
            ) : (
              <span className="font-mono">{shortSha(result.sha_before)}</span>
            )}
            <span className="text-slate-400">→</span>
            <span className="text-slate-600">{t("detail.shaAfter")}:</span>
            {ghAfterUrl ? (
              <a
                href={ghAfterUrl}
                target="_blank"
                rel="noreferrer"
                className="font-mono text-blue-600 hover:underline"
              >
                {shortSha(result.sha_after)}
              </a>
            ) : (
              <span className="font-mono">{shortSha(result.sha_after)}</span>
            )}
          </div>
        </div>
      )}

      {/* Heartbeats */}
      <div className="mt-6">
        <h2 className="mb-3 text-base font-semibold">
          {t("detail.heartbeatsTitle")}{" "}
          <span className="text-xs font-normal text-slate-600">
            {t("detail.heartbeatsCount", { count: heartbeats.length })}
          </span>
        </h2>
        <EventTimeline events={heartbeats.map(heartbeatToEvent)} />
      </div>

      {/* Results summary */}
      <div className="mt-6">
        <h2 className="mb-3 text-base font-semibold">
          {t("detail.summaryTitle")}
        </h2>
        {result?.summary ? (
          <pre
            data-testid="farm-summary"
            className="whitespace-pre-wrap rounded-lg border bg-white p-4 text-sm text-slate-700 shadow-sm font-sans leading-relaxed"
          >
            {result.summary}
          </pre>
        ) : resultMissing ? (
          <p className="text-sm text-slate-600">{t("detail.summaryMissing")}</p>
        ) : (
          <p className="text-sm text-slate-600">{t("detail.summaryEmpty")}</p>
        )}
      </div>
    </div>
  );
}
