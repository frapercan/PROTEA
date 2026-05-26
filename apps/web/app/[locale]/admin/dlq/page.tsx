"use client";

import { useEffect, useState } from "react";
import { useTranslations } from "next-intl";
import { useHasRole } from "@/lib/useRole";
import { useToast } from "@/components/Toast";
import {
  getDlqSummary,
  replayDlq,
  purgeDlq,
  type DlqGroup,
  type DlqSummary,
} from "@/lib/api";

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function ForbiddenPanel() {
  const t = useTranslations("dlq");
  return (
    <main className="max-w-2xl mx-auto px-4 py-12 text-center">
      <h1 className="text-2xl font-bold text-slate-900 mb-2">{t("forbiddenTitle")}</h1>
      <p className="text-sm text-slate-500">{t("forbiddenBody")}</p>
    </main>
  );
}

function SummaryTable({ groups }: { groups: DlqGroup[] }) {
  const t = useTranslations("dlq");
  if (groups.length === 0) {
    return (
      <p className="text-sm text-slate-500 italic py-4">{t("noGroups")}</p>
    );
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm border-collapse">
        <thead>
          <tr className="bg-slate-50 border-b border-slate-200">
            <th className="px-3 py-2 text-left font-medium text-slate-600">{t("colOperation")}</th>
            <th className="px-3 py-2 text-left font-medium text-slate-600">{t("colSourceQueue")}</th>
            <th className="px-3 py-2 text-left font-medium text-slate-600">{t("colAge")}</th>
            <th className="px-3 py-2 text-right font-medium text-slate-600">{t("colCount")}</th>
          </tr>
        </thead>
        <tbody>
          {groups.map((g, i) => (
            <tr key={i} className="border-b border-slate-100 hover:bg-slate-50 transition-colors">
              <td className="px-3 py-2 font-mono text-xs text-slate-800">{g.operation}</td>
              <td className="px-3 py-2 font-mono text-xs text-slate-500">{g.first_death_queue}</td>
              <td className="px-3 py-2 text-xs text-slate-500">{g.age_bucket}</td>
              <td className="px-3 py-2 text-right font-semibold text-slate-800">
                {g.count.toLocaleString()}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Action panel
// ---------------------------------------------------------------------------

type ActionResult = { label: string; detail: string; variant: "success" | "warning" } | null;

function ActionPanel({
  groups,
  onSuccess,
}: {
  groups: DlqGroup[];
  onSuccess: () => void;
}) {
  const t = useTranslations("dlq");
  const toast = useToast();

  const [operation, setOperation] = useState<string>("");
  const [queueFilter, setQueueFilter] = useState<string>("");
  const [dryRun, setDryRun] = useState<boolean>(true);
  const [confirmPurge, setConfirmPurge] = useState<boolean>(false);
  const [loading, setLoading] = useState<"replay" | "purge" | null>(null);
  const [result, setResult] = useState<ActionResult>(null);

  const uniqueOps = Array.from(new Set(groups.map((g) => g.operation))).sort();
  const uniqueQueues = Array.from(new Set(groups.map((g) => g.first_death_queue))).sort();

  async function handleReplay() {
    setLoading("replay");
    setResult(null);
    try {
      const r = await replayDlq({
        operation: operation || null,
        first_death_queue: queueFilter || null,
        dry_run: dryRun,
      });
      const detail = dryRun
        ? t("replayDryResult", { count: r.would_replay, skipped: r.skipped })
        : t("replayResult", { replayed: r.replayed, skipped: r.skipped });
      setResult({ label: dryRun ? t("dryRunBadge") : t("done"), detail, variant: "success" });
      if (!dryRun) onSuccess();
    } catch (e: any) {
      toast(e.message ?? t("actionFailed"), "error");
    } finally {
      setLoading(null);
    }
  }

  async function handlePurge() {
    setLoading("purge");
    setResult(null);
    setConfirmPurge(false);
    try {
      const r = await purgeDlq({
        operation: operation || null,
        first_death_queue: queueFilter || null,
        dry_run: dryRun,
      });
      const detail = dryRun
        ? t("purgeDryResult", { count: r.would_purge, skipped: r.skipped })
        : t("purgeResult", { purged: r.purged, skipped: r.skipped });
      setResult({ label: dryRun ? t("dryRunBadge") : t("done"), detail, variant: dryRun ? "warning" : "success" });
      if (!dryRun) onSuccess();
    } catch (e: any) {
      toast(e.message ?? t("actionFailed"), "error");
    } finally {
      setLoading(null);
    }
  }

  return (
    <div className="border border-slate-200 rounded-lg p-5 bg-white shadow-sm space-y-4">
      <h2 className="font-semibold text-slate-900">{t("actionTitle")}</h2>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div>
          <label className="block text-xs font-medium text-slate-600 mb-1">
            {t("filterOperation")}
          </label>
          <select
            value={operation}
            onChange={(e) => setOperation(e.target.value)}
            className="w-full text-sm border border-slate-300 rounded px-2 py-1.5 bg-white"
          >
            <option value="">{t("allOperations")}</option>
            {uniqueOps.map((op) => (
              <option key={op} value={op}>{op}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-slate-600 mb-1">
            {t("filterQueue")}
          </label>
          <select
            value={queueFilter}
            onChange={(e) => setQueueFilter(e.target.value)}
            className="w-full text-sm border border-slate-300 rounded px-2 py-1.5 bg-white"
          >
            <option value="">{t("allQueues")}</option>
            {uniqueQueues.map((q) => (
              <option key={q} value={q}>{q}</option>
            ))}
          </select>
        </div>
      </div>

      <label className="flex items-center gap-2 cursor-pointer select-none">
        <input
          type="checkbox"
          checked={dryRun}
          onChange={(e) => { setDryRun(e.target.checked); setConfirmPurge(false); }}
          className="rounded"
        />
        <span className="text-sm text-slate-700">{t("dryRunLabel")}</span>
      </label>

      {result && (
        <div
          className={`text-sm px-3 py-2 rounded border ${
            result.variant === "success"
              ? "bg-green-50 border-green-200 text-green-800"
              : "bg-amber-50 border-amber-200 text-amber-800"
          }`}
        >
          <span className="font-semibold mr-1">{result.label}:</span>
          {result.detail}
        </div>
      )}

      <div className="flex flex-wrap gap-2">
        <button
          onClick={handleReplay}
          disabled={loading !== null}
          className="px-3 py-1.5 text-sm bg-sky-600 text-white rounded hover:bg-sky-700 disabled:opacity-50 transition-colors"
        >
          {loading === "replay" ? t("working") : t("replayBtn")}
        </button>

        {!dryRun && !confirmPurge ? (
          <button
            onClick={() => setConfirmPurge(true)}
            disabled={loading !== null}
            className="px-3 py-1.5 text-sm bg-red-600 text-white rounded hover:bg-red-700 disabled:opacity-50 transition-colors"
          >
            {t("purgeBtn")}
          </button>
        ) : !dryRun && confirmPurge ? (
          <button
            onClick={handlePurge}
            disabled={loading !== null}
            className="px-3 py-1.5 text-sm bg-red-700 text-white rounded hover:bg-red-800 disabled:opacity-50 transition-colors font-semibold"
          >
            {loading === "purge" ? t("working") : t("purgeConfirmBtn")}
          </button>
        ) : (
          <button
            onClick={handlePurge}
            disabled={loading !== null}
            className="px-3 py-1.5 text-sm border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-50 transition-colors"
          >
            {loading === "purge" ? t("working") : t("purgeBtn")}
          </button>
        )}
      </div>

      {!dryRun && (
        <p className="text-xs text-red-600">
          {t("liveWarning")}
        </p>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function DlqPage() {
  const t = useTranslations("dlq");
  const toast = useToast();
  const isAdmin = useHasRole("admin");

  const [summary, setSummary] = useState<DlqSummary | null>(null);
  const [loading, setLoading] = useState(false);

  async function fetchSummary() {
    setLoading(true);
    try {
      setSummary(await getDlqSummary(500));
    } catch (e: any) {
      toast(e.message ?? t("loadFailed"), "error");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (isAdmin) fetchSummary();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isAdmin]);

  if (!isAdmin) return <ForbiddenPanel />;

  return (
    <main className="max-w-4xl mx-auto px-4 py-8 space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">{t("title")}</h1>
          <p className="text-sm text-slate-500 mt-1">{t("description")}</p>
        </div>
        <button
          onClick={fetchSummary}
          disabled={loading}
          className="shrink-0 px-3 py-1.5 text-sm border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-50 transition-colors"
        >
          {loading ? t("loading") : t("refresh")}
        </button>
      </div>

      <div className="border border-slate-200 rounded-lg p-5 bg-white shadow-sm">
        <div className="flex items-center justify-between mb-3">
          <h2 className="font-semibold text-slate-900">{t("summaryTitle")}</h2>
          {summary && (
            <div className="flex gap-3 text-xs text-slate-500">
              <span>
                {t("depth")}:{" "}
                <span className="font-semibold text-slate-800">
                  {summary.queue_message_count.toLocaleString()}
                </span>
              </span>
              <span>
                {t("peeked")}:{" "}
                <span className="font-semibold text-slate-800">
                  {summary.total_peeked.toLocaleString()}
                </span>
              </span>
            </div>
          )}
        </div>
        {loading && !summary ? (
          <p className="text-sm text-slate-400 italic">{t("loading")}</p>
        ) : summary ? (
          <SummaryTable groups={summary.groups} />
        ) : (
          <p className="text-sm text-slate-400 italic">{t("notLoaded")}</p>
        )}
      </div>

      {summary && (
        <ActionPanel groups={summary.groups} onSuccess={fetchSummary} />
      )}

      <div className="text-xs text-slate-400 border-t border-slate-100 pt-4">
        {t("baselineNote")}
      </div>
    </main>
  );
}
