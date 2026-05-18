"use client";

// FARM-UI.4 slice-DAG dashboard. Renders the agent-farm plan catalog as
// a cytoscape-dagre DAG via the SliceDag component, wraps the canvas in
// a four-axis filter chip strip (loop / phase / priority / status) with
// URL-synced query params so the view is shareable, and exposes a
// critical-path toggle that highlights the longest pending dep-chain.
//
// The data source is the /plan endpoint on the farm-api sidecar (see
// apps/farm-api/farm_api/routes/plan.py); the response is the flat
// PlanSlice list across every loop directory under plans/. The page is
// a client component because the filter chips depend on useUrlParam
// which runs against Next's client-side router. This matches the
// convention in /[locale]/farm/page.tsx.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslations } from "next-intl";
import { listFarmPlan, type FarmPlanSlice } from "@/lib/farmApi";
import {
  distinctValues,
  longestPendingPath,
  type CriticalPath,
} from "@/lib/planDag";
import { useUrlParam } from "@/lib/useUrlParam";
import { useToast } from "@/components/Toast";
import SliceDag, { SLICE_STATUS_COLOR } from "@/components/SliceDag";

const STATUS_KEYS = [
  "pending",
  "in_progress",
  "blocked",
  "done",
  "deferred",
] as const;

type FilterKey = "loop" | "phase" | "priority" | "status";

function ChipRow({
  label,
  values,
  active,
  onPick,
  allLabel,
  testId,
  labelFor,
}: {
  label: string;
  values: string[];
  active: string;
  onPick: (next: string) => void;
  allLabel: string;
  testId: string;
  labelFor?: (v: string) => string;
}) {
  return (
    <div className="flex flex-wrap items-center gap-1.5" data-testid={testId}>
      <span className="text-xs font-medium text-slate-600 mr-1">{label}:</span>
      <button
        type="button"
        onClick={() => onPick("")}
        className={`rounded-full border px-2.5 py-0.5 text-xs font-medium transition-colors ${
          active === ""
            ? "bg-slate-800 text-white border-slate-800"
            : "bg-white text-slate-600 border-slate-200 hover:bg-slate-50"
        }`}
        aria-pressed={active === ""}
      >
        {allLabel}
      </button>
      {values.map((v) => (
        <button
          key={v}
          type="button"
          onClick={() => onPick(v)}
          className={`rounded-full border px-2.5 py-0.5 text-xs font-medium transition-colors ${
            active === v
              ? "bg-blue-600 text-white border-blue-600"
              : "bg-white text-slate-600 border-slate-200 hover:bg-slate-50"
          }`}
          aria-pressed={active === v}
          data-testid={`${testId}-chip-${v}`}
        >
          {labelFor ? labelFor(v) : v}
        </button>
      ))}
    </div>
  );
}

export default function FarmPlanPage() {
  const t = useTranslations("farm.plan");
  const toast = useToast();
  const [slices, setSlices] = useState<FarmPlanSlice[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // URL-synced filter state. Empty string == no filter, mirrors the
  // pattern used by the existing list page. Hooks are called in a
  // fixed top-level order so React's hook rules stay satisfied.
  const loopFilter = useFilterParam("loop");
  const phaseFilter = useFilterParam("phase");
  const priorityFilter = useFilterParam("priority");
  const statusFilter = useFilterParam("status");
  const filters: Record<FilterKey, [string, (v: string) => void]> = {
    loop: loopFilter,
    phase: phaseFilter,
    priority: priorityFilter,
    status: statusFilter,
  };
  const [criticalRaw, setCriticalRaw] = useUrlParam("critical", "");
  const criticalOn = criticalRaw === "1";
  const toggleCritical = useCallback(() => {
    setCriticalRaw(criticalOn ? null : "1");
  }, [criticalOn, setCriticalRaw]);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setError("");
      const rows = await listFarmPlan();
      setSlices(rows);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      toast(msg, "error");
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const loops = useMemo(() => distinctValues(slices, "loop"), [slices]);
  const phases = useMemo(() => distinctValues(slices, "phase"), [slices]);
  const priorities = useMemo(
    () => distinctValues(slices, "priority"),
    [slices],
  );

  // Apply chip filters to the slice list. We keep deps to other still-
  // visible slices and drop edges that point at filtered-out nodes; the
  // DAG component does the second filter defensively too.
  const loopValue = loopFilter[0];
  const phaseValue = phaseFilter[0];
  const priorityValue = priorityFilter[0];
  const statusValue = statusFilter[0];
  const filteredSlices = useMemo(() => {
    return slices.filter((s) => {
      if (loopValue && s.loop !== loopValue) return false;
      if (phaseValue && s.phase !== phaseValue) return false;
      const prio = s.priority ?? "P2";
      if (priorityValue && prio !== priorityValue) return false;
      if (statusValue && s.status !== statusValue) return false;
      return true;
    });
  }, [slices, loopValue, phaseValue, priorityValue, statusValue]);

  const critical: CriticalPath = useMemo(() => {
    if (!criticalOn) return { ids: [], hours: 0 };
    return longestPendingPath(filteredSlices);
  }, [criticalOn, filteredSlices]);

  const criticalSet = useMemo(() => new Set(critical.ids), [critical]);

  // Tally counts per status across the filtered set so the legend can
  // double as a quick-glance breakdown.
  const statusTally = useMemo(() => {
    const tally: Record<string, number> = {};
    for (const s of filteredSlices) {
      tally[s.status] = (tally[s.status] ?? 0) + 1;
    }
    return tally;
  }, [filteredSlices]);

  return (
    <>
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-xl font-semibold">{t("title")}</h1>
        <span className="text-xs text-slate-600">{t("subtitle")}</span>
        <button
          type="button"
          onClick={() => void refresh()}
          className="ml-auto rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-slate-50"
        >
          {t("refresh")}
        </button>
      </div>

      <div className="mt-4 space-y-3 rounded-lg border bg-white p-3 shadow-sm">
        <ChipRow
          label={t("filters.loopLabel")}
          values={loops}
          active={filters.loop[0]}
          onPick={filters.loop[1]}
          allLabel={t("filters.allLabel")}
          testId="filter-loop"
        />
        <ChipRow
          label={t("filters.phaseLabel")}
          values={phases}
          active={filters.phase[0]}
          onPick={filters.phase[1]}
          allLabel={t("filters.allLabel")}
          testId="filter-phase"
        />
        <ChipRow
          label={t("filters.priorityLabel")}
          values={priorities}
          active={filters.priority[0]}
          onPick={filters.priority[1]}
          allLabel={t("filters.allLabel")}
          testId="filter-priority"
        />
        <ChipRow
          label={t("filters.statusLabel")}
          values={STATUS_KEYS as unknown as string[]}
          active={filters.status[0]}
          onPick={filters.status[1]}
          allLabel={t("filters.allLabel")}
          testId="filter-status"
          labelFor={(v) => t(`status.${v}`)}
        />
        <div className="flex flex-wrap items-center gap-2 pt-1">
          <button
            type="button"
            onClick={toggleCritical}
            aria-pressed={criticalOn}
            data-testid="critical-toggle"
            className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors ${
              criticalOn
                ? "bg-red-600 text-white border-red-600"
                : "bg-white text-slate-600 border-slate-200 hover:bg-slate-50"
            }`}
          >
            {criticalOn
              ? t("criticalPath.toggleOff")
              : t("criticalPath.toggleOn")}
          </button>
          {criticalOn && (
            <span className="text-xs text-slate-600" data-testid="critical-summary">
              {critical.ids.length === 0
                ? t("criticalPath.noneFound")
                : t("criticalPath.summary", {
                    count: critical.ids.length,
                    hours: critical.hours,
                  })}
            </span>
          )}
        </div>
      </div>

      {error && (
        <pre
          data-testid="plan-error"
          className="mt-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700"
        >
          {error}
        </pre>
      )}

      <div className="mt-4">
        {loading ? (
          <div
            className="flex items-center justify-center rounded-lg border bg-white text-sm text-slate-600"
            style={{ height: 560 }}
            data-testid="slice-dag-loading"
          >
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-blue-500 mr-2" />
            Loading slices…
          </div>
        ) : (
          <SliceDag
            slices={filteredSlices}
            criticalPathIds={criticalSet}
            ariaLabel={t("a11y.dagLabel", { count: filteredSlices.length })}
            srSummary={t("a11y.dagSummary", { count: filteredSlices.length })}
            srListTitle={t("a11y.dagListTitle")}
          />
        )}
      </div>

      <div
        className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 rounded-lg border bg-slate-50 px-4 py-2 text-xs text-slate-600"
        data-testid="slice-dag-legend"
      >
        <span className="font-medium text-slate-700">{t("legend.nodes")}:</span>
        {STATUS_KEYS.map((status) => {
          const style = SLICE_STATUS_COLOR[status];
          const count = statusTally[status] ?? 0;
          return (
            <span key={status} className="flex items-center gap-1.5">
              <span
                aria-hidden
                className="inline-block h-3 w-3 rounded-sm border"
                style={{
                  backgroundColor: style.bg,
                  borderColor: style.border,
                }}
              />
              <span className="capitalize">{t(`status.${status}`)}</span>
              <span className="font-mono text-slate-600">({count})</span>
            </span>
          );
        })}
        <span className="ml-auto flex items-center gap-3">
          <span className="flex items-center gap-1.5">
            <span
              aria-hidden
              className="inline-block w-4 border-t border-slate-300"
            />
            {t("legend.deps")}
          </span>
          <span className="flex items-center gap-1.5">
            <span
              aria-hidden
              className="inline-block w-4 border-t-2 border-red-600"
            />
            {t("legend.criticalEdge")}
          </span>
        </span>
      </div>

      <p className="mt-2 text-xs text-slate-600">
        {t("sliceCount", { count: filteredSlices.length })}
      </p>
    </>
  );
}

// Tiny wrapper that returns the value as a non-null string so the
// component reads more naturally than spelling `?? ""` at every call
// site.
function useFilterParam(key: string): [string, (v: string) => void] {
  const [raw, setRaw] = useUrlParam(key, "");
  const value = raw ?? "";
  const setValue = useCallback(
    (next: string) => setRaw(next === "" ? null : next),
    [setRaw],
  );
  return [value, setValue];
}
