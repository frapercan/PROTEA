"use client";

import { useEffect, useState } from "react";
import { useTranslations } from "next-intl";
import { ContextBanner } from "@/components/ContextBanner";
import {
  listAnnotationSets, listPredictionSets, listScoringConfigs,
  listRerankers, baseUrl,
} from "@/lib/api";
import type { AnnotationSet, PredictionSet, ScoringConfig, RerankerModel } from "@/lib/api";

const labelClass = "block text-sm font-medium text-gray-700 mb-1";
const selectClass =
  "w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
const btnPrimary =
  "rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 transition-colors";
const btnSecondary =
  "rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50 transition-colors";

function InfoTooltip({ text }: { text: string }) {
  return (
    <span className="relative inline-block group ml-1 align-middle">
      <span
        aria-label={text}
        className="inline-flex items-center justify-center w-4 h-4 rounded-full bg-gray-200 text-gray-500 text-[10px] font-bold cursor-help select-none"
      >
        ?
      </span>
      <span className="pointer-events-none absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 z-10 hidden group-hover:block w-64 rounded-md border border-gray-200 bg-white px-3 py-2 text-xs text-gray-600 shadow-lg leading-relaxed">
        {text}
      </span>
    </span>
  );
}

function RichTooltip({ children }: { children: React.ReactNode }) {
  return (
    <span className="relative inline-block group ml-1 align-middle">
      <span className="inline-flex items-center justify-center w-4 h-4 rounded-full bg-blue-100 text-blue-500 text-[10px] font-bold cursor-help select-none">
        ℹ
      </span>
      <span className="pointer-events-none absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 z-10 hidden group-hover:block w-72 rounded-md border border-gray-200 bg-white px-3 py-2.5 text-xs text-gray-600 shadow-xl leading-relaxed">
        {children}
      </span>
    </span>
  );
}

type NsMetrics = {
  fmax: number;
  precision: number;
  recall: number;
  tau: number;
  coverage: number;
  n_proteins?: number;
};

type SettingResults = Record<string, NsMetrics>; // BPO | MFO | CCO

type EvaluationResult = {
  id: string;
  evaluation_set_id: string;
  prediction_set_id: string;
  scoring_config_id: string | null;
  reranker_model_id: string | null;
  reranker_config: Record<string, Record<string, string>> | null;
  job_id: string | null;
  created_at: string;
  results: Record<string, SettingResults>; // NK | LK | PK
};

type EvaluationSet = {
  id: string;
  old_annotation_set_id: string;
  new_annotation_set_id: string;
  job_id: string | null;
  created_at: string;
  stats: {
    delta_proteins?: number;
    nk_proteins?: number;
    lk_proteins?: number;
    pk_proteins?: number;
    nk_annotations?: number;
    lk_annotations?: number;
    pk_annotations?: number;
    known_terms_count?: number;
  };
};

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${baseUrl()}${path}`, { cache: "no-store", ...init });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

const listEvaluationSets = () => apiFetch<EvaluationSet[]>("/annotations/evaluation-sets");
const listResults = (evalId: string) =>
  apiFetch<EvaluationResult[]>(`/annotations/evaluation-sets/${evalId}/results`);
const deleteEvaluationSet = (evalId: string) =>
  fetch(`${baseUrl()}/annotations/evaluation-sets/${evalId}`, { method: "DELETE" });

function setLabel(s: AnnotationSet) {
  const date = new Date(s.created_at).toLocaleDateString();
  const count = s.annotation_count != null ? ` · ${s.annotation_count.toLocaleString()} ann.` : "";
  return `[${s.source.toUpperCase()}] ${s.source_version ?? "—"} · ${date}${count}`;
}

function predLabel(p: PredictionSet) {
  const parts: string[] = [];
  if (p.embedding_config_name) parts.push(p.embedding_config_name);
  if (p.annotation_set_label) parts.push(p.annotation_set_label);
  parts.push(`k=${p.limit_per_entry}`);
  if (p.prediction_count != null) parts.push(`${p.prediction_count.toLocaleString()} preds`);
  const date = new Date(p.created_at).toLocaleDateString();
  return `${parts.join(" · ")} · ${date} (${p.id.slice(0, 8)}…)`;
}

function evalLabel(e: EvaluationSet, annotationSets: AnnotationSet[]) {
  const date = new Date(e.created_at).toLocaleDateString();
  const oldSet = annotationSets.find((a) => a.id === e.old_annotation_set_id);
  const newSet = annotationSets.find((a) => a.id === e.new_annotation_set_id);
  const delta = e.stats.delta_proteins ?? "?";
  return `${oldSet?.source_version ?? "old"} → ${newSet?.source_version ?? "new"} · ${delta} delta proteins · ${date}`;
}

function DownloadLink({ href, label, filename }: { href: string; label: string; filename: string }) {
  return (
    <a
      href={href}
      download={filename}
      className="inline-flex items-center gap-1 rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm hover:bg-gray-50 transition-colors"
    >
      ↓ {label}
    </a>
  );
}

function StatBadge({ label, value, tooltip }: { label: string; value: number | undefined; tooltip?: string }) {
  return (
    <div className="rounded-lg border border-gray-200 bg-gray-50 px-4 py-3 text-center">
      <div className="text-lg font-semibold text-gray-900">
        {value != null ? value.toLocaleString() : "—"}
      </div>
      <div className="text-xs text-gray-500 mt-0.5">
        {label}
        {tooltip && <InfoTooltip text={tooltip} />}
      </div>
    </div>
  );
}

const SETTING_COLORS: Record<string, string> = {
  NK: "bg-purple-50 border-purple-200",
  LK: "bg-blue-50 border-blue-200",
  PK: "bg-green-50 border-green-200",
};

function ResultsTable({ results }: { results: Record<string, SettingResults> }) {
  const t = useTranslations("evaluation");
  const settings = ["NK", "LK", "PK"].filter((s) => results[s] && Object.keys(results[s]).length > 0);
  if (settings.length === 0) return <p className="text-sm text-gray-400">{t("evaluationSetCard.noEvaluations")}</p>;

  const NS_LABELS: Record<string, string> = {
    BPO: t("resultMetrics.biologicalProcess"),
    MFO: t("resultMetrics.molecularFunction"),
    CCO: t("resultMetrics.cellularComponent"),
  };

  const SETTING_TOOLTIPS: Record<string, string> = {
    NK: "No-Knowledge: proteins with no experimental annotations in any namespace at t0. Evaluated without -known.",
    LK: "Limited-Knowledge: proteins annotated in some namespaces at t0 but not in the evaluated namespace. Evaluated without -known.",
    PK: "Partial-Knowledge: proteins that already had annotations in the evaluated namespace at t0. Evaluated with -known (old terms excluded from scoring).",
  };

  return (
    <div className="space-y-4">
      {settings.map((setting) => (
        <div key={setting} className={`rounded-lg border p-4 ${SETTING_COLORS[setting] ?? ""}`}>
          <div className="text-sm font-semibold text-gray-700 mb-3">
            {setting}
            {SETTING_TOOLTIPS[setting] && <InfoTooltip text={SETTING_TOOLTIPS[setting]} />}
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {["BPO", "MFO", "CCO"].map((ns) => {
              const m = results[setting]?.[ns];
              if (!m) return null;
              return (
                <div key={ns} className="rounded-md bg-white border border-gray-200 p-3">
                  <div className="text-xs font-medium text-gray-500 mb-2">{NS_LABELS[ns]}</div>
                  <div className="space-y-1 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-500">{t("resultMetrics.fmax")}</span>
                      <span className="font-semibold text-gray-900">{m.fmax.toFixed(3)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">{t("resultMetrics.precision")}</span>
                      <span className="text-gray-700">{m.precision.toFixed(3)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">{t("resultMetrics.recall")}</span>
                      <span className="text-gray-700">{m.recall.toFixed(3)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 flex items-center gap-1">
                        {t("resultMetrics.coverage")}
                        <InfoTooltip text="Fraction of benchmark proteins for which at least one prediction was submitted at the Fmax threshold. Values above 100% can occur in the PK setting: cafaeval shrinks the PK denominator by removing proteins whose new terms are excluded via the -known file, while PROTEA already subtracts those terms when building the ground truth — this double-accounting inflates the ratio. NK and LK coverage is always ≤ 100%." />
                      </span>
                      <span className="text-gray-700">{(m.coverage * 100).toFixed(1)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">{t("resultMetrics.tau")}</span>
                      <span className="text-gray-700">{m.tau.toFixed(2)}</span>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}

function EvaluationSetCard({
  e,
  annotationSets,
  predictionSets,
  scoringConfigs,
  rerankers: initialRerankers,
  isSelected,
  onSelect,
  onDeleted,
}: {
  e: EvaluationSet;
  annotationSets: AnnotationSet[];
  predictionSets: PredictionSet[];
  scoringConfigs: ScoringConfig[];
  rerankers: RerankerModel[];
  isSelected: boolean;
  onSelect: () => void;
  onDeleted: () => void;
}) {
  const t = useTranslations("evaluation");
  const [results, setResults] = useState<EvaluationResult[]>([]);
  const [loadingResults, setLoadingResults] = useState(false);
  const [predSetId, setPredSetId] = useState("");
  const [maxDistance, setMaxDistance] = useState("");
  const [scoringConfigId, setScoringConfigId] = useState("");
  // 3x3 reranker grid: category × aspect
  const [rrGrid, setRrGrid] = useState<Record<string, Record<string, string>>>({
    nk: { bpo: "", mfo: "", cco: "" },
    lk: { bpo: "", mfo: "", cco: "" },
    pk: { bpo: "", mfo: "", cco: "" },
  });
  const setRrCell = (cat: string, asp: string, val: string) =>
    setRrGrid((prev) => ({ ...prev, [cat]: { ...prev[cat], [asp]: val } }));
  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState("");
  const [pendingJobId, setPendingJobId] = useState<string | null>(null);
  const [pollingResults, setPollingResults] = useState(false);

  useEffect(() => {
    if (!isSelected) return;
    setLoadingResults(true);
    listResults(e.id).then(setResults).finally(() => setLoadingResults(false));
  }, [isSelected, e.id]);

  useEffect(() => {
    if (!pendingJobId) return;

    setPollingResults(true);
    let attempts = 0;
    const MAX_ATTEMPTS = 30;

    const interval = setInterval(async () => {
      attempts++;
      try {
        const fresh = await listResults(e.id);
        if (fresh.length > results.length) {
          setResults(fresh);
          setPendingJobId(null);
          setPollingResults(false);
          clearInterval(interval);
          return;
        }
      } catch {
        // Ignore transient errors during polling.
      }
      if (attempts >= MAX_ATTEMPTS) {
        setPollingResults(false);
        clearInterval(interval);
      }
    }, 10_000);

    return () => clearInterval(interval);
  }, [pendingJobId]);

  async function refreshResults() {
    setLoadingResults(true);
    try {
      setResults(await listResults(e.id));
    } finally {
      setLoadingResults(false);
    }
  }

  async function handleRun() {
    if (!predSetId) return;
    setRunning(true);
    setRunError("");
    setPendingJobId(null);
    try {
      const body: Record<string, any> = { prediction_set_id: predSetId };
      if (maxDistance) body.max_distance = parseFloat(maxDistance);
      // Build nested rerankers mapping from the 3×3 grid
      const rerankers: Record<string, Record<string, string>> = {};
      let hasAnyReranker = false;
      for (const cat of ["nk", "lk", "pk"]) {
        const catMap: Record<string, string> = {};
        for (const asp of ["bpo", "mfo", "cco"]) {
          if (rrGrid[cat]?.[asp]) {
            catMap[asp] = rrGrid[cat][asp];
            hasAnyReranker = true;
          }
        }
        if (Object.keys(catMap).length > 0) rerankers[cat] = catMap;
      }
      if (hasAnyReranker) {
        body.rerankers = rerankers;
      } else if (scoringConfigId) {
        body.scoring_config_id = scoringConfigId;
      }
      const res = await apiFetch<{ id: string; status: string }>(
        `/annotations/evaluation-sets/${e.id}/run`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      setPendingJobId(res.id ?? res.status ?? null);
    } catch (err: any) {
      setRunError(err.message ?? "Unknown error");
    } finally {
      setRunning(false);
    }
  }

  return (
    <div
      className={`rounded-lg border transition-colors ${
        isSelected ? "border-blue-400" : "border-gray-200"
      }`}
    >
      {/* Header */}
      <div
        className="cursor-pointer p-4 hover:bg-gray-50 rounded-t-lg"
        onClick={onSelect}
      >
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
          <div className="text-sm font-medium text-gray-800">{evalLabel(e, annotationSets)}</div>
          <button
            onClick={async (ev) => {
              ev.stopPropagation();
              if (!confirm(t("evaluationSetCard.deleteConfirm"))) return;
              await deleteEvaluationSet(e.id);
              onDeleted();
            }}
            className="rounded border border-red-200 px-2 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
          >
            {t("evaluationSetCard.delete")}
          </button>
        </div>
        <div className="mt-3 grid grid-cols-2 sm:grid-cols-4 gap-2">
          <StatBadge
            label={t("evaluationSetCard.deltaProteins")}
            value={e.stats.delta_proteins}
            tooltip="Proteins that gained ≥1 new experimental GO annotation between the old and new snapshot."
          />
          <StatBadge
            label={t("evaluationSetCard.nkProteins")}
            value={e.stats.nk_proteins}
            tooltip="No-Knowledge: proteins with no experimental annotations in any namespace at t0. All new annotations form the ground truth."
          />
          <StatBadge
            label={t("evaluationSetCard.lkProteins")}
            value={e.stats.lk_proteins}
            tooltip="Limited-Knowledge: proteins annotated in some namespaces at t0, but which gained new terms in a namespace where they had nothing. Only the new terms in that empty namespace are evaluated."
          />
          <StatBadge
            label={t("evaluationSetCard.pkProteins")}
            value={e.stats.pk_proteins}
            tooltip="Partial-Knowledge: proteins that already had annotations in a namespace at t0 and gained new terms in that same namespace. Only the novel terms are evaluated; old terms are excluded via -known. A protein can be LK in one namespace and PK in another simultaneously."
          />
        </div>
      </div>

      {isSelected && (
        <div className="border-t border-gray-200 p-4 space-y-5 bg-gray-50 rounded-b-lg">

          {/* Downloads */}
          <div>
            <p className="text-xs font-medium text-gray-500 mb-2">
              {t("evaluationSetCard.groundTruthFiles")}
              <InfoTooltip text="2-column TSV files (protein accession → GO term) used as input to the cafaeval evaluator. Each file contains only the novel experimental annotations for that category." />
            </p>
            <div className="flex flex-wrap gap-2">
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/ground-truth-NK.tsv`}
                label={t("evaluationSetCard.downloadNK")}
                filename="ground_truth_NK.tsv"
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/ground-truth-LK.tsv`}
                label={t("evaluationSetCard.downloadLK")}
                filename="ground_truth_LK.tsv"
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/ground-truth-PK.tsv`}
                label={t("evaluationSetCard.downloadPK")}
                filename="ground_truth_PK.tsv"
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/known-terms.tsv`}
                label={t("evaluationSetCard.downloadKnownTerms")}
                filename="known_terms.tsv"
              />
            </div>
            <p className="text-xs text-gray-400 mt-1">
              {t("evaluationSetCard.downloadKnownTerms")}
              <InfoTooltip text="All experimental annotations from the old snapshot for PK proteins in the relevant namespace. Passed to cafaeval with -known to exclude them from scoring — this penalises methods that simply repeat prior annotations." />
              : passed to cafaeval as <code className="font-mono">-known</code> for the PK pass only.
            </p>
          </div>

          {/* FASTA downloads */}
          <div>
            <p className="text-xs font-medium text-gray-500 mb-2">
              {t("evaluationSetCard.deltaProteinSequences")}
              <InfoTooltip text="Sequences of proteins that gained new experimental GO annotations (delta proteins). Download the full set or per-category subsets to compute embeddings and run predictions before evaluation." />
            </p>
            <div className="flex flex-wrap gap-2">
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/delta-proteins.fasta?category=all`}
                label={t("evaluationSetCard.allDelta")}
                filename={`delta_proteins_all_${e.id.slice(0, 8)}.fasta`}
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/delta-proteins.fasta?category=nk`}
                label={t("evaluationSetCard.nkOnly")}
                filename={`delta_proteins_nk_${e.id.slice(0, 8)}.fasta`}
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/delta-proteins.fasta?category=lk`}
                label={t("evaluationSetCard.lkOnly")}
                filename={`delta_proteins_lk_${e.id.slice(0, 8)}.fasta`}
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/delta-proteins.fasta?category=pk`}
                label={t("evaluationSetCard.pkOnly")}
                filename={`delta_proteins_pk_${e.id.slice(0, 8)}.fasta`}
              />
            </div>
          </div>

          {/* Run evaluation */}
          <div className="space-y-3">
            <p className="text-xs font-medium text-gray-500">{t("evaluationSetCard.runCafaEvaluator")}</p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <div>
                <label className={labelClass}>{t("evaluationSetCard.predictionSetLabel")}</label>
                <select
                  value={predSetId}
                  onChange={(ev) => setPredSetId(ev.target.value)}
                  className={selectClass}
                >
                  <option value="">{t("generateSection.selectSet")}</option>
                  {predictionSets.map((p) => (
                    <option key={p.id} value={p.id}>{predLabel(p)}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className={labelClass}>{t("evaluationSetCard.maxDistanceLabel")}</label>
                <input
                  type="number" min="0" max="2" step="0.05" placeholder="no limit"
                  value={maxDistance}
                  onChange={(ev) => setMaxDistance(ev.target.value)}
                  className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>

            {/* Scoring method — 3×3 grid (category × aspect) */}
            <div>
              <label className={labelClass}>
                Re-ranker models
                <InfoTooltip text="Select a trained LightGBM re-ranker for each cell (category × aspect). Cells left empty fall back to the scoring config or default 1 − distance/2. Models trained for a specific aspect are shown with their aspect badge." />
              </label>
              {initialRerankers.length > 0 && (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs border-collapse">
                    <thead>
                      <tr>
                        <th className="px-2 py-1 text-left text-gray-500 font-medium"></th>
                        <th className="px-2 py-1 text-center text-gray-600 font-semibold">BPO</th>
                        <th className="px-2 py-1 text-center text-gray-600 font-semibold">MFO</th>
                        <th className="px-2 py-1 text-center text-gray-600 font-semibold">CCO</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(["nk", "lk", "pk"] as const).map((cat) => (
                        <tr key={cat}>
                          <td className="px-2 py-1.5 font-semibold text-gray-700 uppercase">{cat}</td>
                          {(["bpo", "mfo", "cco"] as const).map((asp) => {
                            // Show models matching this category+aspect, or category+null (all-aspect models)
                            const candidates = initialRerankers.filter(
                              (r) => r.category === cat && (r.aspect === asp || r.aspect === null)
                            );
                            return (
                              <td key={asp} className="px-1 py-1">
                                <select
                                  value={rrGrid[cat]?.[asp] ?? ""}
                                  onChange={(ev) => { setRrCell(cat, asp, ev.target.value); if (ev.target.value) setScoringConfigId(""); }}
                                  className="w-full rounded border border-gray-300 px-1.5 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400"
                                >
                                  <option value="">—</option>
                                  {candidates.map((r) => (
                                    <option key={r.id} value={r.id}>
                                      {r.name}{r.aspect ? ` [${r.aspect.toUpperCase()}]` : " [all]"} · AUC {r.metrics.val_auc?.toFixed(3)}
                                    </option>
                                  ))}
                                </select>
                              </td>
                            );
                          })}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {(() => {
                const hasAnyRr = Object.values(rrGrid).some((catMap) => Object.values(catMap).some(Boolean));
                return scoringConfigs.length > 0 && !hasAnyRr ? (
                  <div className="mt-2">
                    <label className="text-xs text-gray-500 mb-0.5 block">Scoring config (alternative to re-ranker)</label>
                    <select
                      value={scoringConfigId}
                      onChange={(ev) => setScoringConfigId(ev.target.value)}
                      className={selectClass}
                    >
                      <option value="">Default (1 − distance / 2)</option>
                      {scoringConfigs.map((c) => (
                        <option key={c.id} value={c.id}>{c.name}</option>
                      ))}
                    </select>
                  </div>
                ) : null;
              })()}
            </div>
            {runError && (
              <p className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
                {runError}
              </p>
            )}

            {pendingJobId && (
              <div className="rounded border border-blue-200 bg-blue-50 px-3 py-2.5 text-sm text-blue-800 flex items-center justify-between gap-3">
                <span>
                  {t("evaluationSetCard.jobQueued")}{" "}
                  {pollingResults
                    ? "Checking for results every 10 s…"
                    : "Results will appear below when the job completes."}
                </span>
                <a
                  href={`/jobs/${pendingJobId}`}
                  className="shrink-0 rounded border border-blue-300 bg-white px-2 py-1 text-xs font-medium text-blue-700 hover:bg-blue-100 transition-colors"
                >
                  {t("evaluationSetCard.viewJob")}
                </a>
              </div>
            )}

            <button
              onClick={handleRun}
              disabled={!predSetId || running}
              className={btnPrimary}
            >
              {running ? t("generateSection.generating") : t("evaluationSetCard.runEvaluation")}
            </button>
          </div>

          {/* Results */}
          <div>
            <div className="flex items-center justify-between mb-3">
              <p className="text-xs font-medium text-gray-500">
                {t("evaluationSetCard.resultsHeading")}
                {pollingResults && (
                  <span className="ml-2 text-blue-500 animate-pulse">{t("evaluationSetCard.pollingResults")}</span>
                )}
              </p>
              <button
                onClick={refreshResults}
                disabled={loadingResults}
                className="text-xs text-gray-400 hover:text-gray-700 border rounded px-2 py-0.5 disabled:opacity-40"
              >
                {loadingResults ? t("evaluationSetCard.refreshing") : t("evaluationSetCard.refreshResults")}
              </button>
            </div>
            {loadingResults ? (
              <p className="text-sm text-gray-400">Loading…</p>
            ) : results.length === 0 ? (
              <p className="text-sm text-gray-400">{t("evaluationSetCard.noEvaluations")}</p>
            ) : (
              <div className="space-y-6">
                {results.map((r) => {
                  const pred = predictionSets.find((p) => p.id === r.prediction_set_id);
                  const sc = scoringConfigs.find((c) => c.id === r.scoring_config_id);
                  const hasReranker = !!r.reranker_model_id;
                  const rr = initialRerankers.find((m) => m.id === r.reranker_model_id);
                  return (
                    <div key={r.id} className="rounded-lg border border-gray-200 bg-white p-4 space-y-3">
                      {/* Meta header */}
                      <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-2">
                        <div className="space-y-0.5 text-xs text-gray-500 min-w-0">
                          <div className="flex items-center gap-0.5">
                            <span className="font-medium text-gray-700">{t("evaluationSetCard.predictionSet")} </span>
                            {pred
                              ? <span title={r.prediction_set_id}>{r.prediction_set_id.slice(0, 8)}… · {new Date(pred.created_at).toLocaleDateString()}{pred.prediction_count != null ? ` · ${pred.prediction_count.toLocaleString()} preds.` : ""}</span>
                              : <span className="font-mono">{r.prediction_set_id.slice(0, 8)}…</span>
                            }
                            {pred && (
                              <RichTooltip>
                                <div className="space-y-1.5">
                                  <div className="font-semibold text-gray-700 border-b border-gray-100 pb-1 mb-1">Prediction Set</div>
                                  <div className="flex justify-between gap-3">
                                    <span className="text-gray-400">Config</span>
                                    <span className="text-right">{pred.embedding_config_name ?? pred.embedding_config_id.slice(0, 8) + "…"}</span>
                                  </div>
                                  <div className="flex justify-between gap-3">
                                    <span className="text-gray-400">Annotations</span>
                                    <span className="text-right">{pred.annotation_set_label ?? pred.annotation_set_id.slice(0, 8) + "…"}</span>
                                  </div>
                                  <div className="flex justify-between gap-3">
                                    <span className="text-gray-400">Ontology</span>
                                    <span className="text-right">{pred.ontology_snapshot_version ?? pred.ontology_snapshot_id.slice(0, 8) + "…"}</span>
                                  </div>
                                  <div className="flex justify-between gap-3">
                                    <span className="text-gray-400">Max dist.</span>
                                    <span>{pred.distance_threshold ?? "—"}</span>
                                  </div>
                                  <div className="flex justify-between gap-3">
                                    <span className="text-gray-400">Limit/entry</span>
                                    <span>{pred.limit_per_entry}</span>
                                  </div>
                                </div>
                              </RichTooltip>
                            )}
                          </div>
                          <div className="flex items-center gap-0.5 flex-wrap">
                            <span className="font-medium text-gray-700">{t("evaluationSetCard.scoring")} </span>
                            {r.reranker_config ? (
                              <span className="inline-flex items-center gap-1 flex-wrap">
                                <span className="rounded-full bg-indigo-50 border border-indigo-100 px-1.5 py-0.5 text-[10px] font-medium text-indigo-700">Re-ranker</span>
                                {Object.entries(r.reranker_config).map(([cat, aspMap]) => (
                                  <span key={cat} className="text-[10px] text-gray-500">
                                    {cat.toUpperCase()}({Object.keys(aspMap).map(a => a.toUpperCase()).join(",")})
                                  </span>
                                ))}
                              </span>
                            ) : hasReranker ? (
                              <span className="inline-flex items-center gap-1">
                                <span className="rounded-full bg-indigo-50 border border-indigo-100 px-1.5 py-0.5 text-[10px] font-medium text-indigo-700">Re-ranker</span>
                                {rr ? rr.name : "model"}
                              </span>
                            ) : sc ? sc.name : <span className="italic text-gray-400">{t("evaluationSetCard.fallbackFormula")}</span>}
                            {sc && !hasReranker && (
                              <RichTooltip>
                                <div className="space-y-1.5">
                                  <div className="font-semibold text-gray-700 border-b border-gray-100 pb-1 mb-1">{sc.name}</div>
                                  {sc.description && <div className="italic text-gray-500 mb-1">{sc.description}</div>}
                                  <div className="flex justify-between gap-3">
                                    <span className="text-gray-400">Formula</span>
                                    <code className="text-blue-600 text-[10px]">{sc.formula}</code>
                                  </div>
                                  {Object.keys(sc.weights).length > 0 && (
                                    <div>
                                      <div className="text-gray-400 mb-0.5">Weights</div>
                                      {Object.entries(sc.weights).map(([k, v]) => (
                                        <div key={k} className="flex justify-between gap-3 pl-2">
                                          <span className="text-gray-500">{k}</span>
                                          <span className="font-mono">{v}</span>
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              </RichTooltip>
                            )}
                          </div>
                          <div className="text-gray-400">{new Date(r.created_at).toLocaleString()}</div>
                        </div>
                        <div className="flex items-center gap-2 shrink-0">
                          <a
                            href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/results/${r.id}/artifacts.zip`}
                            download={`cafaeval_${r.id.slice(0, 8)}.zip`}
                            className="inline-flex items-center gap-1 rounded-md border border-gray-300 bg-white px-3 py-1 text-xs hover:bg-gray-50 transition-colors"
                          >
                            {t("evaluationSetCard.artifactsDownload")}
                          </a>
                          <button
                            onClick={async () => {
                              if (!confirm(t("evaluationSetCard.deleteResultConfirm"))) return;
                              const res = await fetch(`${baseUrl()}/annotations/evaluation-sets/${e.id}/results/${r.id}`, { method: "DELETE" });
                              if (!res.ok) throw new Error(await res.text());
                              setResults((prev) => prev.filter((x) => x.id !== r.id));
                            }}
                            className="rounded-md border border-red-200 bg-white px-3 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
                          >
                            {t("evaluationSetCard.resultDelete")}
                          </button>
                        </div>
                      </div>
                      <ResultsTable results={r.results} />
                    </div>
                  );
                })}
              </div>
            )}
          </div>

        </div>
      )}
    </div>
  );
}

export default function EvaluationPage() {
  const t = useTranslations("evaluation");
  const [annotationSets, setAnnotationSets] = useState<AnnotationSet[]>([]);
  const [predictionSets, setPredictionSets] = useState<PredictionSet[]>([]);
  const [evaluationSets, setEvaluationSets] = useState<EvaluationSet[]>([]);
  const [scoringConfigs, setScoringConfigs] = useState<ScoringConfig[]>([]);
  const [rerankers, setRerankers] = useState<RerankerModel[]>([]);
  const [loading, setLoading] = useState(true);

  const [oldSetId, setOldSetId] = useState("");
  const [newSetId, setNewSetId] = useState("");
  const [generating, setGenerating] = useState(false);
  const [genError, setGenError] = useState("");
  const [selectedEvalId, setSelectedEvalId] = useState("");

  const reload = () =>
    Promise.all([listAnnotationSets(), listPredictionSets(), listEvaluationSets(), listScoringConfigs(), listRerankers()])
      .then(([ann, pred, ev, sc, rr]) => {
        setAnnotationSets(ann);
        setPredictionSets(pred);
        setEvaluationSets(ev);
        setScoringConfigs(sc);
        setRerankers(rr);
      })
      .finally(() => setLoading(false));

  useEffect(() => { reload(); }, []);

  const goaSets = annotationSets.filter((s) => s.source === "goa");
  const canGenerate = oldSetId && newSetId && oldSetId !== newSetId;

  async function handleGenerate() {
    setGenerating(true);
    setGenError("");
    try {
      await apiFetch("/annotations/evaluation-sets/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ old_annotation_set_id: oldSetId, new_annotation_set_id: newSetId }),
      });
      setOldSetId("");
      setNewSetId("");
      await reload();
    } catch (e: any) {
      setGenError(e.message ?? "Unknown error");
    } finally {
      setGenerating(false);
    }
  }

  if (loading) return <div className="p-8 text-sm text-gray-500">Loading…</div>;

  return (
    <div className="px-4 sm:px-8 py-6 sm:py-8 max-w-3xl space-y-8 sm:space-y-10">
      <h1 className="text-xl font-semibold text-gray-900">{t("title")}</h1>

      <ContextBanner
        title="Benchmark prediction quality with CAFA metrics"
        description="Compare two GOA releases (temporal holdout) to evaluate prediction accuracy. Computes Fmax, precision, recall, and coverage per aspect and category."
        prerequisites={[
          { label: `${annotationSets.length} annotation set(s)`, met: annotationSets.length >= 2, href: "/annotations" },
          { label: `${predictionSets.length} prediction set(s)`, met: predictionSets.length > 0, href: "/functional-annotation" },
        ]}
        nextStep={{ label: "Scoring configs", href: "/scoring" }}
      />

      {/* ── Generate Evaluation Set ───────────────────────────────── */}
      <section className="rounded-lg border border-gray-200 p-6 space-y-5">
        <div>
          <h2 className="text-base font-semibold text-gray-800">{t("generateSection.heading")}</h2>
          <p className="mt-1 text-sm text-gray-500">
            {t("generateSection.description")}
          </p>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <label className={labelClass}>{t("generateSection.oldSetLabel")}</label>
            <select value={oldSetId} onChange={(e) => setOldSetId(e.target.value)} className={selectClass}>
              <option value="">{t("generateSection.selectSet")}</option>
              {goaSets.map((s) => (
                <option key={s.id} value={s.id}>{setLabel(s)}</option>
              ))}
            </select>
          </div>
          <div>
            <label className={labelClass}>{t("generateSection.newSetLabel")}</label>
            <select value={newSetId} onChange={(e) => setNewSetId(e.target.value)} className={selectClass}>
              <option value="">{t("generateSection.selectSet")}</option>
              {goaSets.map((s) => (
                <option key={s.id} value={s.id}>{setLabel(s)}</option>
              ))}
            </select>
          </div>
        </div>
        {oldSetId && newSetId && oldSetId === newSetId && (
          <p className="text-xs text-red-500">{t("generateSection.errorSameSets")}</p>
        )}
        {genError && (
          <p className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{genError}</p>
        )}
        <button onClick={handleGenerate} disabled={!canGenerate || generating} className={btnPrimary}>
          {generating ? t("generateSection.generating") : t("generateSection.generateEvaluationSet")}
        </button>
      </section>

      {/* ── Evaluation Sets ───────────────────────────────────────── */}
      {evaluationSets.length > 0 && (
        <section className="space-y-4">
          <h2 className="text-base font-semibold text-gray-800">{t("evaluationSetsSection.heading")}</h2>
          {evaluationSets.map((e) => (
            <EvaluationSetCard
              key={e.id}
              e={e}
              annotationSets={annotationSets}
              predictionSets={predictionSets}
              scoringConfigs={scoringConfigs}
              rerankers={rerankers}
              isSelected={selectedEvalId === e.id}
              onSelect={() => setSelectedEvalId(e.id === selectedEvalId ? "" : e.id)}
              onDeleted={() => setEvaluationSets((prev) => prev.filter((x) => x.id !== e.id))}
            />
          ))}
        </section>
      )}

      {/* ── Evaluator command reference ───────────────────────────── */}
      <section className="rounded-lg border border-gray-100 bg-gray-50 p-5">
        <h2 className="text-sm font-semibold text-gray-700 mb-2">{t("manualEvaluatorSection.heading")}</h2>
        <pre className="text-xs text-gray-600 overflow-x-auto whitespace-pre-wrap leading-relaxed">
{`python -m cafaeval go-basic.obo predictions/ ground_truth_NK.tsv -out_dir results/NK
python -m cafaeval go-basic.obo predictions/ ground_truth_LK.tsv -out_dir results/LK
python -m cafaeval go-basic.obo predictions/ ground_truth_PK.tsv -known known_terms.tsv -out_dir results/PK`}
        </pre>
      </section>
    </div>
  );
}
