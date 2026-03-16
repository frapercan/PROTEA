"use client";

import { useEffect, useState } from "react";
import { listAnnotationSets, listPredictionSets, listScoringConfigs, baseUrl } from "@/lib/api";
import type { AnnotationSet, PredictionSet, ScoringConfig } from "@/lib/api";

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
  const date = new Date(p.created_at).toLocaleDateString();
  const count = p.prediction_count != null ? ` · ${p.prediction_count.toLocaleString()} preds.` : "";
  return `${p.id.slice(0, 8)}… · ${date}${count}`;
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

const NS_LABELS: Record<string, string> = {
  BPO: "Biological Process",
  MFO: "Molecular Function",
  CCO: "Cellular Component",
};

const SETTING_COLORS: Record<string, string> = {
  NK: "bg-purple-50 border-purple-200",
  LK: "bg-blue-50 border-blue-200",
  PK: "bg-green-50 border-green-200",
};

const SETTING_TOOLTIPS: Record<string, string> = {
  NK: "No-Knowledge: proteins with no experimental annotations in any namespace at t0. Evaluated without -known.",
  LK: "Limited-Knowledge: proteins annotated in some namespaces at t0 but not in the evaluated namespace. Evaluated without -known.",
  PK: "Partial-Knowledge: proteins that already had annotations in the evaluated namespace at t0. Evaluated with -known (old terms excluded from scoring).",
};

function ResultsTable({ results }: { results: Record<string, SettingResults> }) {
  const settings = ["NK", "LK", "PK"].filter((s) => results[s] && Object.keys(results[s]).length > 0);
  if (settings.length === 0) return <p className="text-sm text-gray-400">No results computed.</p>;

  return (
    <div className="space-y-4">
      {settings.map((setting) => (
        <div key={setting} className={`rounded-lg border p-4 ${SETTING_COLORS[setting] ?? ""}`}>
          <div className="text-sm font-semibold text-gray-700 mb-3">
            {setting}
            {SETTING_TOOLTIPS[setting] && <InfoTooltip text={SETTING_TOOLTIPS[setting]} />}
          </div>
          <div className="grid grid-cols-3 gap-3">
            {["BPO", "MFO", "CCO"].map((ns) => {
              const m = results[setting]?.[ns];
              if (!m) return null;
              return (
                <div key={ns} className="rounded-md bg-white border border-gray-200 p-3">
                  <div className="text-xs font-medium text-gray-500 mb-2">{NS_LABELS[ns]}</div>
                  <div className="space-y-1 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-500">Fmax</span>
                      <span className="font-semibold text-gray-900">{m.fmax.toFixed(3)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">Precision</span>
                      <span className="text-gray-700">{m.precision.toFixed(3)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">Recall</span>
                      <span className="text-gray-700">{m.recall.toFixed(3)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">Coverage</span>
                      <span className="text-gray-700">{(m.coverage * 100).toFixed(1)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">τ</span>
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
  isSelected,
  onSelect,
  onDeleted,
}: {
  e: EvaluationSet;
  annotationSets: AnnotationSet[];
  predictionSets: PredictionSet[];
  scoringConfigs: ScoringConfig[];
  isSelected: boolean;
  onSelect: () => void;
  onDeleted: () => void;
}) {
  const [results, setResults] = useState<EvaluationResult[]>([]);
  const [loadingResults, setLoadingResults] = useState(false);
  const [predSetId, setPredSetId] = useState("");
  const [maxDistance, setMaxDistance] = useState("");
  const [scoringConfigId, setScoringConfigId] = useState("");
  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState("");
  // jobId is set after a successful submission so the user can navigate to
  // the Jobs page to track progress.  Cleared when results refresh.
  const [pendingJobId, setPendingJobId] = useState<string | null>(null);
  const [pollingResults, setPollingResults] = useState(false);

  useEffect(() => {
    if (!isSelected) return;
    setLoadingResults(true);
    listResults(e.id).then(setResults).finally(() => setLoadingResults(false));
  }, [isSelected, e.id]);

  // Poll for new results while a job is pending.  Stops automatically once a
  // new result appears or after a fixed number of attempts.
  useEffect(() => {
    if (!pendingJobId) return;

    setPollingResults(true);
    let attempts = 0;
    const MAX_ATTEMPTS = 30; // ~5 minutes at 10 s intervals

    const interval = setInterval(async () => {
      attempts++;
      try {
        const fresh = await listResults(e.id);
        if (fresh.length > results.length) {
          // A new result appeared — stop polling and refresh.
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
      if (scoringConfigId) body.scoring_config_id = scoringConfigId;
      const res = await apiFetch<{ id: string; status: string }>(
        `/annotations/evaluation-sets/${e.id}/run`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      // Store the job ID so we can show a link and start polling.
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
        <div className="flex items-center justify-between">
          <div className="text-sm font-medium text-gray-800">{evalLabel(e, annotationSets)}</div>
          <button
            onClick={async (ev) => {
              ev.stopPropagation();
              if (!confirm("Delete this evaluation set and all its results?")) return;
              await deleteEvaluationSet(e.id);
              onDeleted();
            }}
            className="rounded border border-red-200 px-2 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
          >
            Delete
          </button>
        </div>
        <div className="mt-3 grid grid-cols-4 gap-2">
          <StatBadge
            label="Delta proteins"
            value={e.stats.delta_proteins}
            tooltip="Proteins that gained ≥1 new experimental GO annotation between the old and new snapshot."
          />
          <StatBadge
            label="NK proteins"
            value={e.stats.nk_proteins}
            tooltip="No-Knowledge: proteins with no experimental annotations in any namespace at t0. All new annotations form the ground truth."
          />
          <StatBadge
            label="LK proteins"
            value={e.stats.lk_proteins}
            tooltip="Limited-Knowledge: proteins annotated in some namespaces at t0, but which gained new terms in a namespace where they had nothing. Only the new terms in that empty namespace are evaluated."
          />
          <StatBadge
            label="PK proteins"
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
              Ground truth files
              <InfoTooltip text="2-column TSV files (protein accession → GO term) used as input to the cafaeval evaluator. Each file contains only the novel experimental annotations for that category." />
            </p>
            <div className="flex flex-wrap gap-2">
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/ground-truth-NK.tsv`}
                label="NK"
                filename="ground_truth_NK.tsv"
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/ground-truth-LK.tsv`}
                label="LK"
                filename="ground_truth_LK.tsv"
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/ground-truth-PK.tsv`}
                label="PK"
                filename="ground_truth_PK.tsv"
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/known-terms.tsv`}
                label="Known terms"
                filename="known_terms.tsv"
              />
            </div>
            <p className="text-xs text-gray-400 mt-1">
              Known terms
              <InfoTooltip text="All experimental annotations from the old snapshot for PK proteins in the relevant namespace. Passed to cafaeval with -known to exclude them from scoring — this penalises methods that simply repeat prior annotations." />
              : passed to cafaeval as <code className="font-mono">-known</code> for the PK pass only.
            </p>
          </div>

          {/* FASTA downloads */}
          <div>
            <p className="text-xs font-medium text-gray-500 mb-2">
              Delta protein sequences (FASTA)
              <InfoTooltip text="Sequences of proteins that gained new experimental GO annotations (delta proteins). Download the full set or per-category subsets to compute embeddings and run predictions before evaluation." />
            </p>
            <div className="flex flex-wrap gap-2">
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/delta-proteins.fasta?category=all`}
                label="All delta (NK+LK+PK)"
                filename={`delta_proteins_all_${e.id.slice(0, 8)}.fasta`}
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/delta-proteins.fasta?category=nk`}
                label="NK only"
                filename={`delta_proteins_nk_${e.id.slice(0, 8)}.fasta`}
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/delta-proteins.fasta?category=lk`}
                label="LK only"
                filename={`delta_proteins_lk_${e.id.slice(0, 8)}.fasta`}
              />
              <DownloadLink
                href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/delta-proteins.fasta?category=pk`}
                label="PK only"
                filename={`delta_proteins_pk_${e.id.slice(0, 8)}.fasta`}
              />
            </div>
          </div>

          {/* Run evaluation */}
          <div className="space-y-3">
            <p className="text-xs font-medium text-gray-500">Run CAFA evaluator</p>
            <div className="grid grid-cols-3 gap-3">
              <div>
                <label className={labelClass}>Prediction set</label>
                <select
                  value={predSetId}
                  onChange={(ev) => setPredSetId(ev.target.value)}
                  className={selectClass}
                >
                  <option value="">— select —</option>
                  {predictionSets.map((p) => (
                    <option key={p.id} value={p.id}>{predLabel(p)}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className={labelClass}>
                  Scoring config (optional)
                  <InfoTooltip text="Scoring formula applied to compute CAFA prediction scores. If omitted, falls back to 1 − cosine_distance / 2." />
                </label>
                <select
                  value={scoringConfigId}
                  onChange={(ev) => setScoringConfigId(ev.target.value)}
                  className={selectClass}
                >
                  <option value="">— fallback (1−d/2) —</option>
                  {scoringConfigs.map((c) => (
                    <option key={c.id} value={c.id}>{c.name}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className={labelClass}>Max distance (optional)</label>
                <input
                  type="number" min="0" max="2" step="0.05" placeholder="no limit"
                  value={maxDistance}
                  onChange={(ev) => setMaxDistance(ev.target.value)}
                  className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>
            {runError && (
              <p className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
                {runError}
              </p>
            )}

            {/* Success banner — shown after a job is successfully submitted */}
            {pendingJobId && (
              <div className="rounded border border-blue-200 bg-blue-50 px-3 py-2.5 text-sm text-blue-800 flex items-center justify-between gap-3">
                <span>
                  Job queued.{" "}
                  {pollingResults
                    ? "Checking for results every 10 s…"
                    : "Results will appear below when the job completes."}
                </span>
                <a
                  href={`/jobs/${pendingJobId}`}
                  className="shrink-0 rounded border border-blue-300 bg-white px-2 py-1 text-xs font-medium text-blue-700 hover:bg-blue-100 transition-colors"
                >
                  View job →
                </a>
              </div>
            )}

            <button
              onClick={handleRun}
              disabled={!predSetId || running}
              className={btnPrimary}
            >
              {running ? "Queuing…" : "Run Evaluation (NK + LK + PK)"}
            </button>
          </div>

          {/* Results */}
          <div>
            <div className="flex items-center justify-between mb-3">
              <p className="text-xs font-medium text-gray-500">
                Results
                {pollingResults && (
                  <span className="ml-2 text-blue-500 animate-pulse">● polling</span>
                )}
              </p>
              <button
                onClick={refreshResults}
                disabled={loadingResults}
                className="text-xs text-gray-400 hover:text-gray-700 border rounded px-2 py-0.5 disabled:opacity-40"
              >
                {loadingResults ? "Refreshing…" : "↻ Refresh"}
              </button>
            </div>
            {loadingResults ? (
              <p className="text-sm text-gray-400">Loading…</p>
            ) : results.length === 0 ? (
              <p className="text-sm text-gray-400">No evaluations run yet.</p>
            ) : (
              <div className="space-y-6">
                {results.map((r) => {
                  const pred = predictionSets.find((p) => p.id === r.prediction_set_id);
                  const sc = scoringConfigs.find((c) => c.id === r.scoring_config_id);
                  return (
                    <div key={r.id} className="rounded-lg border border-gray-200 bg-white p-4 space-y-3">
                      {/* Meta header */}
                      <div className="flex items-start justify-between gap-2">
                        <div className="space-y-0.5 text-xs text-gray-500 min-w-0">
                          <div>
                            <span className="font-medium text-gray-700">Prediction set: </span>
                            {pred
                              ? <span title={r.prediction_set_id}>{r.prediction_set_id.slice(0, 8)}… · {new Date(pred.created_at).toLocaleDateString()}{pred.prediction_count != null ? ` · ${pred.prediction_count.toLocaleString()} preds.` : ""}</span>
                              : <span className="font-mono">{r.prediction_set_id.slice(0, 8)}…</span>
                            }
                          </div>
                          <div>
                            <span className="font-medium text-gray-700">Scoring: </span>
                            {sc ? sc.name : <span className="italic text-gray-400">fallback (1−d/2)</span>}
                            {sc?.description && <InfoTooltip text={sc.description} />}
                          </div>
                          <div className="text-gray-400">{new Date(r.created_at).toLocaleString()}</div>
                        </div>
                        <div className="flex items-center gap-2 shrink-0">
                          <a
                            href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/results/${r.id}/artifacts.zip`}
                            download={`cafaeval_${r.id.slice(0, 8)}.zip`}
                            className="inline-flex items-center gap-1 rounded-md border border-gray-300 bg-white px-3 py-1 text-xs hover:bg-gray-50 transition-colors"
                          >
                            ↓ Artifacts (.zip)
                          </a>
                          <button
                            onClick={async () => {
                              if (!confirm("Delete this evaluation result?")) return;
                              const res = await fetch(`${baseUrl()}/annotations/evaluation-sets/${e.id}/results/${r.id}`, { method: "DELETE" });
                              if (!res.ok) throw new Error(await res.text());
                              setResults((prev) => prev.filter((x) => x.id !== r.id));
                            }}
                            className="rounded-md border border-red-200 bg-white px-3 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
                          >
                            Delete
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
  const [annotationSets, setAnnotationSets] = useState<AnnotationSet[]>([]);
  const [predictionSets, setPredictionSets] = useState<PredictionSet[]>([]);
  const [evaluationSets, setEvaluationSets] = useState<EvaluationSet[]>([]);
  const [scoringConfigs, setScoringConfigs] = useState<ScoringConfig[]>([]);
  const [loading, setLoading] = useState(true);

  const [oldSetId, setOldSetId] = useState("");
  const [newSetId, setNewSetId] = useState("");
  const [generating, setGenerating] = useState(false);
  const [genError, setGenError] = useState("");
  const [selectedEvalId, setSelectedEvalId] = useState("");

  const reload = () =>
    Promise.all([listAnnotationSets(), listPredictionSets(), listEvaluationSets(), listScoringConfigs()])
      .then(([ann, pred, ev, sc]) => {
        setAnnotationSets(ann);
        setPredictionSets(pred);
        setEvaluationSets(ev);
        setScoringConfigs(sc);
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
    <div className="p-8 max-w-3xl space-y-10">
      <h1 className="text-xl font-semibold text-gray-900">CAFA Evaluation</h1>

      {/* ── Generate Evaluation Set ───────────────────────────────── */}
      <section className="rounded-lg border border-gray-200 p-6 space-y-5">
        <div>
          <h2 className="text-base font-semibold text-gray-800">New Evaluation Set</h2>
          <p className="mt-1 text-sm text-gray-500">
            Computes the delta between two GOA releases. Applies experimental evidence
            filtering and NOT-qualifier propagation through the GO DAG.
          </p>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className={labelClass}>Old GOA set (reference)</label>
            <select value={oldSetId} onChange={(e) => setOldSetId(e.target.value)} className={selectClass}>
              <option value="">— select —</option>
              {goaSets.map((s) => (
                <option key={s.id} value={s.id}>{setLabel(s)}</option>
              ))}
            </select>
          </div>
          <div>
            <label className={labelClass}>New GOA set (ground truth)</label>
            <select value={newSetId} onChange={(e) => setNewSetId(e.target.value)} className={selectClass}>
              <option value="">— select —</option>
              {goaSets.map((s) => (
                <option key={s.id} value={s.id}>{setLabel(s)}</option>
              ))}
            </select>
          </div>
        </div>
        {oldSetId && newSetId && oldSetId === newSetId && (
          <p className="text-xs text-red-500">Old and new sets must be different.</p>
        )}
        {genError && (
          <p className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{genError}</p>
        )}
        <button onClick={handleGenerate} disabled={!canGenerate || generating} className={btnPrimary}>
          {generating ? "Queuing…" : "Generate Evaluation Set"}
        </button>
      </section>

      {/* ── Evaluation Sets ───────────────────────────────────────── */}
      {evaluationSets.length > 0 && (
        <section className="space-y-4">
          <h2 className="text-base font-semibold text-gray-800">Evaluation Sets</h2>
          {evaluationSets.map((e) => (
            <EvaluationSetCard
              key={e.id}
              e={e}
              annotationSets={annotationSets}
              predictionSets={predictionSets}
              scoringConfigs={scoringConfigs}
              isSelected={selectedEvalId === e.id}
              onSelect={() => setSelectedEvalId(e.id === selectedEvalId ? "" : e.id)}
              onDeleted={() => setEvaluationSets((prev) => prev.filter((x) => x.id !== e.id))}
            />
          ))}
        </section>
      )}

      {/* ── Evaluator command reference ───────────────────────────── */}
      <section className="rounded-lg border border-gray-100 bg-gray-50 p-5">
        <h2 className="text-sm font-semibold text-gray-700 mb-2">Manual evaluator command</h2>
        <pre className="text-xs text-gray-600 overflow-x-auto whitespace-pre-wrap leading-relaxed">
{`python -m cafaeval go-basic.obo predictions/ ground_truth_NK.tsv -out_dir results/NK
python -m cafaeval go-basic.obo predictions/ ground_truth_LK.tsv -out_dir results/LK
python -m cafaeval go-basic.obo predictions/ ground_truth_PK.tsv -known known_terms.tsv -out_dir results/PK`}
        </pre>
      </section>
    </div>
  );
}
