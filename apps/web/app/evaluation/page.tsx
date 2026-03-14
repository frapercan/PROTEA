"use client";

import { useEffect, useState } from "react";
import { listAnnotationSets, listPredictionSets, baseUrl } from "@/lib/api";
import type { AnnotationSet, PredictionSet } from "@/lib/api";

const labelClass = "block text-sm font-medium text-gray-700 mb-1";
const selectClass =
  "w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
const btnPrimary =
  "rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 transition-colors";
const btnSecondary =
  "rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50 transition-colors";

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
    nk_annotations?: number;
    lk_annotations?: number;
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

function StatBadge({ label, value }: { label: string; value: number | undefined }) {
  return (
    <div className="rounded-lg border border-gray-200 bg-gray-50 px-4 py-3 text-center">
      <div className="text-lg font-semibold text-gray-900">
        {value != null ? value.toLocaleString() : "—"}
      </div>
      <div className="text-xs text-gray-500 mt-0.5">{label}</div>
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

function ResultsTable({ results }: { results: Record<string, SettingResults> }) {
  const settings = ["NK", "LK", "PK"].filter((s) => results[s] && Object.keys(results[s]).length > 0);
  if (settings.length === 0) return <p className="text-sm text-gray-400">No results computed.</p>;

  return (
    <div className="space-y-4">
      {settings.map((setting) => (
        <div key={setting} className={`rounded-lg border p-4 ${SETTING_COLORS[setting] ?? ""}`}>
          <div className="text-sm font-semibold text-gray-700 mb-3">{setting}</div>
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
  isSelected,
  onSelect,
}: {
  e: EvaluationSet;
  annotationSets: AnnotationSet[];
  predictionSets: PredictionSet[];
  isSelected: boolean;
  onSelect: () => void;
}) {
  const [results, setResults] = useState<EvaluationResult[]>([]);
  const [loadingResults, setLoadingResults] = useState(false);
  const [predSetId, setPredSetId] = useState("");
  const [maxDistance, setMaxDistance] = useState("");
  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState("");

  useEffect(() => {
    if (!isSelected) return;
    setLoadingResults(true);
    listResults(e.id).then(setResults).finally(() => setLoadingResults(false));
  }, [isSelected, e.id]);

  async function handleRun() {
    if (!predSetId) return;
    setRunning(true);
    setRunError("");
    try {
      const body: Record<string, any> = { prediction_set_id: predSetId };
      if (maxDistance) body.max_distance = parseFloat(maxDistance);
      await apiFetch(`/annotations/evaluation-sets/${e.id}/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      // Refresh results after a short delay
      setTimeout(() => {
        listResults(e.id).then(setResults);
      }, 2000);
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
        <div className="text-sm font-medium text-gray-800">{evalLabel(e, annotationSets)}</div>
        <div className="mt-3 grid grid-cols-3 gap-2">
          <StatBadge label="Delta proteins" value={e.stats.delta_proteins} />
          <StatBadge label="NK proteins" value={e.stats.nk_proteins} />
          <StatBadge label="LK proteins" value={e.stats.lk_proteins} />
        </div>
      </div>

      {isSelected && (
        <div className="border-t border-gray-200 p-4 space-y-5 bg-gray-50 rounded-b-lg">

          {/* Downloads */}
          <div>
            <p className="text-xs font-medium text-gray-500 mb-2">Ground truth files</p>
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
          </div>

          {/* Run evaluation */}
          <div className="space-y-3">
            <p className="text-xs font-medium text-gray-500">Run CAFA evaluator</p>
            <div className="grid grid-cols-2 gap-3">
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
              <p className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{runError}</p>
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
            <p className="text-xs font-medium text-gray-500 mb-3">Results</p>
            {loadingResults ? (
              <p className="text-sm text-gray-400">Loading…</p>
            ) : results.length === 0 ? (
              <p className="text-sm text-gray-400">No evaluations run yet.</p>
            ) : (
              <div className="space-y-6">
                {results.map((r) => (
                  <div key={r.id}>
                    <div className="flex items-center justify-between mb-2">
                      <div className="text-xs text-gray-400">
                        Pred: {r.prediction_set_id.slice(0, 8)}… · {new Date(r.created_at).toLocaleString()}
                      </div>
                      <a
                        href={`${baseUrl()}/annotations/evaluation-sets/${e.id}/results/${r.id}/artifacts.zip`}
                        download={`cafaeval_${r.id.slice(0, 8)}.zip`}
                        className="inline-flex items-center gap-1 rounded-md border border-gray-300 bg-white px-3 py-1 text-xs hover:bg-gray-50 transition-colors"
                      >
                        ↓ Artifacts (.zip)
                      </a>
                    </div>
                    <ResultsTable results={r.results} />
                  </div>
                ))}
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
  const [loading, setLoading] = useState(true);

  const [oldSetId, setOldSetId] = useState("");
  const [newSetId, setNewSetId] = useState("");
  const [generating, setGenerating] = useState(false);
  const [genError, setGenError] = useState("");
  const [selectedEvalId, setSelectedEvalId] = useState("");

  const reload = () =>
    Promise.all([listAnnotationSets(), listPredictionSets(), listEvaluationSets()])
      .then(([ann, pred, ev]) => {
        setAnnotationSets(ann);
        setPredictionSets(pred);
        setEvaluationSets(ev);
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
              isSelected={selectedEvalId === e.id}
              onSelect={() => setSelectedEvalId(e.id === selectedEvalId ? "" : e.id)}
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
