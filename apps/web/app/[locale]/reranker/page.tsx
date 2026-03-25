"use client";

import { useEffect, useState } from "react";
import { ContextBanner } from "@/components/ContextBanner";
import {
  baseUrl,
  listPredictionSets,
  listAnnotationSets,
  listRerankers,
  trainReranker,
  deleteReranker,
  getRerankedTsvUrl,
  getRerankerMetrics,
  getTrainingDataTsvUrl,
} from "@/lib/api";
import type { PredictionSet, AnnotationSet, RerankerModel } from "@/lib/api";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${baseUrl()}${path}`, { cache: "no-store", ...init });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

type EvaluationSet = {
  id: string;
  old_annotation_set_id: string;
  new_annotation_set_id: string;
  created_at: string;
  stats: Record<string, number>;
};

const listEvaluationSets = () => apiFetch<EvaluationSet[]>("/annotations/evaluation-sets");

function shortId(id: string) { return id.slice(0, 8); }

function predLabel(p: PredictionSet) {
  const parts: string[] = [];
  if (p.embedding_config_name) parts.push(p.embedding_config_name);
  if (p.annotation_set_label) parts.push(p.annotation_set_label);
  parts.push(`k=${p.limit_per_entry}`);
  if (p.prediction_count != null) parts.push(`${p.prediction_count.toLocaleString()} preds`);
  return `${parts.join(" · ")} (${shortId(p.id)}…)`;
}

function evalLabel(es: EvaluationSet, annotationSets: AnnotationSet[]) {
  const oldSet = annotationSets.find((a) => a.id === es.old_annotation_set_id);
  const newSet = annotationSets.find((a) => a.id === es.new_annotation_set_id);
  const oldVer = oldSet ? `[${oldSet.source.toUpperCase()}] ${oldSet.source_version ?? "?"}` : shortId(es.old_annotation_set_id);
  const newVer = newSet ? `[${newSet.source.toUpperCase()}] ${newSet.source_version ?? "?"}` : shortId(es.new_annotation_set_id);
  const delta = es.stats.delta_proteins ?? "?";
  return `${oldVer} → ${newVer} · ${delta} delta proteins (${shortId(es.id)}…)`;
}

const labelClass = "block text-sm font-medium text-gray-700 mb-1";
const selectClass =
  "w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
const btnPrimary =
  "rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 transition-colors";
const btnDanger =
  "rounded-md bg-red-50 border border-red-200 px-3 py-1.5 text-xs font-medium text-red-600 hover:bg-red-100 transition-colors";

const CATEGORY_HINTS: Record<string, string> = {
  nk: "No Knowledge: proteins with zero GO annotations at t0. Hardest setting — measures pure prediction ability.",
  lk: "Limited Knowledge: proteins annotated in some GO namespaces but not all at t0. New annotations in previously empty namespaces.",
  pk: "Partial Knowledge: proteins that already had annotations in a namespace at t0 and gained new ones at t1.",
};

const ASPECT_LABELS: Record<string, string> = {
  bpo: "BPO (Biological Process)",
  mfo: "MFO (Molecular Function)",
  cco: "CCO (Cellular Component)",
};

// ---------------------------------------------------------------------------
// Feature importance bar chart
// ---------------------------------------------------------------------------

function FeatureImportanceChart({ importance }: { importance: Record<string, number> }) {
  const entries = Object.entries(importance)
    .sort(([, a], [, b]) => b - a)
    .filter(([, v]) => v > 0);
  if (entries.length === 0) return <p className="text-xs text-gray-400">No feature importance data</p>;
  const maxVal = entries[0][1];

  return (
    <div className="space-y-1">
      {entries.map(([name, val]) => (
        <div key={name} className="flex items-center gap-2 text-xs">
          <span className="w-40 shrink-0 text-right text-gray-600 truncate" title={name}>{name}</span>
          <div className="flex-1 h-4 bg-gray-100 rounded overflow-hidden">
            <div
              className="h-4 bg-blue-400 rounded"
              style={{ width: `${Math.round((val / maxVal) * 100)}%` }}
            />
          </div>
          <span className="w-20 shrink-0 font-mono text-gray-500 text-right">
            {val >= 1000 ? `${(val / 1000).toFixed(1)}k` : val.toFixed(0)}
          </span>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Metrics display
// ---------------------------------------------------------------------------

function MetricsBadge({ label, value, suffix }: { label: string; value: number | string | undefined; suffix?: string }) {
  if (value === undefined) return null;
  const formatted = typeof value === "number" ? value.toFixed(4) : value;
  return (
    <div className="rounded-lg border bg-white p-3 shadow-sm text-center">
      <p className="text-[10px] font-semibold uppercase tracking-wide text-gray-400">{label}</p>
      <p className="text-lg font-bold text-gray-900 mt-0.5">{formatted}{suffix}</p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Reranker card
// ---------------------------------------------------------------------------

function RerankerCard({
  model,
  predictionSets,
  evaluationSets,
  annotationSets,
  onDelete,
}: {
  model: RerankerModel;
  predictionSets: PredictionSet[];
  evaluationSets: EvaluationSet[];
  annotationSets: AnnotationSet[];
  onDelete: () => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [metrics, setMetrics] = useState<Record<string, any> | null>(null);
  const [metricsError, setMetricsError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);

  // For computing metrics on a different prediction set
  const [metricsPsId, setMetricsPsId] = useState(model.prediction_set_id ?? "");
  const [metricsEsId, setMetricsEsId] = useState(model.evaluation_set_id ?? "");
  const [metricsCategory, setMetricsCategory] = useState(model.category);

  async function handleComputeMetrics() {
    if (!metricsPsId || !metricsEsId) return;
    setMetricsLoading(true);
    setMetricsError(null);
    setMetrics(null);
    try {
      const result = await getRerankerMetrics(metricsPsId, model.id, metricsEsId, metricsCategory);
      setMetrics(result);
    } catch (e: any) {
      setMetricsError(e.message ?? "Failed to compute metrics");
    } finally {
      setMetricsLoading(false);
    }
  }

  async function handleDelete() {
    if (!confirm(`Delete reranker "${model.name}"?`)) return;
    setDeleting(true);
    try {
      await deleteReranker(model.id);
      onDelete();
    } catch {
      setDeleting(false);
    }
  }

  const m = model.metrics;

  return (
    <div className="rounded-lg border bg-white shadow-sm overflow-hidden">
      <div
        className="px-4 py-3 cursor-pointer hover:bg-gray-50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <span className="font-semibold text-gray-900">{model.name}</span>
            <span className="rounded-full border px-2 py-0.5 text-xs font-medium bg-indigo-50 text-indigo-700 border-indigo-100 uppercase">
              {model.category}
            </span>
            {model.aspect && (
              <span className="rounded-full border px-2 py-0.5 text-xs font-medium bg-amber-50 text-amber-700 border-amber-100 uppercase">
                {model.aspect}
              </span>
            )}
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-400">{new Date(model.created_at).toLocaleDateString()}</span>
            <span className="text-gray-300 text-xs">{expanded ? "▲" : "▼"}</span>
          </div>
        </div>
        <div className="flex flex-wrap gap-4 mt-2 text-xs text-gray-500">
          <span>AUC: <strong className="text-gray-700">{m.val_auc?.toFixed(4) ?? "—"}</strong></span>
          <span>F1: <strong className="text-gray-700">{m.val_f1?.toFixed(4) ?? "—"}</strong></span>
          <span>Precision: <strong className="text-gray-700">{m.val_precision?.toFixed(4) ?? "—"}</strong></span>
          <span>Recall: <strong className="text-gray-700">{m.val_recall?.toFixed(4) ?? "—"}</strong></span>
          <span>Positive rate: <strong className="text-gray-700">{m.positive_rate != null ? `${(m.positive_rate * 100).toFixed(2)}%` : "—"}</strong></span>
        </div>
      </div>

      {expanded && (
        <div className="border-t px-4 py-4 space-y-5">
          {/* Validation metrics */}
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-2">Validation metrics</p>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
              <MetricsBadge label="AUC" value={m.val_auc} />
              <MetricsBadge label="Log-loss" value={m.val_logloss} />
              <MetricsBadge label="F1" value={m.val_f1} />
              <MetricsBadge label="Best iteration" value={m.best_iteration} />
            </div>
            <div className="flex flex-wrap gap-4 mt-2 text-xs text-gray-500">
              <span>Train samples: {m.train_samples?.toLocaleString()}</span>
              <span>Val samples: {m.val_samples?.toLocaleString()}</span>
            </div>
          </div>

          {/* Feature importance */}
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-2">Feature importance (gain)</p>
            <FeatureImportanceChart importance={model.feature_importance} />
          </div>

          {/* Download reranked TSV */}
          {model.prediction_set_id && (
            <div>
              <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-2">Download re-ranked predictions</p>
              <a
                href={getRerankedTsvUrl(model.prediction_set_id, model.id)}
                download={`reranked_${shortId(model.id)}.tsv`}
                className="inline-flex items-center gap-1.5 rounded-md border bg-white px-3 py-1.5 text-sm font-medium text-gray-700 shadow-sm hover:bg-gray-50"
              >
                ↓ Download reranked TSV
              </a>
            </div>
          )}

          {/* Compute CAFA metrics */}
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-2">Compute CAFA metrics</p>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 mb-2">
              <div>
                <label className="text-xs text-gray-500 mb-0.5 block">Prediction set</label>
                <select value={metricsPsId} onChange={(e) => setMetricsPsId(e.target.value)} className={selectClass}>
                  <option value="">Select...</option>
                  {predictionSets.map((ps) => (
                    <option key={ps.id} value={ps.id}>{predLabel(ps)}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="text-xs text-gray-500 mb-0.5 block">Evaluation set</label>
                <select value={metricsEsId} onChange={(e) => setMetricsEsId(e.target.value)} className={selectClass}>
                  <option value="">Select...</option>
                  {evaluationSets.map((es) => (
                    <option key={es.id} value={es.id}>{evalLabel(es, annotationSets)}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="text-xs text-gray-500 mb-0.5 block">Category</label>
                <select value={metricsCategory} onChange={(e) => setMetricsCategory(e.target.value)} className={selectClass}>
                  <option value="nk">NK (No Knowledge)</option>
                  <option value="lk">LK (Limited Knowledge)</option>
                  <option value="pk">PK (Partial Knowledge)</option>
                </select>
                <p className="text-[10px] text-gray-400 mt-1 leading-snug">{CATEGORY_HINTS[metricsCategory]}</p>
              </div>
            </div>
            <button
              onClick={handleComputeMetrics}
              disabled={!metricsPsId || !metricsEsId || metricsLoading}
              className={btnPrimary}
            >
              {metricsLoading ? "Computing... (this may take 30-60s)" : "Compute metrics"}
            </button>
            {metricsError && <p className="text-xs text-red-500 mt-2">{metricsError}</p>}
            {metrics && (
              <div className="mt-3 rounded-md border bg-gray-50 p-3">
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2">
                  <MetricsBadge label="Fmax" value={metrics.fmax} />
                  <MetricsBadge label="AUC-PR" value={metrics.auc_pr} />
                  <MetricsBadge label="Threshold" value={metrics.threshold_at_fmax} />
                  <MetricsBadge label="GT proteins" value={metrics.n_ground_truth_proteins} />
                  <MetricsBadge label="Pred. proteins" value={metrics.n_predicted_proteins} />
                  <MetricsBadge label="Predictions" value={metrics.n_predictions} />
                </div>
                {metrics.curve && metrics.curve.length > 0 && (
                  <p className="text-[10px] text-gray-400 mt-2">{metrics.curve.length} PR curve points computed</p>
                )}
              </div>
            )}
          </div>

          {/* Source info */}
          <div className="flex flex-wrap gap-4 text-xs text-gray-400 border-t pt-3">
            <span>Prediction set: <span className="font-mono">{model.prediction_set_id ? shortId(model.prediction_set_id) : "—"}</span></span>
            <span>Evaluation set: <span className="font-mono">{model.evaluation_set_id ? shortId(model.evaluation_set_id) : "—"}</span></span>
            <span>ID: <span className="font-mono">{shortId(model.id)}</span></span>
          </div>

          {/* Delete */}
          <div className="border-t pt-3">
            <button onClick={handleDelete} disabled={deleting} className={btnDanger}>
              {deleting ? "Deleting..." : "Delete reranker"}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function RerankerPage() {
  const [rerankers, setRerankers] = useState<RerankerModel[]>([]);
  const [predictionSets, setPredictionSets] = useState<PredictionSet[]>([]);
  const [evaluationSets, setEvaluationSets] = useState<EvaluationSet[]>([]);
  const [annotationSets, setAnnotationSets] = useState<AnnotationSet[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Train form
  const [trainName, setTrainName] = useState("");
  const [trainPsId, setTrainPsId] = useState("");
  const [trainEsId, setTrainEsId] = useState("");
  const [trainCategory, setTrainCategory] = useState("nk");
  const [trainAspect, setTrainAspect] = useState("");
  const [trainNegPosRatio, setTrainNegPosRatio] = useState("");
  const [extraPairs, setExtraPairs] = useState<{ psId: string; esId: string }[]>([]);
  const [training, setTraining] = useState(false);
  const [trainError, setTrainError] = useState<string | null>(null);

  async function loadAll() {
    setLoading(true);
    setError(null);
    try {
      const [r, ps, es, as_] = await Promise.all([
        listRerankers(),
        listPredictionSets(),
        listEvaluationSets(),
        listAnnotationSets(),
      ]);
      setRerankers(r);
      setPredictionSets(ps);
      setEvaluationSets(es);
      setAnnotationSets(as_);
    } catch (e: any) {
      setError(e.message ?? "Failed to load data");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadAll(); }, []);

  async function handleTrain() {
    if (!trainName.trim() || !trainPsId || !trainEsId) return;
    setTraining(true);
    setTrainError(null);
    try {
      const validExtraPairs = extraPairs
        .filter((p) => p.psId && p.esId)
        .map((p) => ({ prediction_set_id: p.psId, evaluation_set_id: p.esId }));
      const model = await trainReranker({
        name: trainName.trim(),
        prediction_set_id: trainPsId,
        evaluation_set_id: trainEsId,
        category: trainCategory,
        aspect: trainAspect || null,
        neg_pos_ratio: trainNegPosRatio ? parseFloat(trainNegPosRatio) : null,
        extra_pairs: validExtraPairs.length > 0 ? validExtraPairs : undefined,
      });
      setRerankers((prev) => [...prev, model]);
      setTrainName("");
    } catch (e: any) {
      setTrainError(e.message ?? "Training failed");
    } finally {
      setTraining(false);
    }
  }

  return (
    <>
      <h1 className="text-xl font-semibold mb-1">Re-ranker Models</h1>

      <ContextBanner
        title="Train a LightGBM model to re-rank KNN predictions"
        description="Uses features like alignment scores, taxonomic distance, and embedding similarity to learn an optimal ranking. Requires a prediction set and evaluation set for training."
        prerequisites={[
          { label: `${predictionSets.length} prediction set(s)`, met: predictionSets.length > 0, href: "/functional-annotation" },
          { label: `${evaluationSets.length} evaluation set(s)`, met: evaluationSets.length > 0, href: "/evaluation" },
        ]}
        nextStep={{ label: "Evaluation", href: "/evaluation" }}
      />
      <p className="text-sm text-gray-500 mb-6">
        LightGBM binary classifiers trained on temporal holdout data (CAFA protocol).
        A re-ranker uses alignment, taxonomy, and aggregate features to re-score GO predictions
        with calibrated probabilities, replacing the raw embedding distance ranking.
      </p>

      {/* Train new reranker */}
      <div className="rounded-lg border bg-white p-5 shadow-sm mb-6">
        <h2 className="text-sm font-semibold text-gray-700 mb-4">Train new re-ranker</h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-6 gap-3 mb-3">
          <div>
            <label className={labelClass}>Name</label>
            <input
              type="text"
              value={trainName}
              onChange={(e) => setTrainName(e.target.value)}
              placeholder="e.g. reranker-nk-bpo-v1"
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className={labelClass}>Prediction set</label>
            <select value={trainPsId} onChange={(e) => setTrainPsId(e.target.value)} className={selectClass}>
              <option value="">Select...</option>
              {predictionSets.map((ps) => (
                <option key={ps.id} value={ps.id}>{predLabel(ps)}</option>
              ))}
            </select>
          </div>
          <div>
            <label className={labelClass}>Evaluation set</label>
            <select value={trainEsId} onChange={(e) => setTrainEsId(e.target.value)} className={selectClass}>
              <option value="">Select...</option>
              {evaluationSets.map((es) => (
                <option key={es.id} value={es.id}>{evalLabel(es, annotationSets)}</option>
              ))}
            </select>
          </div>
          <div>
            <label className={labelClass}>Category</label>
            <select value={trainCategory} onChange={(e) => setTrainCategory(e.target.value)} className={selectClass}>
              <option value="nk">NK (No Knowledge)</option>
              <option value="lk">LK (Limited Knowledge)</option>
              <option value="pk">PK (Partial Knowledge)</option>
            </select>
            <p className="text-[10px] text-gray-400 mt-1 leading-snug">{CATEGORY_HINTS[trainCategory]}</p>
          </div>
          <div>
            <label className={labelClass}>Aspect</label>
            <select value={trainAspect} onChange={(e) => setTrainAspect(e.target.value)} className={selectClass}>
              <option value="">All aspects</option>
              <option value="bpo">BPO (Biological Process)</option>
              <option value="mfo">MFO (Molecular Function)</option>
              <option value="cco">CCO (Cellular Component)</option>
            </select>
          </div>
          <div>
            <label className={labelClass}>Neg:Pos ratio</label>
            <input
              type="number" min="1" step="1" placeholder="all (no limit)"
              value={trainNegPosRatio}
              onChange={(e) => setTrainNegPosRatio(e.target.value)}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        {/* Extra training pairs */}
        <div className="mb-3">
          <div className="flex items-center gap-2 mb-2">
            <label className="text-xs font-medium text-gray-600">Additional training pairs (multi-temporal)</label>
            <button
              type="button"
              onClick={() => setExtraPairs((prev) => [...prev, { psId: "", esId: "" }])}
              className="rounded border border-gray-300 bg-white px-2 py-0.5 text-xs text-gray-600 hover:bg-gray-50"
            >
              + Add pair
            </button>
          </div>
          {extraPairs.map((pair, i) => (
            <div key={i} className="grid grid-cols-[1fr_1fr_auto] gap-2 mb-1.5">
              <select
                value={pair.psId}
                onChange={(e) => setExtraPairs((prev) => prev.map((p, j) => j === i ? { ...p, psId: e.target.value } : p))}
                className={selectClass}
              >
                <option value="">Prediction set...</option>
                {predictionSets.map((ps) => (
                  <option key={ps.id} value={ps.id}>{predLabel(ps)}</option>
                ))}
              </select>
              <select
                value={pair.esId}
                onChange={(e) => setExtraPairs((prev) => prev.map((p, j) => j === i ? { ...p, esId: e.target.value } : p))}
                className={selectClass}
              >
                <option value="">Evaluation set...</option>
                {evaluationSets.map((es) => (
                  <option key={es.id} value={es.id}>{evalLabel(es, annotationSets)}</option>
                ))}
              </select>
              <button
                type="button"
                onClick={() => setExtraPairs((prev) => prev.filter((_, j) => j !== i))}
                className="rounded border border-red-200 px-2 py-1 text-xs text-red-500 hover:bg-red-50"
              >
                x
              </button>
            </div>
          ))}
          {extraPairs.length > 0 && (
            <p className="text-[10px] text-gray-400 mt-1">
              Data from all pairs will be concatenated before training a single model.
              {extraPairs.filter((p) => p.psId && p.esId).length > 0 &&
                ` (${1 + extraPairs.filter((p) => p.psId && p.esId).length} pairs total)`}
            </p>
          )}
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={handleTrain}
            disabled={!trainName.trim() || !trainPsId || !trainEsId || training}
            className={btnPrimary}
          >
            {training ? "Training… (this may take 1-2 min)" : "Train"}
          </button>
          {trainPsId && trainEsId && (
            <a
              href={getTrainingDataTsvUrl(trainPsId, trainEsId, trainCategory)}
              download={`training_data_${shortId(trainPsId)}_${trainCategory}.tsv`}
              className="text-xs text-blue-600 hover:underline"
            >
              ↓ Preview training data TSV
            </a>
          )}
        </div>
        {trainError && <p className="text-xs text-red-500 mt-2">{trainError}</p>}
      </div>

      {/* List of rerankers */}
      {loading && <p className="text-sm text-gray-400">Loading...</p>}
      {error && <p className="text-sm text-red-500">{error}</p>}

      {!loading && rerankers.length === 0 && (
        <div className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-gray-400 shadow-sm">
          No re-ranker models trained yet. Use the form above to train one.
        </div>
      )}

      <div className="space-y-3">
        {rerankers.map((model) => (
          <RerankerCard
            key={model.id}
            model={model}
            predictionSets={predictionSets}
            evaluationSets={evaluationSets}
            annotationSets={annotationSets}
            onDelete={() => setRerankers((prev) => prev.filter((r) => r.id !== model.id))}
          />
        ))}
      </div>
    </>
  );
}
