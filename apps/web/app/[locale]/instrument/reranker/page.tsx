"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { ContextBanner } from "@/components/ContextBanner";
import { SkeletonTableRow } from "@/components/Skeleton";
import { Tooltip } from "@/components/Tooltip";
import { ImportRerankerDialog } from "@/components/ImportRerankerDialog";
import { RegisterRerankerDialog } from "@/components/RegisterRerankerDialog";
import {
  baseUrl,
  listPredictionSets,
  listAnnotationSets,
  listRerankers,
  deleteReranker,
  getRerankedTsvUrl,
  getRerankerMetrics,
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

const selectClass =
  "w-full rounded-md border border-slate-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
const btnPrimary =
  "rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 transition-colors";
const btnDanger =
  "rounded-md bg-red-50 border border-red-200 px-3 py-1.5 text-xs font-medium text-red-600 hover:bg-red-100 transition-colors";

const CATEGORY_HINTS: Record<string, string> = {
  nk: "No Knowledge: proteins with zero GO annotations at t0. Hardest setting, measures pure prediction ability.",
  lk: "Limited Knowledge: proteins annotated in some GO namespaces but not all at t0. New annotations in previously empty namespaces.",
  pk: "Partial Knowledge: proteins that already had annotations in a namespace at t0 and gained new ones at t1.",
};

// ---------------------------------------------------------------------------
// Feature importance bar chart
// ---------------------------------------------------------------------------

function FeatureImportanceChart({ importance }: { importance: Record<string, number> }) {
  const entries = Object.entries(importance)
    .sort(([, a], [, b]) => b - a)
    .filter(([, v]) => v > 0);
  if (entries.length === 0) return <p className="text-xs text-slate-600">No feature importance data</p>;
  const maxVal = entries[0][1];

  return (
    <div className="space-y-1">
      {entries.map(([name, val]) => (
        <div key={name} className="flex items-center gap-2 text-xs">
          <span className="w-40 shrink-0 text-right text-slate-600 truncate" title={name}>{name}</span>
          <div className="flex-1 h-4 bg-slate-100 rounded overflow-hidden">
            <div
              className="h-4 bg-blue-400 rounded"
              style={{ width: `${Math.round((val / maxVal) * 100)}%` }}
            />
          </div>
          <span className="w-20 shrink-0 font-mono text-slate-500 text-right">
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
      <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-600">{label}</p>
      <p className="text-lg font-bold text-slate-900 mt-0.5">{formatted}{suffix}</p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Provenance / feature-selection panel
// ---------------------------------------------------------------------------

// The 56-feature champion reranker (v27-binary, NK+LK) relies on the lineage
// feature family being present; we highlight it so a model can be checked
// against that reference at a glance.
const LINEAGE_FAMILIES = new Set(["lineage", "anc2vec_query", "anc2vec_neighbor", "go_context"]);

function CopyableSha({ value }: { value: string | null | undefined }) {
  const [copied, setCopied] = useState(false);
  if (!value) return <span className="font-mono text-slate-400">—</span>;
  const short = value.length > 12 ? `${value.slice(0, 12)}…` : value;
  return (
    <button
      type="button"
      title={`${value} (click to copy)`}
      onClick={() => {
        navigator.clipboard?.writeText(value).then(
          () => {
            setCopied(true);
            setTimeout(() => setCopied(false), 1200);
          },
          () => {},
        );
      }}
      className="inline-flex items-center gap-1 rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[12px] text-slate-700 hover:bg-slate-200 transition-colors"
    >
      {short}
      <span className="text-[10px] text-slate-400">{copied ? "✓" : "⧉"}</span>
    </button>
  );
}

function ProvField({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">{label}</span>
      <span className="text-[13px] text-slate-700">{children}</span>
    </div>
  );
}

function FamilyChips({ fs }: { fs: RerankerModel["feature_selection"] }) {
  if (!fs) return <span className="text-[13px] text-slate-400">Not recorded</span>;
  // families_enabled === null is the lab's "all available families" sentinel.
  const enabled = fs.families_enabled ?? fs.families_available;
  const allEnabled = fs.families_enabled === null;
  if (!enabled || enabled.length === 0) return <span className="text-[13px] text-slate-400">Not recorded</span>;
  return (
    <div className="flex flex-wrap gap-1.5">
      {allEnabled && (
        <span className="rounded border border-emerald-200 bg-emerald-50 px-1.5 py-0.5 text-[11px] font-medium text-emerald-700">
          all available
        </span>
      )}
      {enabled.map((fam) => {
        const lineage = LINEAGE_FAMILIES.has(fam);
        return (
          <span
            key={fam}
            title={lineage ? "Lineage family (present in the 56-feature champion)" : undefined}
            className={`rounded px-1.5 py-0.5 text-[11px] font-medium ${
              lineage
                ? "border border-blue-300 bg-blue-50 text-blue-700"
                : "border border-slate-200 bg-slate-50 text-slate-600"
            }`}
          >
            {fam}
          </span>
        );
      })}
    </div>
  );
}

// Tooltip copy for the four card-level provenance pills. Kept verbose
// enough that a researcher reading the card without context can still
// reason about whether a booster will inference cleanly.
const PROV_TOOLTIPS = {
  schema: (
    "feature_schema_sha: feature-family-aware fingerprint of the booster. " +
    "Must match the live pipeline's schema at inference; mismatch refuses to score."
  ),
  manifest: (
    "dataset_manifest_sha: sha256 of the source dataset's serialised manifest. " +
    "A drift here means the lab trained on a different dump than is published now."
  ),
  dataset: (
    "dataset_id: UUID of the linked Dataset row (export_research_dataset output). " +
    "Click to copy the full id."
  ),
  source: (
    "external_source: provenance tag for boosters trained outside PROTEA " +
    "(e.g. protea-reranker-lab@<git-sha>)."
  ),
} as const;

function ProvPill({
  label,
  value,
  tooltip,
}: {
  label: string;
  value: string | null | undefined;
  tooltip: string;
}) {
  const [copied, setCopied] = useState(false);
  if (!value) {
    return (
      <Tooltip text={tooltip}>
        <span className="inline-flex items-center gap-1 rounded bg-slate-50 px-1.5 py-0.5 font-mono text-[11px] text-slate-400 border border-slate-200">
          {label}=<span className="italic">unset</span>
        </span>
      </Tooltip>
    );
  }
  const short = value.length > 12 ? `${value.slice(0, 12)}` : value;
  return (
    <Tooltip text={tooltip}>
      <button
        type="button"
        title={`${value} (click to copy)`}
        onClick={(e) => {
          e.stopPropagation();
          navigator.clipboard?.writeText(value).then(
            () => {
              setCopied(true);
              setTimeout(() => setCopied(false), 1200);
            },
            () => {},
          );
        }}
        className="inline-flex items-center gap-1 rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5 font-mono text-[11px] text-slate-700 hover:bg-slate-100 transition-colors"
      >
        {label}={short}
        <span className="text-[10px] text-slate-400">{copied ? "✓" : "⧉"}</span>
      </button>
    </Tooltip>
  );
}

function ProvenanceStrip({ model }: { model: RerankerModel }) {
  return (
    <div
      className="flex flex-wrap items-center gap-1.5 mt-2"
      data-testid="reranker-provenance-strip"
    >
      <ProvPill label="schema" value={model.feature_schema_sha} tooltip={PROV_TOOLTIPS.schema} />
      <ProvPill label="manifest" value={model.dataset_manifest_sha} tooltip={PROV_TOOLTIPS.manifest} />
      <ProvPill label="dataset" value={model.dataset_id} tooltip={PROV_TOOLTIPS.dataset} />
      <ProvPill label="source" value={model.external_source} tooltip={PROV_TOOLTIPS.source} />
    </div>
  );
}

function ProvenancePanel({ model, locale }: { model: RerankerModel; locale: string }) {
  const fs = model.feature_selection;
  const drop = fs?.drop_features ?? [];
  return (
    <div className="rounded-md border border-slate-200 bg-slate-50/60 p-3">
      <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-3">
        Provenance &amp; feature selection
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-3">
        <ProvField label="Feature schema SHA">
          <CopyableSha value={model.feature_schema_sha} />
        </ProvField>
        <ProvField label="Dataset">
          {model.dataset_name && model.dataset_id ? (
            // Link straight to /datasets/{id} so reviewers can jump into the
            // dump that produced this booster (schema_sha + manifest_sha +
            // artifact URIs live there). Memory
            // feedback_ui_surface_provenance_not_hide_params: these fields
            // are reproducibility-critical and stay visible by default.
            <Link
              href={`/${locale}/instrument/datasets/${model.dataset_id}`}
              className="inline-flex items-center gap-1 text-blue-700 hover:underline"
              title={`Open dataset ${model.dataset_name}`}
            >
              <span>{model.dataset_name}</span>
              <span className="font-mono text-[11px] text-slate-400">
                ({shortId(model.dataset_id)})
              </span>
              <span aria-hidden className="text-[11px]">→</span>
            </Link>
          ) : model.dataset_name ? (
            <span>
              {model.dataset_name}{" "}
              <span className="font-mono text-[11px] text-slate-400">
                ({model.dataset_id ? shortId(model.dataset_id) : "—"})
              </span>
            </span>
          ) : (
            <span className="text-slate-400">Not linked</span>
          )}
        </ProvField>
        <ProvField label="Dataset schema SHA">
          <CopyableSha value={model.dataset_schema_sha} />
        </ProvField>
        <ProvField label="Dataset manifest SHA">
          <CopyableSha value={model.dataset_manifest_sha} />
        </ProvField>
        <ProvField label="Producer version">
          <span className="font-mono text-[12px]">{model.producer_version ?? "—"}</span>
        </ProvField>
        <ProvField label="Producer git SHA">
          <CopyableSha value={model.producer_git_sha} />
        </ProvField>
        <ProvField label="External source">
          <span className="font-mono text-[12px]">{model.external_source ?? "—"}</span>
        </ProvField>
        <ProvField label={`Feature count${fs?.feature_count != null ? "" : ""}`}>
          <span>{fs?.feature_count != null ? fs.feature_count : "—"}</span>
        </ProvField>
      </div>

      <div className="mt-3 border-t border-slate-200 pt-3">
        <ProvField label="Enabled feature families">
          <FamilyChips fs={fs} />
        </ProvField>
      </div>

      {drop.length > 0 && (
        <div className="mt-3">
          <ProvField label="Dropped features">
            <div className="flex flex-wrap gap-1.5">
              {drop.map((f) => (
                <span key={f} className="rounded border border-rose-200 bg-rose-50 px-1.5 py-0.5 text-[11px] text-rose-600 line-through">
                  {f}
                </span>
              ))}
            </div>
          </ProvField>
        </div>
      )}
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
  locale,
  initiallyExpanded = false,
}: {
  model: RerankerModel;
  predictionSets: PredictionSet[];
  evaluationSets: EvaluationSet[];
  annotationSets: AnnotationSet[];
  onDelete: () => void;
  locale: string;
  initiallyExpanded?: boolean;
}) {
  const [expanded, setExpanded] = useState(initiallyExpanded);
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
    // id={model.id} so eval/[id]?result=… can deep-link via hash anchor
    // (#<reranker_id>) from the new research-flow shortcut cards.
    // scroll-mt offsets the sticky nav header on hash-jump.
    <div id={model.id} className="rounded-lg border bg-white shadow-sm overflow-hidden scroll-mt-24">
      <div
        className="px-4 py-3 cursor-pointer hover:bg-slate-50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3 flex-wrap">
            <span className="font-semibold text-slate-900">{model.name}</span>
            <span className="rounded-full border px-2 py-0.5 text-xs font-medium bg-indigo-50 text-indigo-700 border-indigo-100 uppercase">
              {model.category}
            </span>
            {model.aspect && (
              <span className="rounded-full border px-2 py-0.5 text-xs font-medium bg-amber-50 text-amber-700 border-amber-100 uppercase">
                {model.aspect}
              </span>
            )}
            {/* Dataset-name pill: lets researchers scan "which reranker used
                which dataset" from the list view without opening each card.
                Links straight to the Dataset detail page; stopPropagation
                so the click doesn't toggle expand. */}
            {model.dataset_name && model.dataset_id && (
              <Link
                href={`/${locale}/instrument/datasets/${model.dataset_id}`}
                onClick={(e) => e.stopPropagation()}
                title={model.dataset_name}
                className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[11px] font-medium text-slate-700 hover:bg-blue-50 hover:border-blue-200 hover:text-blue-700 transition-colors"
              >
                <span aria-hidden className="text-slate-400">dataset:</span>
                <span className="font-mono max-w-[260px] truncate">{model.dataset_name}</span>
              </Link>
            )}
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-slate-600">{new Date(model.created_at).toLocaleDateString()}</span>
            <span className="text-slate-300 text-xs">{expanded ? "▲" : "▼"}</span>
          </div>
        </div>
        <div className="flex flex-wrap gap-4 mt-2 text-[13px] text-slate-500">
          {m.test_fmax != null ? (
            <>
              <span>Test Fmax: <strong className="text-slate-700">{m.test_fmax.toFixed(4)}</strong></span>
              <span>Best iter: <strong className="text-slate-700">{m.best_iteration ?? "—"}</strong></span>
              {m.positive_rate_train != null && (
                <span>Train pos. rate: <strong className="text-slate-700">{(m.positive_rate_train * 100).toFixed(2)}%</strong></span>
              )}
            </>
          ) : (
            <>
              <span>AUC: <strong className="text-slate-700">{m.val_auc?.toFixed(4) ?? "—"}</strong></span>
              <span>F1: <strong className="text-slate-700">{m.val_f1?.toFixed(4) ?? "—"}</strong></span>
              <span>Precision: <strong className="text-slate-700">{m.val_precision?.toFixed(4) ?? "—"}</strong></span>
              <span>Recall: <strong className="text-slate-700">{m.val_recall?.toFixed(4) ?? "—"}</strong></span>
              <span>Positive rate: <strong className="text-slate-700">{m.positive_rate != null ? `${(m.positive_rate * 100).toFixed(2)}%` : "—"}</strong></span>
            </>
          )}
        </div>
        {/* Reproducibility-critical provenance always visible on the collapsed card:
            schema_sha / manifest_sha / dataset_id / external_source. Hover each
            pill for the full meaning; click to copy the full sha. */}
        <ProvenanceStrip model={model} />
      </div>

      {expanded && (
        <div className="border-t px-4 py-4 space-y-5">
          {/* Training-time metrics */}
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-2">Training-time metrics</p>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
              <MetricsBadge label="Test Fmax" value={m.test_fmax} />
              <MetricsBadge label="Best iteration" value={m.best_iteration} />
              <MetricsBadge label="AUC" value={m.val_auc} />
              <MetricsBadge label="Log-loss" value={m.val_logloss} />
              <MetricsBadge label="F1" value={m.val_f1} />
            </div>
            <div className="flex flex-wrap gap-4 mt-2 text-[13px] text-slate-500">
              {m.train_samples != null && <span>Train samples: {m.train_samples.toLocaleString()}</span>}
              {m.val_samples != null && <span>Val samples: {m.val_samples.toLocaleString()}</span>}
              {m.positive_rate_train != null && (
                <span>Train positive rate: {(m.positive_rate_train * 100).toFixed(2)}%</span>
              )}
            </div>
          </div>

          {/* Provenance & feature selection (reproducibility) */}
          <ProvenancePanel model={model} locale={locale} />

          {/* Feature importance */}
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-2">Feature importance (gain)</p>
            <FeatureImportanceChart importance={model.feature_importance} />
          </div>

          {/* Full spec.yaml (reproducibility) */}
          {model.spec_yaml && (
            <details className="rounded-md border border-slate-200 bg-white">
              <summary className="cursor-pointer px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-600 hover:bg-slate-50">
                ExperimentSpec (spec.yaml)
              </summary>
              <pre className="overflow-x-auto border-t border-slate-200 bg-slate-50 px-3 py-2 text-[11px] leading-relaxed text-slate-700 font-mono whitespace-pre-wrap">
                {model.spec_yaml}
              </pre>
            </details>
          )}

          {/* Download reranked TSV */}
          {model.prediction_set_id && (
            <div>
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-2">Download re-ranked predictions</p>
              <a
                href={getRerankedTsvUrl(model.prediction_set_id, model.id)}
                download={`reranked_${shortId(model.id)}.tsv`}
                className="inline-flex items-center gap-1.5 rounded-md border bg-white px-3 py-1.5 text-sm font-medium text-slate-700 shadow-sm hover:bg-slate-50"
              >
                ↓ Download reranked TSV
              </a>
            </div>
          )}

          {/* Compute CAFA metrics */}
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-2">Compute CAFA metrics</p>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 mb-2">
              <div>
                <label htmlFor={`rr-metrics-ps-${model.id}`} className="text-[13px] text-slate-500 mb-0.5 block">Prediction set</label>
                <select
                  id={`rr-metrics-ps-${model.id}`}
                  value={metricsPsId}
                  onChange={(e) => setMetricsPsId(e.target.value)}
                  className={selectClass}
                >
                  <option value="">Select...</option>
                  {predictionSets.map((ps) => (
                    <option key={ps.id} value={ps.id}>{predLabel(ps)}</option>
                  ))}
                </select>
              </div>
              <div>
                <label htmlFor={`rr-metrics-es-${model.id}`} className="text-[13px] text-slate-500 mb-0.5 block">Evaluation set</label>
                <select
                  id={`rr-metrics-es-${model.id}`}
                  value={metricsEsId}
                  onChange={(e) => setMetricsEsId(e.target.value)}
                  className={selectClass}
                >
                  <option value="">Select...</option>
                  {evaluationSets.map((es) => (
                    <option key={es.id} value={es.id}>{evalLabel(es, annotationSets)}</option>
                  ))}
                </select>
              </div>
              <div>
                <label htmlFor={`rr-metrics-cat-${model.id}`} className="text-[13px] text-slate-500 mb-0.5 block">Category</label>
                <select
                  id={`rr-metrics-cat-${model.id}`}
                  value={metricsCategory}
                  onChange={(e) => setMetricsCategory(e.target.value)}
                  className={selectClass}
                >
                  <option value="nk">NK (No Knowledge)</option>
                  <option value="lk">LK (Limited Knowledge)</option>
                  <option value="pk">PK (Partial Knowledge)</option>
                </select>
                <p className="text-[10px] text-slate-600 mt-1 leading-snug">{CATEGORY_HINTS[metricsCategory]}</p>
              </div>
            </div>
            <button
              onClick={handleComputeMetrics}
              disabled={!metricsPsId || !metricsEsId || metricsLoading}
              className={btnPrimary}
            >
              {metricsLoading ? "Computing... (this may take 30-60s)" : "Compute metrics"}
            </button>
            {metricsError && <p className="text-xs text-red-600 mt-2">{metricsError}</p>}
            {metrics && (
              <div className="mt-3 rounded-md border bg-slate-50 p-3">
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2">
                  <MetricsBadge label="Fmax" value={metrics.fmax} />
                  <MetricsBadge label="AUC-PR" value={metrics.auc_pr} />
                  <MetricsBadge label="Threshold" value={metrics.threshold_at_fmax} />
                  <MetricsBadge label="GT proteins" value={metrics.n_ground_truth_proteins} />
                  <MetricsBadge label="Pred. proteins" value={metrics.n_predicted_proteins} />
                  <MetricsBadge label="Predictions" value={metrics.n_predictions} />
                </div>
                {metrics.curve && metrics.curve.length > 0 && (
                  <p className="text-[10px] text-slate-600 mt-2">{metrics.curve.length} PR curve points computed</p>
                )}
              </div>
            )}
          </div>

          {/* Source info */}
          <div className="flex flex-wrap gap-4 text-xs text-slate-600 border-t pt-3">
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
  const params = useParams<{ locale: string }>();
  const locale = params?.locale ?? "en";
  const [rerankers, setRerankers] = useState<RerankerModel[]>([]);
  const [predictionSets, setPredictionSets] = useState<PredictionSet[]>([]);
  const [evaluationSets, setEvaluationSets] = useState<EvaluationSet[]>([]);
  const [annotationSets, setAnnotationSets] = useState<AnnotationSet[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // Hash anchor (set by the eval/[id] shortcut card) selects which card
  // auto-expands on mount and scrolls into view. Read once on first render
  // so client-side navigation between rerankers doesn't re-toggle cards.
  const targetHashRef = useRef<string | null>(null);
  if (typeof window !== "undefined" && targetHashRef.current === null) {
    targetHashRef.current = window.location.hash.replace(/^#/, "");
  }

  // Dialog flags for the lab-bridge import paths (multipart upload +
  // register-by-reference). Both dialogs reload the reranker list on
  // success so the new booster card appears immediately. LightGBM
  // training itself lives in protea-reranker-lab and is registered back
  // via these two paths; the legacy in-PROTEA "Train" form was removed
  // when ``TrainRerankerOperation`` was unregistered from the operation
  // registry (POST /scoring/rerankers/train returns 405).
  const [importOpen, setImportOpen] = useState(false);
  const [registerOpen, setRegisterOpen] = useState(false);

  // Pagination: the registry holds ~50 boosters, each card is tall, so the
  // un-paginated page renders ~49k px and is impossible to navigate on a
  // projector. Reveal PAGE_SIZE at a time behind a "Load more" control.
  const PAGE_SIZE = 10;
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);

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

  // After rerankers land, if the URL carries a hash anchor for one of them
  // (set by the eval/[id] shortcut card), scroll the matching card into
  // view. Browsers handle bare ``#<id>`` navigation on initial server-
  // rendered pages, but here the list is hydrated client-side so we have
  // to nudge the scroll once the targeted card actually exists in the DOM.
  useEffect(() => {
    const hash = targetHashRef.current;
    if (!hash || rerankers.length === 0) return;
    // If the deep-linked card sits past the current page window, reveal
    // enough cards so it exists in the DOM before we scroll to it.
    const idx = rerankers.findIndex((r) => r.id === hash);
    if (idx >= 0 && idx >= visibleCount) {
      setVisibleCount(idx + 1);
      return;
    }
    const el = document.getElementById(hash);
    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [rerankers, visibleCount]);

  return (
    <>
      <div className="flex flex-wrap items-center gap-2.5 mb-1">
        <h1 className="text-xl font-semibold">Re-ranker Models</h1>
        <span
          title="Research / LAB surface: research and reproducibility parameters are intentionally exposed."
          className="rounded bg-violet-100 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-violet-700 ring-1 ring-violet-200"
        >
          LAB
        </span>
        <div className="ml-auto flex gap-2">
          <button
            type="button"
            onClick={() => setImportOpen(true)}
            className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50"
          >
            Import booster (upload)
          </button>
          <button
            type="button"
            onClick={() => setRegisterOpen(true)}
            className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50"
          >
            Register by URI
          </button>
        </div>
      </div>

      <ContextBanner
        title="Register a LightGBM booster trained in protea-reranker-lab"
        description="LightGBM training lives in the offline lab; this page registers the resulting booster so PROTEA can score predictions with it. Use Import booster to upload a fresh model.txt + spec.yaml + run.json, or Register by URI when the artefact already lives in MinIO."
        prerequisites={[
          { label: `${predictionSets.length} prediction set(s)`, met: predictionSets.length > 0, href: "/instrument/functional-annotation" },
          { label: `${evaluationSets.length} evaluation set(s)`, met: evaluationSets.length > 0, href: "/instrument/evaluation" },
        ]}
        nextStep={{ label: "Evaluation", href: "/instrument/evaluation" }}
      />
      <p className="text-sm text-slate-500 mb-6">
        LightGBM binary classifiers trained on dated GOA windows, LAFA-style: the model sees only
        annotations known at the earlier release date and is scored against those that appeared by the
        later one, never random splits.
        A re-ranker uses alignment, taxonomy, and aggregate features to re-score GO predictions
        with calibrated probabilities, replacing the raw embedding distance ranking.
      </p>

      {/* List of rerankers */}
      {loading && (
        <div className="rounded-lg border bg-white shadow-sm overflow-hidden">
          {Array.from({ length: 4 }).map((_, i) => (
            <SkeletonTableRow key={i} cols={5} />
          ))}
        </div>
      )}
      {error && <p className="text-sm text-red-600">{error}</p>}

      {!loading && rerankers.length === 0 && (
        <div className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-slate-600 shadow-sm">
          No re-ranker models registered yet. Use the buttons above to import a booster trained in protea-reranker-lab.
        </div>
      )}

      <div className="space-y-3">
        {rerankers.slice(0, visibleCount).map((model) => (
          <RerankerCard
            key={model.id}
            model={model}
            predictionSets={predictionSets}
            evaluationSets={evaluationSets}
            annotationSets={annotationSets}
            onDelete={() => setRerankers((prev) => prev.filter((r) => r.id !== model.id))}
            locale={locale}
            initiallyExpanded={targetHashRef.current === model.id}
          />
        ))}
      </div>

      {/* Load-more pager: keeps the initial render to ~10 cards so the
          page is navigable on a projector instead of ~49k px tall. */}
      {!loading && rerankers.length > visibleCount && (
        <div className="mt-4 flex flex-col items-center gap-1.5">
          <button
            type="button"
            onClick={() => setVisibleCount((c) => c + PAGE_SIZE)}
            className="rounded-md border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 shadow-sm hover:bg-slate-50 transition-colors"
          >
            Load more rerankers
          </button>
          <span className="text-xs text-slate-500 tabular-nums">
            Showing {Math.min(visibleCount, rerankers.length)} of {rerankers.length}
          </span>
        </div>
      )}

      {/* Lab-bridge dialogs: both reload the reranker list on success so
          the newly registered booster card materialises in place. */}
      <ImportRerankerDialog
        open={importOpen}
        onClose={() => setImportOpen(false)}
        onImported={() => loadAll()}
      />
      <RegisterRerankerDialog
        open={registerOpen}
        onClose={() => setRegisterOpen(false)}
        onRegistered={() => loadAll()}
      />
    </>
  );
}
