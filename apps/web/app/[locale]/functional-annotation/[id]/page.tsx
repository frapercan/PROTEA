"use client";

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import {
  getPredictionSet,
  getPredictionSetProteins,
  getProteinPredictions,
  getProteinAnnotations,
  getGoSubgraph,
  getGoTermDistribution,
  listScoringConfigs,
  getScoredTsvUrl,
  createScoringConfig,
  Prediction,
  ProteinAnnotation,
  GoSubgraph,
  ScoringConfig,
} from "@/lib/api";

// ── Scoring engine (mirrors protea/core/scoring.py) ──────────────────────────
//
// Evidence-code quality weights: same default table as the Python backend.
// When a ScoringConfig carries custom `evidence_weights`, those overrides take
// precedence; codes absent from the override still resolve via this table.

const DEFAULT_EVIDENCE_WEIGHTS: Record<string, number> = {
  // Experimental — direct biological evidence
  EXP: 1.0, IDA: 1.0, IPI: 1.0, IMP: 1.0, IGI: 1.0, IEP: 1.0,
  HTP: 1.0, HDA: 1.0, HMP: 1.0, HGI: 1.0, HEP: 1.0,
  IC:  1.0, TAS: 1.0,
  // Computational / Phylogenetic
  ISS: 0.7, ISO: 0.7, ISA: 0.7, ISM: 0.7, IGC: 0.7,
  IBA: 0.7, IBD: 0.7, IKR: 0.7, IRD: 0.7, RCA: 0.7,
  // Electronic / author statement
  NAS: 0.5,
  IEA: 0.3,
  // No biological data
  ND: 0.1,
};

/** Fallback weight for codes not found in any lookup table. */
const DEFAULT_EVIDENCE_WEIGHT_FALLBACK = 0.5;

/**
 * Resolve the quality weight for a single GO evidence code.
 *
 * Resolution order:
 * 1. Config-level override (config.evidence_weights), if present.
 * 2. Module-level DEFAULT_EVIDENCE_WEIGHTS table.
 * 3. DEFAULT_EVIDENCE_WEIGHT_FALLBACK (0.5).
 */
function resolveEvidenceWeight(
  code: string | null | undefined,
  overrides: Record<string, number> | null | undefined,
): number {
  if (!code) return DEFAULT_EVIDENCE_WEIGHT_FALLBACK;
  if (overrides && code in overrides) return overrides[code];
  return DEFAULT_EVIDENCE_WEIGHTS[code] ?? DEFAULT_EVIDENCE_WEIGHT_FALLBACK;
}

/**
 * Compute a [0, 1] confidence score for a prediction row.
 *
 * Mirrors the logic in `protea/core/scoring.py::compute_score()`, including
 * the two-level evidence weight resolution so the UI score always matches
 * the backend TSV export exactly.
 */
function computeScore(pred: Prediction, config: ScoringConfig): number {
  const evWeight = resolveEvidenceWeight(pred.evidence_code, config.evidence_weights);

  const signals: Record<string, number | null> = {
    embedding_similarity: 1 - pred.distance / 2,
    identity_nw: pred.identity_nw,
    identity_sw: pred.identity_sw,
    evidence_weight: pred.evidence_code != null ? evWeight : null,
    taxonomic_proximity:
      pred.taxonomic_distance != null ? 1 / (1 + pred.taxonomic_distance) : null,
  };

  let weightedSum = 0;
  let totalWeight = 0;
  for (const [signal, weight] of Object.entries(config.weights)) {
    if (weight <= 0) continue;
    const val = signals[signal];
    if (val == null) continue;
    weightedSum += weight * Math.max(0, Math.min(1, val));
    totalWeight += weight;
  }

  if (totalWeight === 0) return 0;
  let score = weightedSum / totalWeight;

  if (config.formula === "evidence_weighted") {
    score *= evWeight;
  }

  return score;
}
// ── Scoring signals ───────────────────────────────────────────────────────────

const SIGNALS: { key: string; label: string; hint: string }[] = [
  { key: "embedding_similarity", label: "Emb. similarity", hint: "1 − cosine_distance / 2, always available" },
  { key: "identity_nw",          label: "Identity NW",     hint: "Global identity Needleman-Wunsch (requires compute_alignments)" },
  { key: "identity_sw",          label: "Identity SW",     hint: "Local identity Smith-Waterman (requires compute_alignments)" },
  { key: "evidence_weight",      label: "Evidence",        hint: "GO evidence code quality (EXP→1.0, IEA→0.3)" },
  { key: "taxonomic_proximity",  label: "Tax. proximity",  hint: "1/(1+tax_dist) (requires compute_taxonomy)" },
];

const DEFAULT_CUSTOM_WEIGHTS: Record<string, number> = {
  embedding_similarity: 1.0,
  identity_nw: 0.0,
  identity_sw: 0.0,
  evidence_weight: 0.0,
  taxonomic_proximity: 0.0,
};

export const CUSTOM_ID = "__custom__";

// ── WeightPanel ───────────────────────────────────────────────────────────────

function WeightPanel({
  config,
  isCustom,
  customWeights,
  customFormula,
  onWeightChange,
  onFormulaChange,
  onSave,
  saving,
}: {
  config?: ScoringConfig;
  isCustom: boolean;
  customWeights: Record<string, number>;
  customFormula: string;
  onWeightChange: (key: string, val: number) => void;
  onFormulaChange: (f: string) => void;
  onSave: (name: string) => void;
  saving: boolean;
}) {
  const [saveName, setSaveName] = useState("");
  const [showSaveForm, setShowSaveForm] = useState(false);

  if (!isCustom && config) {
    return (
      <div className="flex flex-wrap items-center gap-1.5 text-xs max-w-sm">
        <span className="rounded bg-blue-50 px-1.5 py-0.5 font-mono text-blue-700 border border-blue-100">
          {config.formula}
        </span>
        {SIGNALS.map(({ key, label }) => {
          const w = config.weights[key] ?? 0;
          return (
            <span
              key={key}
              title={`${label}: ${w}`}
              className={`rounded px-1.5 py-0.5 font-mono ${w > 0 ? "bg-gray-100 text-gray-700" : "text-gray-300"}`}
            >
              {label} {w}
            </span>
          );
        })}
        {config.description && (
          <span className="text-gray-400 italic ml-1">{config.description}</span>
        )}
      </div>
    );
  }

  if (isCustom) {
    return (
      <div className="rounded-lg border bg-white p-3 shadow-sm w-72">
        <div className="flex items-center justify-between mb-3">
          <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Custom weights</span>
          <select
            value={customFormula}
            onChange={(e) => onFormulaChange(e.target.value)}
            className="rounded border px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
          >
            <option value="linear">linear</option>
            <option value="evidence_weighted">evidence_weighted</option>
          </select>
        </div>
        <div className="space-y-2">
          {SIGNALS.map(({ key, label, hint }) => {
            const val = customWeights[key] ?? 0;
            return (
              <div key={key} className="flex items-center gap-2" title={hint}>
                <span className="text-xs text-gray-600 w-32 shrink-0">{label}</span>
                <input
                  type="range" min="0" max="1" step="0.05"
                  value={val}
                  onChange={(e) => onWeightChange(key, parseFloat(e.target.value))}
                  className="flex-1 accent-blue-500"
                />
                <span className="font-mono text-xs text-gray-700 w-8 text-right">{val.toFixed(2)}</span>
              </div>
            );
          })}
        </div>
        <div className="mt-3 border-t pt-2.5">
          {!showSaveForm ? (
            <button
              onClick={() => setShowSaveForm(true)}
              className="text-xs text-blue-600 hover:underline"
            >
              Save as named config…
            </button>
          ) : (
            <div className="flex gap-1">
              <input
                type="text"
                value={saveName}
                onChange={(e) => setSaveName(e.target.value)}
                placeholder="Config name"
                className="flex-1 rounded border px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <button
                onClick={() => { onSave(saveName); setShowSaveForm(false); setSaveName(""); }}
                disabled={!saveName.trim() || saving}
                className="rounded bg-blue-600 px-2 py-1 text-xs text-white hover:bg-blue-700 disabled:opacity-40"
              >
                {saving ? "…" : "OK"}
              </button>
              <button
                onClick={() => { setShowSaveForm(false); setSaveName(""); }}
                className="text-xs text-gray-400 hover:text-gray-600 px-1"
              >
                ✕
              </button>
            </div>
          )}
        </div>
      </div>
    );
  }

  return null;
}

import dynamic from "next/dynamic";
const GoGraph = dynamic(() => import("@/components/GoGraph"), { ssr: false });

type Tab = "proteins" | "distribution";

const ASPECT_LABELS: Record<string, string> = {
  F: "Molecular Function",
  P: "Biological Process",
  C: "Cellular Component",
};
const ASPECT_COLORS: Record<string, string> = {
  F: "bg-purple-50 text-purple-700 border-purple-100",
  P: "bg-green-50 text-green-700 border-green-100",
  C: "bg-orange-50 text-orange-700 border-orange-100",
};

function AspectBadge({ aspect }: { aspect?: string | null }) {
  if (!aspect) return <span className="text-gray-300 text-xs">—</span>;
  return (
    <span className={`rounded-full border px-2 py-0.5 text-xs font-medium ${ASPECT_COLORS[aspect] ?? "bg-gray-50 text-gray-600"}`}>
      {aspect}
    </span>
  );
}

function shortId(id: string) {
  return id.slice(0, 8);
}

const PAGE_SIZE = 50;

const RELATION_COLORS: Record<string, string> = {
  same:         "bg-blue-50 text-blue-700",
  parent:       "bg-indigo-50 text-indigo-700",
  child:        "bg-indigo-50 text-indigo-700",
  ancestor:     "bg-violet-50 text-violet-700",
  descendant:   "bg-violet-50 text-violet-700",
  close:        "bg-green-50 text-green-700",
  intermediate: "bg-yellow-50 text-yellow-700",
  distant:      "bg-orange-50 text-orange-700",
  "root-only":  "bg-gray-50 text-gray-500",
  unrelated:    "bg-red-50 text-red-600",
};

function pct(v: number | null) {
  if (v == null) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function scoreColor(score: number): string {
  // green for high scores, yellow for mid, red for low
  if (score >= 0.75) return "text-green-700 font-semibold";
  if (score >= 0.5) return "text-yellow-700";
  return "text-red-500";
}

// Evidence code quality tier colours
function evidenceBadgeClass(code: string): string {
  const w = DEFAULT_EVIDENCE_WEIGHTS[code] ?? DEFAULT_EVIDENCE_WEIGHT_FALLBACK;
  if (w >= 1.0) return "bg-green-100 text-green-700 border-green-200";
  if (w >= 0.7) return "bg-blue-100 text-blue-700 border-blue-200";
  if (w >= 0.5) return "bg-yellow-100 text-yellow-700 border-yellow-200";
  return "bg-gray-100 text-gray-500 border-gray-200";
}

type GroupedAnnotation = { go_id: string; name: string | null; aspect: string | null; evidence_codes: string[] };

function groupAnnotations(anns: ProteinAnnotation[]): Map<string, GroupedAnnotation> {
  const map = new Map<string, GroupedAnnotation>();
  for (const a of anns) {
    const existing = map.get(a.go_id);
    if (existing) {
      if (a.evidence_code && !existing.evidence_codes.includes(a.evidence_code))
        existing.evidence_codes.push(a.evidence_code);
    } else {
      map.set(a.go_id, { go_id: a.go_id, name: a.name, aspect: a.aspect, evidence_codes: a.evidence_code ? [a.evidence_code] : [] });
    }
  }
  return map;
}

function PredictionTable({ preds, knownByGoId, scoringConfig }: {
  preds: Prediction[];
  knownByGoId: Map<string, GroupedAnnotation>;
  scoringConfig?: ScoringConfig;
}) {
  const hasAlignment = preds.some((p) => p.identity_nw != null);
  const hasTaxonomy = preds.some((p) => p.taxonomic_relation != null);
  const hasReranker = preds.some((p) => p.vote_count != null);
  const hasScore = !!scoringConfig;
  const hasDetail = hasAlignment || hasTaxonomy || hasReranker;
  const [expanded, setExpanded] = useState<string | null>(null);

  // Sort: by score desc if config active, else by distance asc
  const sorted = hasScore
    ? [...preds].sort((a, b) => computeScore(b, scoringConfig!) - computeScore(a, scoringConfig!))
    : [...preds].sort((a, b) => a.distance - b.distance);

  // Tailwind requires complete class strings — no dynamic construction
  const gridClass = hasScore
    ? "grid-cols-[60px_90px_1fr_110px_60px_60px_65px]"
    : "grid-cols-[90px_1fr_110px_60px_60px_65px]";

  return (
    <div className="rounded-md border bg-white text-xs">
      {/* Desktop header */}
      <div className={`hidden lg:grid ${gridClass} gap-x-3 border-b bg-gray-50 px-3 py-1.5 font-semibold uppercase tracking-wide text-gray-400`}>
        {hasScore && <div>Score</div>}
        <div>GO ID</div>
        <div>Name</div>
        <div>Via (ref)</div>
        <div>Pred. ev.</div>
        <div>Known ev.</div>
        <div>Dist</div>
      </div>

      {sorted.length === 0 ? (
        <p className="px-3 py-3 text-gray-300">—</p>
      ) : sorted.map((pred) => {
        const isExpanded = expanded === pred.go_id;
        const knownAnn = knownByGoId.get(pred.go_id);

        return (
          <div key={pred.go_id}>
            {/* ── Mobile card ── */}
            <div
              className={`lg:hidden border-b px-3 py-2.5 ${knownAnn ? "bg-green-50" : ""} ${hasDetail ? "cursor-pointer active:bg-blue-50/40" : ""}`}
              onClick={() => hasDetail ? setExpanded(isExpanded ? null : pred.go_id) : undefined}
            >
              <div className="flex items-start justify-between gap-2 mb-1">
                <span className="font-mono text-blue-600">{pred.go_id}</span>
                <div className="flex items-center gap-1.5 shrink-0">
                  {hasScore && (
                    <span className={`font-mono text-[10px] ${scoreColor(computeScore(pred, scoringConfig!))}`}>
                      {computeScore(pred, scoringConfig!).toFixed(3)}
                    </span>
                  )}
                  <span className="font-mono text-gray-500 text-[10px]">{pred.distance.toFixed(4)}</span>
                  {hasDetail && <span className="text-gray-300 text-[10px]">{isExpanded ? "▲" : "▼"}</span>}
                </div>
              </div>
              <p className="text-gray-700 leading-snug text-xs mb-1">{pred.name ?? "—"}</p>
              <div className="flex flex-wrap items-center gap-2 text-[10px]">
                <span className="text-gray-400">via</span>
                <Link
                  href={`/proteins/${pred.ref_protein_accession}`}
                  className="font-mono text-blue-500 hover:underline"
                  onClick={(e) => e.stopPropagation()}
                >{pred.ref_protein_accession}</Link>
                {pred.evidence_code && (
                  <span className={`rounded border px-1 py-0.5 font-mono font-medium ${evidenceBadgeClass(pred.evidence_code)}`}>
                    {pred.evidence_code}
                  </span>
                )}
                {knownAnn && knownAnn.evidence_codes.map((ec) => (
                  <span key={ec} className={`rounded border px-1 py-0.5 font-mono font-medium ${evidenceBadgeClass(ec)}`}>
                    {ec}
                  </span>
                ))}
              </div>
            </div>

            {/* ── Desktop row ── */}
            <div
              className={`hidden lg:grid ${gridClass} gap-x-3 border-b px-3 py-2 last:border-0 items-start
                ${knownAnn ? "bg-green-50" : ""}
                ${hasDetail ? "cursor-pointer hover:bg-blue-50/40" : ""}`}
              onClick={() => hasDetail ? setExpanded(isExpanded ? null : pred.go_id) : undefined}
            >
              {hasScore && (
                <span className={`font-mono ${scoreColor(computeScore(pred, scoringConfig!))}`}>
                  {computeScore(pred, scoringConfig!).toFixed(3)}
                </span>
              )}
              <span className="font-mono text-blue-600">{pred.go_id}</span>
              <span className="text-gray-700 leading-snug">{pred.name ?? "—"}</span>
              <div className="flex items-start gap-1">
                <Link
                  href={`/proteins/${pred.ref_protein_accession}`}
                  className="font-mono text-blue-500 hover:underline"
                  onClick={(e) => e.stopPropagation()}
                >{pred.ref_protein_accession}</Link>
                {hasDetail && <span className="text-gray-300 text-[10px] mt-0.5">{isExpanded ? "▲" : "▼"}</span>}
              </div>
              <div>
                {pred.evidence_code ? (
                  <span className={`rounded border px-1 py-0.5 text-[10px] font-mono font-medium ${evidenceBadgeClass(pred.evidence_code)}`}>
                    {pred.evidence_code}
                  </span>
                ) : <span className="text-gray-300">—</span>}
              </div>
              <div className="flex flex-wrap gap-0.5">
                {knownAnn ? knownAnn.evidence_codes.map((ec) => (
                  <span key={ec} className={`rounded border px-1 py-0.5 text-[10px] font-mono font-medium ${evidenceBadgeClass(ec)}`}>
                    {ec}
                  </span>
                )) : <span className="text-gray-300">—</span>}
              </div>
              <span className="font-mono text-gray-500">{pred.distance.toFixed(4)}</span>
            </div>

            {/* Expanded: alignment + taxonomy + reranker detail */}
            {isExpanded && hasDetail && (
              <div className="border-b bg-gray-50 px-4 py-3 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6">
                {hasAlignment && (
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-gray-400 mb-2">
                      Alignment — query vs {pred.ref_protein_accession}
                    </p>
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="text-gray-400">
                          <th className="text-left font-medium pr-4 pb-1">Metric</th>
                          <th className="text-right font-medium pr-4 pb-1">NW (global)</th>
                          <th className="text-right font-medium pb-1">SW (local)</th>
                        </tr>
                      </thead>
                      <tbody className="font-mono">
                        <tr><td className="pr-4 text-gray-500 font-sans py-0.5">Identity</td><td className="text-right pr-4">{pct(pred.identity_nw)}</td><td className="text-right">{pct(pred.identity_sw)}</td></tr>
                        <tr><td className="pr-4 text-gray-500 font-sans py-0.5">Similarity</td><td className="text-right pr-4">{pct(pred.similarity_nw)}</td><td className="text-right">{pct(pred.similarity_sw)}</td></tr>
                        <tr><td className="pr-4 text-gray-500 font-sans py-0.5">Score</td><td className="text-right pr-4">{pred.alignment_score_nw?.toFixed(0) ?? "—"}</td><td className="text-right">{pred.alignment_score_sw?.toFixed(0) ?? "—"}</td></tr>
                        <tr><td className="pr-4 text-gray-500 font-sans py-0.5">Gaps</td><td className="text-right pr-4">{pct(pred.gaps_pct_nw)}</td><td className="text-right">{pct(pred.gaps_pct_sw)}</td></tr>
                        <tr><td className="pr-4 text-gray-500 font-sans py-0.5">Aln length</td><td className="text-right pr-4">{pred.alignment_length_nw ?? "—"}</td><td className="text-right">{pred.alignment_length_sw ?? "—"}</td></tr>
                        <tr><td className="pr-4 text-gray-500 font-sans py-0.5">Seq length</td><td className="text-right pr-4">{pred.length_query ?? "—"} (q)</td><td className="text-right">{pred.length_ref ?? "—"} (r)</td></tr>
                      </tbody>
                    </table>
                  </div>
                )}
                {hasTaxonomy && (
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-gray-400 mb-2">
                      Taxonomy — query vs {pred.ref_protein_accession}
                    </p>
                    <div className="space-y-1.5 text-xs">
                      <div className="flex justify-between">
                        <span className="text-gray-500">Relation</span>
                        <span className={`rounded px-1.5 py-0.5 font-medium ${RELATION_COLORS[pred.taxonomic_relation ?? ""] ?? "bg-gray-50 text-gray-500"}`}>
                          {pred.taxonomic_relation ?? "—"}
                        </span>
                      </div>
                      <div className="flex justify-between"><span className="text-gray-500">Distance</span><span className="font-mono">{pred.taxonomic_distance ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-gray-500">Common ancestors</span><span className="font-mono">{pred.taxonomic_common_ancestors ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-gray-500">LCA taxid</span><span className="font-mono">{pred.taxonomic_lca ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-gray-500">Query taxid</span><span className="font-mono">{pred.query_taxonomy_id ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-gray-500">Ref taxid</span><span className="font-mono">{pred.ref_taxonomy_id ?? "—"}</span></div>
                    </div>
                  </div>
                )}
                {hasReranker && (
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-gray-400 mb-2">
                      Re-ranker features
                    </p>
                    <div className="space-y-1.5 text-xs">
                      <div className="flex justify-between"><span className="text-gray-500">Vote count</span><span className="font-mono">{pred.vote_count ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-gray-500">K position</span><span className="font-mono">{pred.k_position ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-gray-500">GO term frequency</span><span className="font-mono">{pred.go_term_frequency != null ? pred.go_term_frequency.toFixed(4) : "—"}</span></div>
                      <div className="flex justify-between"><span className="text-gray-500">Ref annotation density</span><span className="font-mono">{pred.ref_annotation_density != null ? pred.ref_annotation_density.toFixed(4) : "—"}</span></div>
                      <div className="flex justify-between"><span className="text-gray-500">Neighbor dist std</span><span className="font-mono">{pred.neighbor_distance_std != null ? pred.neighbor_distance_std.toFixed(4) : "—"}</span></div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function ProteinDetail({
  accession,
  inDb,
  predictions,
  annotations,
  loading,
  onClose,
  ontologySnapshotId,
  scoringConfig,
}: {
  accession: string;
  inDb: boolean;
  predictions: Prediction[];
  annotations: ProteinAnnotation[];
  loading: boolean;
  onClose: () => void;
  ontologySnapshotId: string | null;
  scoringConfig?: ScoringConfig;
}) {
  const [subgraph, setSubgraph] = useState<GoSubgraph | null>(null);
  const [loadingGraph, setLoadingGraph] = useState(false);
  const [showGraph, setShowGraph] = useState(false);

  async function toggleGraph() {
    if (showGraph) { setShowGraph(false); return; }
    setLoadingGraph(true);
    setShowGraph(true);
    try {
      const allGoIds = Array.from(new Set([
        ...predictions.map((p) => p.go_id),
        ...annotations.map((a) => a.go_id),
      ]));
      setSubgraph(await getGoSubgraph(ontologySnapshotId!, allGoIds, 3));
    } catch {
      setShowGraph(false);
    } finally {
      setLoadingGraph(false);
    }
  }
  const knownByGoId = groupAnnotations(annotations);

  const predByAspect: Record<string, Prediction[]> = { F: [], P: [], C: [], other: [] };
  for (const p of predictions) {
    const key = p.aspect && predByAspect[p.aspect] ? p.aspect : "other";
    predByAspect[key].push(p);
  }

  const aspects = (["F", "P", "C"] as const).filter((asp) => predByAspect[asp].length > 0);

  const predictedGoIds = new Set(predictions.map((p) => p.go_id));
  const annotatedGoIds = new Set(annotations.map((a) => a.go_id));
  const totalMatches = predictions.filter((p) => knownByGoId.has(p.go_id)).length;
  const totalUniquePredicted = predictions.length;

  // Known terms not covered by any prediction
  const uncoveredKnown = Array.from(knownByGoId.values()).filter((a) => !predictedGoIds.has(a.go_id));

  return (
    <div className="mt-4 rounded-lg border bg-gray-50 p-4">
      <div className="flex flex-wrap items-center gap-2 sm:gap-3 mb-4">
        <span className="font-mono font-semibold text-gray-900">{accession}</span>
        {inDb && (
          <Link href={`/proteins/${accession}`} className="text-xs text-blue-500 hover:underline">
            View protein →
          </Link>
        )}
        {annotations.length > 0 && predictions.length > 0 && (
          <span className="text-xs text-green-700 font-medium sm:ml-auto">
            {totalMatches} / {totalUniquePredicted} match known
          </span>
        )}
        {ontologySnapshotId && predictions.length > 0 && (
          <button onClick={toggleGraph} className="rounded border bg-white px-2 py-1 text-xs hover:bg-gray-50">
            {loadingGraph ? "Loading…" : showGraph ? "Hide graph" : "GO graph"}
          </button>
        )}
        <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-lg leading-none ml-auto sm:ml-2">×</button>
      </div>

      {loading && <p className="text-sm text-gray-400">Loading…</p>}

      {showGraph && subgraph && (
        <div className="mb-4">
          <GoGraph
            subgraph={subgraph}
            predictedGoIds={predictedGoIds}
            knownGoIds={annotatedGoIds}
            height={400}
          />
        </div>
      )}

      {!loading && predictions.length === 0 && annotations.length === 0 && (
        <p className="text-sm text-gray-400">No data found.</p>
      )}

      {!loading && aspects.map((asp) => {
        const preds = predByAspect[asp];
        const uniquePredCount = new Set(preds.map((p) => p.go_id)).size;
        const knownInAspect = Array.from(knownByGoId.values()).filter((a) => a.aspect === asp);
        return (
          <div key={asp} className="mb-5 last:mb-0">
            <div className="flex items-center gap-2 mb-2">
              <AspectBadge aspect={asp} />
              <span className="text-xs font-semibold text-gray-600">{ASPECT_LABELS[asp]}</span>
              <span className="text-xs text-gray-400 ml-1">{uniquePredCount} predicted · {knownInAspect.length} known</span>
            </div>
            <PredictionTable preds={preds} knownByGoId={knownByGoId} scoringConfig={scoringConfig} />
          </div>
        );
      })}

      {/* Known terms with no matching prediction */}
      {!loading && uncoveredKnown.length > 0 && (
        <div className="mt-4">
          <p className="text-xs font-semibold text-gray-500 mb-2">
            Known annotations not covered by any prediction ({uncoveredKnown.length})
          </p>
          <div className="overflow-x-auto rounded-md border bg-white text-xs">
            <div className="grid grid-cols-[90px_1fr_80px] gap-2 border-b bg-gray-50 px-3 py-1.5 font-semibold uppercase tracking-wide text-gray-400">
              <div>GO ID</div><div>Name</div><div>Evidence</div>
            </div>
            {uncoveredKnown.map((ann) => (
              <div key={ann.go_id} className="grid grid-cols-[90px_1fr_80px] gap-2 border-b px-3 py-2 last:border-0 items-start">
                <span className="font-mono text-blue-600 pt-0.5">{ann.go_id}</span>
                <span className="text-gray-700 leading-snug">{ann.name ?? "—"}</span>
                <div className="flex flex-wrap gap-0.5 justify-end">
                  {ann.evidence_codes.map((ec) => (
                    <span key={ec} className={`rounded border px-1 py-0.5 text-[10px] font-mono font-medium ${evidenceBadgeClass(ec)}`}>
                      {ec}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function DownloadButton({ setId, scoringConfigId, customBlocked }: { setId: string; scoringConfigId?: string; customBlocked?: boolean }) {
  const [open, setOpen] = useState(false);
  const [aspect, setAspect] = useState("");
  const [maxDist, setMaxDist] = useState("");
  const [minScore, setMinScore] = useState("");

  const apiBase = process.env.NEXT_PUBLIC_API_URL ?? "";

  function buildRawUrl() {
    const params = new URLSearchParams();
    if (aspect) params.set("aspect", aspect);
    if (maxDist) params.set("max_distance", maxDist);
    const qs = params.toString();
    return `${apiBase}/embeddings/prediction-sets/${setId}/predictions.tsv${qs ? `?${qs}` : ""}`;
  }

  function buildScoredUrl() {
    return getScoredTsvUrl(setId, scoringConfigId!, minScore ? { minScore: parseFloat(minScore) } : undefined);
  }

  const isScored = !!scoringConfigId;

  return (
    <div className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        title={customBlocked ? "Guarda el config custom para descargar scored TSV" : undefined}
        className="flex items-center gap-1.5 rounded-md border bg-white px-3 py-1.5 text-sm font-medium text-gray-700 shadow-sm hover:bg-gray-50"
      >
        ↓ Download TSV
      </button>

      {open && (
        <div className="absolute right-0 top-10 z-20 w-64 rounded-lg border bg-white p-4 shadow-lg">
          <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-3">Download options</p>

          {!isScored && (
            <>
              <label className="block text-xs text-gray-600 mb-1">GO Aspect</label>
              <select
                value={aspect}
                onChange={(e) => setAspect(e.target.value)}
                className="w-full rounded border px-2 py-1.5 text-sm mb-3 focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">All aspects</option>
                <option value="F">F — Molecular Function</option>
                <option value="P">P — Biological Process</option>
                <option value="C">C — Cellular Component</option>
              </select>

              <label className="block text-xs text-gray-600 mb-1">Max distance</label>
              <input
                type="number"
                min="0"
                max="2"
                step="0.01"
                placeholder="e.g. 0.3 (no limit)"
                value={maxDist}
                onChange={(e) => setMaxDist(e.target.value)}
                className="w-full rounded border px-2 py-1.5 text-sm mb-4 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </>
          )}

          {customBlocked && (
            <p className="text-xs text-amber-700 bg-amber-50 rounded px-2 py-1.5 mb-3">
              Guarda el config custom para habilitar scored TSV
            </p>
          )}

          {isScored && (
            <>
              <p className="text-xs text-blue-700 bg-blue-50 rounded px-2 py-1.5 mb-3">
                Scored TSV — includes computed score column
              </p>
              <label className="block text-xs text-gray-600 mb-1">Min score</label>
              <input
                type="number"
                min="0"
                max="1"
                step="0.01"
                placeholder="e.g. 0.5 (no limit)"
                value={minScore}
                onChange={(e) => setMinScore(e.target.value)}
                className="w-full rounded border px-2 py-1.5 text-sm mb-4 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </>
          )}

          <a
            href={isScored ? buildScoredUrl() : buildRawUrl()}
            download={isScored ? `scored_${setId.slice(0, 8)}.tsv` : `predictions_${setId.slice(0, 8)}.tsv`}
            onClick={() => setOpen(false)}
            className="block w-full rounded-md bg-blue-600 px-3 py-2 text-center text-sm font-medium text-white hover:bg-blue-700"
          >
            Download
          </a>
        </div>
      )}
    </div>
  );
}


export default function PredictionSetDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id: setId } = use(params);
  const toast = useToast();
  const [activeTab, setActiveTab] = useState<Tab>("proteins");
  const [annotationSetId, setAnnotationSetId] = useState<string | null>(null);
  const [ontologySnapshotId, setOntologySnapshotId] = useState<string | null>(null);
  const [limitPerEntry, setLimitPerEntry] = useState<number | null>(null);

  // Scoring
  const [scoringConfigs, setScoringConfigs] = useState<ScoringConfig[]>([]);
  const [selectedConfigId, setSelectedConfigId] = useState<string>("");
  const [customWeights, setCustomWeights] = useState<Record<string, number>>(DEFAULT_CUSTOM_WEIGHTS);
  const [customFormula, setCustomFormula] = useState("linear");
  const [savingCustom, setSavingCustom] = useState(false);

  // Proteins tab
  const [proteins, setProteins] = useState<{ accession: string; go_count: number; min_distance: number | null; annotation_count: number; match_count: number; in_db: boolean }[]>([]);
  const [proteinTotal, setProteinTotal] = useState(0);
  const [proteinOffset, setProteinOffset] = useState(0);
  const [proteinSearch, setProteinSearch] = useState("");
  const [proteinSearchInput, setProteinSearchInput] = useState("");
  const [loadingProteins, setLoadingProteins] = useState(false);

  // Inline detail
  const [selectedAccession, setSelectedAccession] = useState<string | null>(null);
  const [selectedInDb, setSelectedInDb] = useState(false);
  const [predictions, setPredictions] = useState<Prediction[]>([]);
  const [knownAnnotations, setKnownAnnotations] = useState<ProteinAnnotation[]>([]);
  const [loadingDetail, setLoadingDetail] = useState(false);

  // Distribution tab
  const [distribution, setDistribution] = useState<{
    by_aspect: Record<string, { go_id: string; name: string | null; count: number }[]>;
    aspect_totals: Record<string, number>;
    top_terms: { go_id: string; name: string | null; aspect: string | null; count: number }[];
  } | null>(null);
  const [loadingDist, setLoadingDist] = useState(false);

  useEffect(() => {
    getPredictionSet(setId)
      .then((ps) => {
        setAnnotationSetId(ps.annotation_set_id);
        setOntologySnapshotId(ps.ontology_snapshot_id);
        setLimitPerEntry(ps.limit_per_entry);
      })
      .catch(() => {});
    listScoringConfigs()
      .then(setScoringConfigs)
      .catch(() => {});
  }, [setId]);

  const selectedConfig: ScoringConfig | undefined =
    selectedConfigId === CUSTOM_ID
      ? { id: CUSTOM_ID, name: "Custom", formula: customFormula, weights: customWeights, evidence_weights: null, created_at: "" }
      : scoringConfigs.find((c) => c.id === selectedConfigId);

  async function loadProteins(offset = 0, search = proteinSearch) {
    setLoadingProteins(true);
    try {
      const res = await getPredictionSetProteins(setId, { search: search || undefined, limit: PAGE_SIZE, offset });
      setProteins(res.items);
      setProteinTotal(res.total);
      setProteinOffset(offset);
    } catch (e: any) {
      toast(e.message ?? "Failed to load proteins", "error");
    } finally {
      setLoadingProteins(false);
    }
  }

  async function loadDistribution() {
    setLoadingDist(true);
    try {
      setDistribution(await getGoTermDistribution(setId));
    } catch (e: any) {
      toast(e.message ?? "Failed to load distribution", "error");
    } finally {
      setLoadingDist(false);
    }
  }

  useEffect(() => {
    if (activeTab === "proteins") {
      loadProteins(0, "");
      if (!distribution) loadDistribution();
    }
    if (activeTab === "distribution") loadDistribution();
  }, [activeTab]);

  function handleProteinSearch(e: React.FormEvent) {
    e.preventDefault();
    setProteinSearch(proteinSearchInput);
    loadProteins(0, proteinSearchInput);
  }

  async function selectProtein(accession: string, inDb: boolean) {
    if (selectedAccession === accession) {
      setSelectedAccession(null);
      return;
    }
    setSelectedAccession(accession);
    setSelectedInDb(inDb);
    setLoadingDetail(true);
    setPredictions([]);
    setKnownAnnotations([]);
    try {
      const [preds, anns] = await Promise.all([
        getProteinPredictions(setId, accession),
        getProteinAnnotations(accession, annotationSetId ?? undefined),
      ]);
      setPredictions(preds);
      setKnownAnnotations(anns);
    } catch (e: any) {
      toast(e.message ?? "Failed to load detail", "error");
    } finally {
      setLoadingDetail(false);
    }
  }

  const totalPages = Math.ceil(proteinTotal / PAGE_SIZE);
  const currentPage = Math.floor(proteinOffset / PAGE_SIZE) + 1;

  const tabs: { key: Tab; label: string }[] = [
    { key: "proteins", label: "Proteins" },
    { key: "distribution", label: "GO Distribution" },
  ];

  return (
    <>
      <div className="mb-6 space-y-3">
        <Breadcrumbs />
        <div>
          <h1 className="text-xl font-semibold mt-2">
            Prediction Set <span className="font-mono text-base text-gray-500">{shortId(setId)}…</span>
          </h1>
          {limitPerEntry != null && (
            <p className="text-xs text-gray-400 mt-0.5">k = {limitPerEntry}</p>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="flex items-center gap-1.5">
            <label className="text-xs text-gray-500 whitespace-nowrap">Scoring</label>
            <select
              value={selectedConfigId}
              onChange={(e) => setSelectedConfigId(e.target.value)}
              className="rounded-md border bg-white px-2 py-1.5 text-sm text-gray-700 shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">Raw distance</option>
              {scoringConfigs.map((c) => (
                <option key={c.id} value={c.id}>{c.name}</option>
              ))}
              <option value={CUSTOM_ID}>Custom…</option>
            </select>
          </div>
          <DownloadButton
            setId={setId}
            scoringConfigId={
              selectedConfigId && selectedConfigId !== CUSTOM_ID ? selectedConfigId : undefined
            }
            customBlocked={selectedConfigId === CUSTOM_ID}
          />
        </div>
        <div>
          {selectedConfigId && (
            <WeightPanel
              config={selectedConfigId !== CUSTOM_ID ? selectedConfig : undefined}
              isCustom={selectedConfigId === CUSTOM_ID}
              customWeights={customWeights}
              customFormula={customFormula}
              onWeightChange={(key, val) => setCustomWeights((prev) => ({ ...prev, [key]: val }))}
              onFormulaChange={setCustomFormula}
              onSave={async (name) => {
                setSavingCustom(true);
                try {
                  const saved = await createScoringConfig({ name, formula: customFormula, weights: customWeights });
                  setScoringConfigs((prev) => [...prev, saved]);
                  setSelectedConfigId(saved.id);
                } finally {
                  setSavingCustom(false);
                }
              }}
              saving={savingCustom}
            />
          )}
        </div>
      </div>

      <div className="flex gap-1 border-b mb-6 overflow-x-auto">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setActiveTab(t.key)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === t.key
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* ── Executive summary ── */}
      {activeTab === "proteins" && distribution && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-6">
          <div className="rounded-lg border bg-white p-3 text-center">
            <div className="text-xl font-bold text-gray-900 tabular-nums">{proteinTotal.toLocaleString()}</div>
            <div className="text-xs text-gray-500">Proteins</div>
          </div>
          {(["P", "F", "C"] as const).map((aspect) => (
            <div key={aspect} className="rounded-lg border bg-white p-3 text-center">
              <div className="text-xl font-bold text-gray-900 tabular-nums">
                {(distribution.aspect_totals[aspect] ?? 0).toLocaleString()}
              </div>
              <div className="text-xs text-gray-500">{ASPECT_LABELS[aspect]}</div>
            </div>
          ))}
        </div>
      )}

      {/* ── Proteins ── */}
      {activeTab === "proteins" && (
        <div>
          <div className="flex flex-wrap items-center gap-3 mb-4">
            <form onSubmit={handleProteinSearch} className="flex gap-2 flex-1 min-w-0">
              <input
                type="text"
                value={proteinSearchInput}
                onChange={(e) => setProteinSearchInput(e.target.value)}
                placeholder="Filter by accession…"
                className="rounded-md border px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-full sm:w-56"
              />
              <button type="submit" className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
                Filter
              </button>
              {proteinSearch && (
                <button type="button" onClick={() => { setProteinSearchInput(""); setProteinSearch(""); loadProteins(0, ""); }}
                  className="rounded-md border bg-white px-3 py-1.5 text-sm text-gray-500 hover:bg-gray-50">
                  Clear
                </button>
              )}
            </form>
            <span className="text-sm text-gray-400">{proteinTotal.toLocaleString()} proteins</span>
          </div>

          {/* Mobile card list */}
          <div className="lg:hidden space-y-2">
            {loadingProteins && Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="rounded-lg border bg-white p-4 shadow-sm animate-pulse">
                <div className="h-4 bg-gray-200 rounded w-1/3 mb-2" />
                <div className="h-3 bg-gray-100 rounded w-2/3" />
              </div>
            ))}
            {!loadingProteins && proteins.length === 0 && (
              <div className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-gray-400 shadow-sm">No proteins found.</div>
            )}
            {!loadingProteins && proteins.map((p) => (
              <div key={p.accession} className="rounded-lg border bg-white shadow-sm overflow-hidden">
                <div
                  className={`p-4 cursor-pointer transition-colors ${
                    selectedAccession === p.accession ? "bg-blue-50" : "hover:bg-gray-50"
                  }`}
                  onClick={() => selectProtein(p.accession, p.in_db)}
                >
                  <div className="flex items-center justify-between mb-1">
                    <div className="flex items-center gap-1.5">
                      <span className={`inline-block w-2 h-2 rounded-full ${
                        p.min_distance == null ? "bg-gray-300"
                        : p.min_distance < 0.3 ? "bg-green-500"
                        : p.min_distance < 0.6 ? "bg-amber-400"
                        : "bg-red-500"
                      }`} title={`min distance: ${p.min_distance?.toFixed(4) ?? "N/A"}`} />
                      {p.in_db ? (
                        <Link href={`/proteins/${p.accession}`} className="font-mono text-sm text-blue-600 hover:underline" onClick={(e) => e.stopPropagation()}>
                          {p.accession}
                        </Link>
                      ) : (
                        <span className="font-mono text-sm text-gray-700">{p.accession}</span>
                      )}
                    </div>
                    <span className="text-xs text-gray-500">{p.go_count} predicted</span>
                  </div>
                  <div className="flex gap-4 text-xs text-gray-500">
                    <span>dist: {p.min_distance?.toFixed(4) ?? "—"}</span>
                    <span>known/pred: {p.annotation_count}/{p.go_count}</span>
                  </div>
                </div>
                {selectedAccession === p.accession && (
                  <div className="border-t px-4 pb-4">
                    <ProteinDetail
                      accession={p.accession}
                      inDb={p.in_db}
                      predictions={predictions}
                      annotations={knownAnnotations}
                      loading={loadingDetail}
                      onClose={() => setSelectedAccession(null)}
                      ontologySnapshotId={ontologySnapshotId}
                      scoringConfig={selectedConfig}
                    />
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Desktop table */}
          <div className="hidden lg:block overflow-x-auto rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[160px_90px_120px_120px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>Accession</div>
              <div>Predicted</div>
              <div>Min Distance</div>
              <div>Known / Pred.</div>
            </div>

            {loadingProteins && Array.from({ length: 8 }).map((_, i) => <SkeletonTableRow key={i} cols={5} />)}

            {!loadingProteins && proteins.length === 0 && (
              <div className="px-4 py-12 text-center text-sm text-gray-400">No proteins found.</div>
            )}

            {!loadingProteins && proteins.map((p) => (
              <div key={p.accession}>
                <div
                  className={`grid grid-cols-[160px_90px_120px_120px] gap-2 border-b px-4 py-3 text-sm items-center cursor-pointer transition-colors ${
                    selectedAccession === p.accession ? "bg-blue-50" : "hover:bg-gray-50"
                  }`}
                  onClick={() => selectProtein(p.accession, p.in_db)}
                >
                  <div className="flex items-center gap-2">
                    <span className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${
                      p.min_distance == null ? "bg-gray-300"
                      : p.min_distance < 0.3 ? "bg-green-500"
                      : p.min_distance < 0.6 ? "bg-amber-400"
                      : "bg-red-500"
                    }`} title={`min distance: ${p.min_distance?.toFixed(4) ?? "N/A"}`} />
                    {p.in_db ? (
                      <Link
                        href={`/proteins/${p.accession}`}
                        className="font-mono text-xs text-blue-600 hover:underline"
                        onClick={(e) => e.stopPropagation()}
                      >
                        {p.accession}
                      </Link>
                    ) : (
                      <span className="font-mono text-xs text-gray-700">{p.accession}</span>
                    )}
                  </div>
                  <div className="text-gray-700 font-medium">{p.go_count}</div>
                  <div className="text-gray-600 font-mono text-xs">{p.min_distance?.toFixed(4) ?? "—"}</div>
                  <div className="text-sm font-mono">
                    {p.annotation_count > 0
                      ? <span className="text-gray-700">{p.annotation_count}</span>
                      : <span className="text-gray-300">0</span>}
                    <span className="text-gray-300 mx-1">/</span>
                    <span className="text-gray-700">{p.go_count}</span>
                  </div>
                </div>

                {selectedAccession === p.accession && (
                  <div className="border-b px-4 pb-4">
                    <ProteinDetail
                      accession={p.accession}
                      inDb={p.in_db}
                      predictions={predictions}
                      annotations={knownAnnotations}
                      loading={loadingDetail}
                      onClose={() => setSelectedAccession(null)}
                      ontologySnapshotId={ontologySnapshotId}
                      scoringConfig={selectedConfig}
                    />
                  </div>
                )}
              </div>
            ))}
          </div>

          {totalPages > 1 && (
            <div className="mt-4 flex items-center justify-between text-sm text-gray-500">
              <span>Page {currentPage} of {totalPages}</span>
              <div className="flex gap-2">
                <button onClick={() => loadProteins(proteinOffset - PAGE_SIZE)} disabled={proteinOffset === 0}
                  className="rounded-md border bg-white px-3 py-1.5 hover:bg-gray-50 disabled:opacity-40">Previous</button>
                <button onClick={() => loadProteins(proteinOffset + PAGE_SIZE)} disabled={proteinOffset + PAGE_SIZE >= proteinTotal}
                  className="rounded-md border bg-white px-3 py-1.5 hover:bg-gray-50 disabled:opacity-40">Next</button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── GO Distribution ── */}
      {activeTab === "distribution" && (
        <div>
          {loadingDist && <p className="text-sm text-gray-400">Loading…</p>}

          {distribution && (
            <div className="space-y-6">
              <div className="grid grid-cols-3 gap-3">
                {(["F", "P", "C"] as const).map((asp) => (
                  <div key={asp} className="rounded-lg border bg-white p-4 shadow-sm">
                    <p className="text-xs font-semibold uppercase tracking-wide text-gray-400">{ASPECT_LABELS[asp]}</p>
                    <p className="mt-1 text-2xl font-bold text-gray-900">
                      {(distribution.aspect_totals[asp] ?? 0).toLocaleString()}
                    </p>
                    <p className="text-xs text-gray-400 mt-0.5">predictions</p>
                  </div>
                ))}
              </div>

              {(["F", "P", "C"] as const).map((asp) => {
                const terms = distribution.by_aspect[asp] ?? [];
                if (terms.length === 0) return null;
                const maxCount = terms[0]?.count ?? 1;
                return (
                  <div key={asp}>
                    <p className="text-sm font-semibold text-gray-700 mb-3">
                      {ASPECT_LABELS[asp]}
                      <span className="ml-2 text-xs font-normal text-gray-400">top {terms.length} terms</span>
                    </p>
                    <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
                      {terms.map((t) => (
                        <div key={t.go_id} className="flex items-center gap-3 border-b px-4 py-2.5 last:border-0">
                          <span className="font-mono text-xs text-blue-600 w-24 shrink-0">{t.go_id}</span>
                          <span className="text-xs text-gray-700 flex-1 truncate">{t.name ?? "—"}</span>
                          <div className="flex items-center gap-2 shrink-0">
                            <div className="w-24 h-1.5 rounded-full bg-gray-100 overflow-hidden">
                              <div
                                className="h-1.5 rounded-full bg-blue-400"
                                style={{ width: `${Math.round((t.count / maxCount) * 100)}%` }}
                              />
                            </div>
                            <span className="text-xs text-gray-500 w-12 text-right">{t.count.toLocaleString()}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </>
  );
}
