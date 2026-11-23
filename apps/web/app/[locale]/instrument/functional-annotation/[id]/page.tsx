"use client";

import { formatCount } from "@/lib/format";

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { notFound } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import {
  getPredictionSet,
  getPredictionSetProteins,
  getProteinPredictions,
  getProteinAnnotations,
  getGoSubgraph,
  getGoTermDistribution,
  ApiError,
  Prediction,
  ProteinAnnotation,
  GoSubgraph,
  ScoredSetSummary,
} from "@/lib/api";

import dynamic from "next/dynamic";
const GoGraph = dynamic(() => import("@/components/GoGraph"), { ssr: false });

type Tab = "proteins" | "distribution";

/** What the page says while a set's counts are read: a set is counted on its
 *  first visit and kept, which on a set of a few million rows takes a while once. */
const LOADING_NOTE =
  "Loading\u2026 A set is counted on its first visit and the counts are kept: on a set of a few million rows that first visit takes up to a minute.";

/** What the page says of a set whose job is still writing it. */
const PROVISIONAL_NOTE =
  "This set is still being written: its counts are read as they stand and will change, and none is kept until its job ends.";

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
  if (!aspect) return <span className="text-slate-300 text-xs">—</span>;
  return (
    <span className={`rounded-full border px-2 py-0.5 text-xs font-medium ${ASPECT_COLORS[aspect] ?? "bg-slate-50 text-slate-600"}`}>
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
  "root-only":  "bg-slate-50 text-slate-500",
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

/**
 * The witness of a native method's row (ADR-D65): which unit gave the score and
 * how many of its donors hold the term, or the base rate the row was floored at.
 * A method that wrote no explanation shows its name, as before.
 */
function MethodWitness({ pred }: { pred: Prediction }) {
  const why = pred.explanation;
  const num = (v: unknown) => (typeof v === "number" ? v : null);
  if (why && num(why.unit) != null) {
    const k = num(why.k), n = num(why.n);
    return (
      <span className="font-mono text-slate-700" title={`m ${num(why.strength)?.toFixed(2) ?? "?"}, base rate ${num(why.base_rate)?.toFixed(4) ?? "?"}`}>
        theme {num(why.unit)}
        {k != null && n != null && <span className="text-slate-500"> · {k} of {n} donors</span>}
      </span>
    );
  }
  if (why && why.prior) {
    return (
      <span className="text-slate-500" title="no unit of the protein scores the term above its base rate">
        prior · base rate {num(why.base_rate)?.toFixed(4) ?? "?"}
      </span>
    );
  }
  return <span className="font-mono text-slate-500">{pred.ref_protein_accession}</span>;
}

// Evidence code tiers, for the colour of a badge and nothing else: experimental,
// curated computational or phylogenetic, electronic or none, and the rest (an
// author statement, or a code no tier names).
const EXPERIMENTAL_CODES = new Set([
  "EXP", "IDA", "IPI", "IMP", "IGI", "IEP", "HTP", "HDA", "HMP", "HGI", "HEP", "IC", "TAS",
]);
const CURATED_CODES = new Set([
  "ISS", "ISO", "ISA", "ISM", "IGC", "IBA", "IBD", "IKR", "IRD", "RCA",
]);
const UNSUPPORTED_CODES = new Set(["IEA", "ND"]);

function evidenceBadgeClass(code: string): string {
  if (EXPERIMENTAL_CODES.has(code)) return "bg-green-100 text-green-700 border-green-200";
  if (CURATED_CODES.has(code)) return "bg-blue-100 text-blue-700 border-blue-200";
  if (UNSUPPORTED_CODES.has(code)) return "bg-slate-100 text-slate-500 border-slate-200";
  return "bg-yellow-100 text-yellow-700 border-yellow-200";
}

type AnnotationSource = { source: string; version: string | null };
type GroupedAnnotation = {
  go_id: string;
  name: string | null;
  aspect: string | null;
  evidence_codes: string[];
  sources: AnnotationSource[];
};

// "GOA 2025-03" (version optional, null-safe).
function formatSource({ source, version }: AnnotationSource): string {
  return version ? `${source} ${version}` : source;
}

function groupAnnotations(anns: ProteinAnnotation[]): Map<string, GroupedAnnotation> {
  const map = new Map<string, GroupedAnnotation>();
  for (const a of anns) {
    const source: AnnotationSource = { source: a.annotation_set_source, version: a.annotation_set_version };
    const existing = map.get(a.go_id);
    if (existing) {
      if (a.evidence_code && !existing.evidence_codes.includes(a.evidence_code))
        existing.evidence_codes.push(a.evidence_code);
      if (!existing.sources.some((s) => s.source === source.source && s.version === source.version))
        existing.sources.push(source);
    } else {
      map.set(a.go_id, {
        go_id: a.go_id,
        name: a.name,
        aspect: a.aspect,
        evidence_codes: a.evidence_code ? [a.evidence_code] : [],
        sources: [source],
      });
    }
  }
  return map;
}

export function PredictionTable({ preds, knownByGoId }: {
  preds: Prediction[];
  knownByGoId: Map<string, GroupedAnnotation>;
}) {
  const locale = useLocale();
  // Every candidate of a retrieval arm has one; a set written by
  // ingest_method_predictions has none, because nothing was retrieved. The
  // page used to assume it was always there and crashed on .toFixed().
  const hasDistance = preds.some((p) => p.distance != null);
  const hasAlignment = preds.some((p) => p.identity_nw != null);
  const hasTaxonomy = preds.some((p) => p.taxonomic_relation != null);
  const hasReranker = preds.some((p) => p.vote_count != null);
  // A scored set's rows carry the score it stored, the one the evaluator reads.
  // A candidate set has none: it is scored by a job, never in the browser.
  const hasScore = preds.some((p) => p.score != null);
  const scoreOf = (p: Prediction) => p.score ?? 0;
  // A native method (ADR-D60): its own score and a witness instead of a donor and a distance.
  const isMethod = !hasDistance && preds.some((p) => p.method_score != null);
  const hasDetail = hasAlignment || hasTaxonomy || hasReranker;
  const [expanded, setExpanded] = useState<string | null>(null);

  // Sort: by stored score desc on a scored set, else by distance asc. With no
  // distance the comparator returned NaN for every pair, which leaves the
  // order up to the sort implementation: by GO id instead, so the same set
  // reads the same way twice.
  const sorted = hasScore
    ? [...preds].sort((a, b) => scoreOf(b) - scoreOf(a) || a.go_id.localeCompare(b.go_id))
    : isMethod
    ? [...preds].sort((a, b) => (b.method_score ?? 0) - (a.method_score ?? 0) || a.go_id.localeCompare(b.go_id))
    : hasDistance
      ? [...preds].sort((a, b) => (a.distance ?? 0) - (b.distance ?? 0))
      : [...preds].sort((a, b) => a.go_id.localeCompare(b.go_id));

  // Tailwind requires complete class strings — no dynamic construction
  const gridClass = isMethod
    ? "grid-cols-[60px_90px_1fr_210px_65px]"
    : hasScore
    ? "grid-cols-[60px_90px_1fr_110px_60px_60px_65px]"
    : "grid-cols-[90px_1fr_110px_60px_60px_65px]";
  const methodScore = (p: Prediction) => p.method_score ?? 0;

  return (
    <div className="rounded-md border bg-white text-xs">
      {/* Desktop header */}
      <div className={`hidden lg:grid ${gridClass} gap-x-3 border-b bg-slate-50 px-3 py-1.5 font-semibold uppercase tracking-wide text-slate-600`}>
        {(hasScore || isMethod) && <div>Score</div>}
        <div>GO ID</div>
        <div>Name</div>
        {isMethod ? <div>Witness</div> : <div>Via (ref)</div>}
        {!isMethod && <div>Pred. ev.</div>}
        <div>Known ev.</div>
        {!isMethod && <div>Dist</div>}
      </div>

      {sorted.length === 0 ? (
        <p className="px-3 py-3 text-slate-300">—</p>
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
                    <span className={`font-mono text-[10px] ${scoreColor(scoreOf(pred))}`}>
                      {scoreOf(pred).toFixed(3)}
                    </span>
                  )}
                  {isMethod && !hasScore && (
                    <span className={`font-mono text-[10px] ${scoreColor(methodScore(pred))}`}>
                      {methodScore(pred).toFixed(3)}
                    </span>
                  )}
                  {!isMethod && <span className="font-mono text-slate-500 text-[10px]">{pred.distance != null ? pred.distance.toFixed(4) : "—"}</span>}
                  {hasDetail && <span className="text-slate-300 text-[10px]">{isExpanded ? "▲" : "▼"}</span>}
                </div>
              </div>
              <p className="text-slate-700 leading-snug text-xs mb-1">{pred.name ?? "—"}</p>
              <div className="flex flex-wrap items-center gap-2 text-[10px]">
                {isMethod ? <MethodWitness pred={pred} /> : <span className="text-slate-600">via</span>}
                {!isMethod && <Link
                  href={`/${locale}/instrument/proteins/${pred.ref_protein_accession}`}
                  className="font-mono text-blue-500 hover:underline"
                  onClick={(e) => e.stopPropagation()}
                >{pred.ref_protein_accession}</Link>}
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
                <span className={`font-mono ${scoreColor(scoreOf(pred))}`}>
                  {scoreOf(pred).toFixed(3)}
                </span>
              )}
              {isMethod && !hasScore && (
                <span className={`font-mono ${scoreColor(methodScore(pred))}`}>{methodScore(pred).toFixed(3)}</span>
              )}
              <span className="font-mono text-blue-600">{pred.go_id}</span>
              <span className="text-slate-700 leading-snug">{pred.name ?? "—"}</span>
              {isMethod ? (
                <MethodWitness pred={pred} />
              ) : (
              <div className="flex items-start gap-1">
                <Link
                  href={`/${locale}/instrument/proteins/${pred.ref_protein_accession}`}
                  className="font-mono text-blue-500 hover:underline"
                  onClick={(e) => e.stopPropagation()}
                >{pred.ref_protein_accession}</Link>
                {hasDetail && <span className="text-slate-300 text-[10px] mt-0.5">{isExpanded ? "▲" : "▼"}</span>}
              </div>
              )}
              {!isMethod && (
                <div>
                  {pred.evidence_code ? (
                    <span className={`rounded border px-1 py-0.5 text-[10px] font-mono font-medium ${evidenceBadgeClass(pred.evidence_code)}`}>
                      {pred.evidence_code}
                    </span>
                  ) : <span className="text-slate-300">—</span>}
                </div>
              )}
              <div className="flex flex-wrap gap-0.5">
                {knownAnn ? knownAnn.evidence_codes.map((ec) => (
                  <span key={ec} className={`rounded border px-1 py-0.5 text-[10px] font-mono font-medium ${evidenceBadgeClass(ec)}`}>
                    {ec}
                  </span>
                )) : <span className="text-slate-300">—</span>}
              </div>
              {!isMethod && (
                <span className="font-mono text-slate-500">{pred.distance != null ? pred.distance.toFixed(4) : "—"}</span>
              )}
            </div>

            {/* Expanded: alignment + taxonomy + reranker detail */}
            {isExpanded && hasDetail && (
              <div className="border-b bg-slate-50 px-4 py-3 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6">
                {hasAlignment && (
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-600 mb-2">
                      Alignment — query vs {pred.ref_protein_accession}
                    </p>
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="text-slate-600">
                          <th scope="col" className="text-left font-medium pr-4 pb-1">Metric</th>
                          <th scope="col" className="text-right font-medium pr-4 pb-1">NW (global)</th>
                          <th scope="col" className="text-right font-medium pb-1">SW (local)</th>
                        </tr>
                      </thead>
                      <tbody className="font-mono">
                        <tr><td className="pr-4 text-slate-500 font-sans py-0.5">Identity</td><td className="text-right pr-4">{pct(pred.identity_nw)}</td><td className="text-right">{pct(pred.identity_sw)}</td></tr>
                        <tr><td className="pr-4 text-slate-500 font-sans py-0.5">Similarity</td><td className="text-right pr-4">{pct(pred.similarity_nw)}</td><td className="text-right">{pct(pred.similarity_sw)}</td></tr>
                        <tr><td className="pr-4 text-slate-500 font-sans py-0.5">Score</td><td className="text-right pr-4">{pred.alignment_score_nw?.toFixed(0) ?? "—"}</td><td className="text-right">{pred.alignment_score_sw?.toFixed(0) ?? "—"}</td></tr>
                        <tr><td className="pr-4 text-slate-500 font-sans py-0.5">Gaps</td><td className="text-right pr-4">{pct(pred.gaps_pct_nw)}</td><td className="text-right">{pct(pred.gaps_pct_sw)}</td></tr>
                        <tr><td className="pr-4 text-slate-500 font-sans py-0.5">Aln length</td><td className="text-right pr-4">{pred.alignment_length_nw ?? "—"}</td><td className="text-right">{pred.alignment_length_sw ?? "—"}</td></tr>
                        <tr><td className="pr-4 text-slate-500 font-sans py-0.5">Seq length</td><td className="text-right pr-4">{pred.length_query ?? "—"} (q)</td><td className="text-right">{pred.length_ref ?? "—"} (r)</td></tr>
                      </tbody>
                    </table>
                  </div>
                )}
                {hasTaxonomy && (
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-600 mb-2">
                      Taxonomy — query vs {pred.ref_protein_accession}
                    </p>
                    <div className="space-y-1.5 text-xs">
                      <div className="flex justify-between">
                        <span className="text-slate-500">Relation</span>
                        <span className={`rounded px-1.5 py-0.5 font-medium ${RELATION_COLORS[pred.taxonomic_relation ?? ""] ?? "bg-slate-50 text-slate-500"}`}>
                          {pred.taxonomic_relation ?? "—"}
                        </span>
                      </div>
                      <div className="flex justify-between"><span className="text-slate-500">Distance</span><span className="font-mono">{pred.taxonomic_distance ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-slate-500">Common ancestors</span><span className="font-mono">{pred.taxonomic_common_ancestors ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-slate-500">LCA taxid</span><span className="font-mono">{pred.taxonomic_lca ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-slate-500">Query taxid</span><span className="font-mono">{pred.query_taxonomy_id ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-slate-500">Ref taxid</span><span className="font-mono">{pred.ref_taxonomy_id ?? "—"}</span></div>
                    </div>
                  </div>
                )}
                {hasReranker && (
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-600 mb-2">
                      Re-ranker features
                    </p>
                    <div className="space-y-1.5 text-xs">
                      <div className="flex justify-between"><span className="text-slate-500">Vote count</span><span className="font-mono">{pred.vote_count ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-slate-500">K position</span><span className="font-mono">{pred.k_position ?? "—"}</span></div>
                      <div className="flex justify-between"><span className="text-slate-500">GO term frequency</span><span className="font-mono">{pred.go_term_frequency != null ? pred.go_term_frequency.toFixed(4) : "—"}</span></div>
                      <div className="flex justify-between"><span className="text-slate-500">Ref annotation density</span><span className="font-mono">{pred.ref_annotation_density != null ? pred.ref_annotation_density.toFixed(4) : "—"}</span></div>
                      <div className="flex justify-between"><span className="text-slate-500">Neighbor dist std</span><span className="font-mono">{pred.neighbor_distance_std != null ? pred.neighbor_distance_std.toFixed(4) : "—"}</span></div>
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
}: {
  accession: string;
  inDb: boolean;
  predictions: Prediction[];
  annotations: ProteinAnnotation[];
  loading: boolean;
  onClose: () => void;
  ontologySnapshotId: string | null;
}) {
  const locale = useLocale();
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
    <div className="mt-4 rounded-lg border bg-slate-50 p-4">
      <div className="flex flex-wrap items-center gap-2 sm:gap-3 mb-4">
        <span className="font-mono font-semibold text-slate-900">{accession}</span>
        {inDb && (
          <Link href={`/${locale}/instrument/proteins/${accession}`} className="text-xs text-blue-500 hover:underline">
            View protein →
          </Link>
        )}
        {annotations.length > 0 && predictions.length > 0 && (
          <span className="text-xs text-green-700 font-medium sm:ml-auto">
            {totalMatches} / {totalUniquePredicted} match known
          </span>
        )}
        {ontologySnapshotId && predictions.length > 0 && (
          <button onClick={toggleGraph} className="rounded border bg-white px-2 py-1 text-xs hover:bg-slate-50">
            {loadingGraph ? "Loading…" : showGraph ? "Hide graph" : "GO graph"}
          </button>
        )}
        <button onClick={onClose} className="text-slate-600 hover:text-slate-600 text-lg leading-none ml-auto sm:ml-2">×</button>
      </div>

      {loading && <p className="text-sm text-slate-600">Loading…</p>}

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
        <p className="text-sm text-slate-600">No data found.</p>
      )}

      {!loading && aspects.map((asp) => {
        const preds = predByAspect[asp];
        const uniquePredCount = new Set(preds.map((p) => p.go_id)).size;
        const knownInAspect = Array.from(knownByGoId.values()).filter((a) => a.aspect === asp);
        return (
          <div key={asp} className="mb-5 last:mb-0">
            <div className="flex items-center gap-2 mb-2">
              <AspectBadge aspect={asp} />
              <span className="text-xs font-semibold text-slate-600">{ASPECT_LABELS[asp]}</span>
              <span className="text-xs text-slate-600 ml-1">{uniquePredCount} predicted · {knownInAspect.length} known</span>
            </div>
            <PredictionTable preds={preds} knownByGoId={knownByGoId} />
          </div>
        );
      })}

      {/* Known terms with no matching prediction */}
      {!loading && uncoveredKnown.length > 0 && (
        <div className="mt-4">
          <p className="text-xs font-semibold text-slate-500 mb-2">
            Known annotations not covered by any prediction ({uncoveredKnown.length})
          </p>
          <div className="overflow-x-auto rounded-md border bg-white text-xs">
            <div className="grid grid-cols-[90px_1fr_80px] gap-2 border-b bg-slate-50 px-3 py-1.5 font-semibold uppercase tracking-wide text-slate-600">
              <div>GO ID</div><div>Name</div><div>Evidence</div>
            </div>
            {uncoveredKnown.map((ann) => (
              <div key={ann.go_id} className="grid grid-cols-[90px_1fr_80px] gap-2 border-b px-3 py-2 last:border-0 items-start">
                <span className="font-mono text-blue-600 pt-0.5">{ann.go_id}</span>
                <span className="text-slate-700 leading-snug">{ann.name ?? "—"}</span>
                <div className="flex flex-col items-end gap-0.5">
                  <div className="flex flex-wrap gap-0.5 justify-end">
                    {ann.evidence_codes.map((ec) => (
                      <span key={ec} className={`rounded border px-1 py-0.5 text-[10px] font-mono font-medium ${evidenceBadgeClass(ec)}`}>
                        {ec}
                      </span>
                    ))}
                  </div>
                  <div className="flex flex-wrap gap-0.5 justify-end">
                    {ann.sources.map((s) => (
                      <span key={`${s.source}-${s.version ?? ""}`} className="rounded bg-slate-100 px-1 py-0.5 text-[10px] text-slate-500">
                        {formatSource(s)}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function DownloadButton({ setId }: { setId: string }) {
  const [open, setOpen] = useState(false);
  const [aspect, setAspect] = useState("");
  const [maxDist, setMaxDist] = useState("");

  const apiBase = process.env.NEXT_PUBLIC_API_URL ?? "";

  function buildRawUrl() {
    const params = new URLSearchParams();
    if (aspect) params.set("aspect", aspect);
    if (maxDist) params.set("max_distance", maxDist);
    const qs = params.toString();
    return `${apiBase}/embeddings/prediction-sets/${setId}/predictions.tsv${qs ? `?${qs}` : ""}`;
  }

  return (
    <div className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-1.5 rounded-md border bg-white px-3 py-1.5 text-sm font-medium text-slate-700 shadow-sm hover:bg-slate-50"
      >
        ↓ Download TSV
      </button>

      {open && (
        <div className="absolute right-0 top-10 z-20 w-64 rounded-lg border bg-white p-4 shadow-lg">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-3">Download options</p>
          <label className="block text-xs text-slate-600 mb-1">GO Aspect</label>
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

          <label className="block text-xs text-slate-600 mb-1">Max distance</label>
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
          <p className="text-[11px] text-slate-500 mb-3">
            The candidates with their evidence. A set&apos;s stored scores, in CAFA format, are at{" "}
            <code className="font-mono">predictions-cafa.tsv</code>.
          </p>

          <a
            href={buildRawUrl()}
            download={`predictions_${setId.slice(0, 8)}.tsv`}
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
const tToast = useTranslations("toasts");
  const locale = useLocale();
  const toast = useToast();
  const [activeTab, setActiveTab] = useState<Tab>("proteins");
  const [annotationSetId, setAnnotationSetId] = useState<string | null>(null);
  const [ontologySnapshotId, setOntologySnapshotId] = useState<string | null>(null);
  const [limitPerEntry, setLimitPerEntry] = useState<number | null>(null);
  // A scored set: the candidate set holding its rows, and the configuration it read.
  const [scoredFrom, setScoredFrom] = useState<string | null>(null);
  const [setScoringConfigId, setSetScoringConfigId] = useState<string | null>(null);
  const [setScoringConfigName, setSetScoringConfigName] = useState<string | null>(null);
  // A candidate set: the scored sets made from it, one per scoring job.
  const [scoredSets, setScoredSets] = useState<ScoredSetSummary[]>([]);
  // Until the set's record arrives, the page says it is loading rather than
  // stating anything about the set: on a large set the record takes seconds.
  const [setLoaded, setSetLoaded] = useState(false);
  const [countsProvisional, setCountsProvisional] = useState(false);
  // A failure loading the prediction set means this whole page has no
  // subject to render. A missing id resolves to the not-found boundary;
  // any other failure is re-thrown to the route error boundary. Both
  // signals are raised during render (not from the async callback) so
  // React actually routes them to a boundary.
  const [notFoundFlag, setNotFoundFlag] = useState(false);
  const [fatalError, setFatalError] = useState<Error | null>(null);
  if (notFoundFlag) notFound();
  if (fatalError) throw fatalError;

  // Proteins tab
  const [proteins, setProteins] = useState<{ accession: string; go_count: number; min_distance: number | null; annotation_count: number; match_count: number; in_db: boolean }[]>([]);
  const [proteinTotal, setProteinTotal] = useState(0);
  const [proteinOffset, setProteinOffset] = useState(0);
  const [proteinSearch, setProteinSearch] = useState("");
  const [proteinSearchInput, setProteinSearchInput] = useState("");
  // True from the first render: the first page of proteins is being counted, and
  // "0 proteins" before it arrives would read as a fact about the set.
  const [loadingProteins, setLoadingProteins] = useState(true);

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
        setScoredFrom(ps.scored_from ?? null);
        setSetScoringConfigId(ps.scoring_config_id ?? null);
        setSetScoringConfigName(ps.scoring_config_name ?? null);
        setScoredSets(ps.scored_sets ?? []);
        setCountsProvisional(Boolean(ps.counts_provisional));
        setSetLoaded(true);
      })
      .catch((e: unknown) => {
        // A missing id is a 404: hand it to not-found, not the generic
        // error screen. Everything else (auth, network, 500) is fatal
        // for this page and belongs on the error boundary.
        if (e instanceof ApiError && e.status === 404) {
          setNotFoundFlag(true);
          return;
        }
        setFatalError(e instanceof Error ? e : new Error(String(e)));
      });
  }, [setId]);

  async function loadProteins(offset = 0, search = proteinSearch) {
    setLoadingProteins(true);
    try {
      const res = await getPredictionSetProteins(setId, { search: search || undefined, limit: PAGE_SIZE, offset });
      setProteins(res.items);
      setProteinTotal(res.total);
      setProteinOffset(offset);
    } catch (e: any) {
      toast(e.message ?? tToast("loadProteinsFailed"), "error");
    } finally {
      setLoadingProteins(false);
    }
  }

  async function loadDistribution() {
    setLoadingDist(true);
    try {
      setDistribution(await getGoTermDistribution(setId));
    } catch (e: any) {
      toast(e.message ?? tToast("loadDistributionFailed"), "error");
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
      toast(e.message ?? tToast("loadDetailFailed"), "error");
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
        <div>
          <h1 className="text-xl font-semibold mt-2">
            Prediction Set <span className="font-mono text-base text-slate-500">{shortId(setId)}…</span>
          </h1>
          {limitPerEntry != null && (
            <p className="text-xs text-slate-600 mt-0.5">k = {limitPerEntry}</p>
          )}
        </div>
        {setLoaded && countsProvisional ? (
          <p className="text-xs text-amber-800" data-testid="set-provisional-notice">
            {PROVISIONAL_NOTE}
          </p>
        ) : null}
        {!setLoaded ? (
          <p className="text-xs text-slate-600" data-testid="set-loading-notice">
            {LOADING_NOTE}
          </p>
        ) : scoredFrom ? (
          <div className="flex flex-wrap items-center gap-2">
            {/* A scored set's scores are stored, and they are the ones the
              evaluator reads, so nothing is recomputed here. */}
            <p className="text-xs text-slate-600" data-testid="scored-set-notice">
              Scored set: the scores shown are the ones stored with configuration{" "}
              <span className="font-medium">
                {setScoringConfigName ?? setScoringConfigId ?? "unknown"}
              </span>
              , over the candidates of{" "}
              <Link
                href={`/${locale}/instrument/functional-annotation/${scoredFrom}`}
                className="font-mono text-blue-600 hover:underline"
              >
                {scoredFrom.slice(0, 8)}
              </Link>
              .
            </p>
            <DownloadButton setId={setId} />
          </div>
        ) : (
          <div className="flex flex-wrap items-start gap-2">
            {/* A candidate set holds neighbours and their evidence, not scores.
              It is scored by a job, each scoring a set of its own, and the
              scores the evaluator reads are the ones those sets stored. */}
            <div className="space-y-1 text-xs text-slate-600" data-testid="candidate-set-notice">
              <p>
                Candidate set: the neighbours and their evidence. Scoring is a job of its own,{" "}
                <code className="font-mono">score_prediction_set</code>, and each scoring is a set
                that stores its scores.
              </p>
              {scoredSets.length === 0 ? (
                <p>No scored set has been made from it yet.</p>
              ) : (
                <ul className="flex flex-wrap gap-x-3 gap-y-1">
                  {scoredSets.map((s) => (
                    <li key={s.id}>
                      <Link
                        href={`/${locale}/instrument/functional-annotation/${s.id}`}
                        className="text-blue-600 hover:underline"
                        title={s.id}
                      >
                        {s.scoring_config_name ?? s.scoring_config_id ?? "unknown"}{" "}
                        <span className="font-mono">{s.id.slice(0, 8)}</span>
                      </Link>
                    </li>
                  ))}
                </ul>
              )}
            </div>
            <DownloadButton setId={setId} />
          </div>
        )}
      </div>

      <div className="flex gap-1 border-b mb-6 overflow-x-auto">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setActiveTab(t.key)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === t.key
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-slate-500 hover:text-slate-700"
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
            <div className="text-xl font-bold text-slate-900 tabular-nums">{formatCount(proteinTotal)}</div>
            <div className="text-xs text-slate-500">Proteins</div>
          </div>
          {(["P", "F", "C"] as const).map((aspect) => (
            <div key={aspect} className="rounded-lg border bg-white p-3 text-center">
              <div className="text-xl font-bold text-slate-900 tabular-nums">
                {formatCount((distribution.aspect_totals[aspect] ?? 0))}
              </div>
              <div className="text-xs text-slate-500">{ASPECT_LABELS[aspect]}</div>
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
              <button type="submit" className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-slate-50">
                Filter
              </button>
              {proteinSearch && (
                <button type="button" onClick={() => { setProteinSearchInput(""); setProteinSearch(""); loadProteins(0, ""); }}
                  className="rounded-md border bg-white px-3 py-1.5 text-sm text-slate-500 hover:bg-slate-50">
                  Clear
                </button>
              )}
            </form>
            <span className="text-sm text-slate-600">
              {loadingProteins ? "Counting proteins\u2026" : `${formatCount(proteinTotal)} proteins`}
            </span>
          </div>

          {/* Mobile card list */}
          <div className="lg:hidden space-y-2">
            {loadingProteins && Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="rounded-lg border bg-white p-4 shadow-sm animate-pulse">
                <div className="h-4 bg-slate-200 rounded w-1/3 mb-2" />
                <div className="h-3 bg-slate-100 rounded w-2/3" />
              </div>
            ))}
            {!loadingProteins && proteins.length === 0 && (
              <div className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-slate-600 shadow-sm">No proteins found.</div>
            )}
            {!loadingProteins && proteins.map((p) => (
              <div key={p.accession} className="rounded-lg border bg-white shadow-sm overflow-hidden">
                <div
                  className={`p-4 cursor-pointer transition-colors ${
                    selectedAccession === p.accession ? "bg-blue-50" : "hover:bg-slate-50"
                  }`}
                  onClick={() => selectProtein(p.accession, p.in_db)}
                >
                  <div className="flex items-center justify-between mb-1">
                    <div className="flex items-center gap-1.5">
                      <span className={`inline-block w-2 h-2 rounded-full ${
                        p.min_distance == null ? "bg-slate-300"
                        : p.min_distance < 0.3 ? "bg-green-500"
                        : p.min_distance < 0.6 ? "bg-amber-400"
                        : "bg-red-500"
                      }`} title={`min distance: ${p.min_distance?.toFixed(4) ?? "N/A"}`} />
                      {p.in_db ? (
                        <Link href={`/${locale}/instrument/proteins/${p.accession}`} className="font-mono text-sm text-blue-600 hover:underline" onClick={(e) => e.stopPropagation()}>
                          {p.accession}
                        </Link>
                      ) : (
                        <span className="font-mono text-sm text-slate-700">{p.accession}</span>
                      )}
                    </div>
                    <span className="text-xs text-slate-500">{p.go_count} predicted</span>
                  </div>
                  <div className="flex gap-4 text-xs text-slate-500">
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
                    />
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Desktop table */}
          <div className="hidden lg:block overflow-x-auto rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[160px_90px_120px_120px] gap-2 border-b bg-slate-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-500">
              <div>Accession</div>
              <div>Predicted</div>
              <div>Min Distance</div>
              <div>Known / Pred.</div>
            </div>

            {loadingProteins && Array.from({ length: 8 }).map((_, i) => <SkeletonTableRow key={i} cols={5} />)}

            {!loadingProteins && proteins.length === 0 && (
              <div className="px-4 py-12 text-center text-sm text-slate-600">No proteins found.</div>
            )}

            {!loadingProteins && proteins.map((p) => (
              <div key={p.accession}>
                <div
                  className={`grid grid-cols-[160px_90px_120px_120px] gap-2 border-b px-4 py-3 text-sm items-center cursor-pointer transition-colors ${
                    selectedAccession === p.accession ? "bg-blue-50" : "hover:bg-slate-50"
                  }`}
                  onClick={() => selectProtein(p.accession, p.in_db)}
                >
                  <div className="flex items-center gap-2">
                    <span className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${
                      p.min_distance == null ? "bg-slate-300"
                      : p.min_distance < 0.3 ? "bg-green-500"
                      : p.min_distance < 0.6 ? "bg-amber-400"
                      : "bg-red-500"
                    }`} title={`min distance: ${p.min_distance?.toFixed(4) ?? "N/A"}`} />
                    {p.in_db ? (
                      <Link
                        href={`/${locale}/instrument/proteins/${p.accession}`}
                        className="font-mono text-xs text-blue-600 hover:underline"
                        onClick={(e) => e.stopPropagation()}
                      >
                        {p.accession}
                      </Link>
                    ) : (
                      <span className="font-mono text-xs text-slate-700">{p.accession}</span>
                    )}
                  </div>
                  <div className="text-slate-700 font-medium">{p.go_count}</div>
                  <div className="text-slate-600 font-mono text-xs">{p.min_distance?.toFixed(4) ?? "—"}</div>
                  <div className="text-sm font-mono">
                    {p.annotation_count > 0
                      ? <span className="text-slate-700">{p.annotation_count}</span>
                      : <span className="text-slate-300">0</span>}
                    <span className="text-slate-300 mx-1">/</span>
                    <span className="text-slate-700">{p.go_count}</span>
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
                    />
                  </div>
                )}
              </div>
            ))}
          </div>

          {totalPages > 1 && (
            <div className="mt-4 flex items-center justify-between text-sm text-slate-500">
              <span>Page {currentPage} of {totalPages}</span>
              <div className="flex gap-2">
                <button onClick={() => loadProteins(proteinOffset - PAGE_SIZE)} disabled={proteinOffset === 0}
                  className="rounded-md border bg-white px-3 py-1.5 hover:bg-slate-50 disabled:opacity-40">Previous</button>
                <button onClick={() => loadProteins(proteinOffset + PAGE_SIZE)} disabled={proteinOffset + PAGE_SIZE >= proteinTotal}
                  className="rounded-md border bg-white px-3 py-1.5 hover:bg-slate-50 disabled:opacity-40">Next</button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── GO Distribution ── */}
      {activeTab === "distribution" && (
        <div>
          {loadingDist && <p className="text-sm text-slate-600">{LOADING_NOTE}</p>}

          {distribution && (
            <div className="space-y-6">
              <div className="grid grid-cols-3 gap-3">
                {(["F", "P", "C"] as const).map((asp) => (
                  <div key={asp} className="rounded-lg border bg-white p-4 shadow-sm">
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">{ASPECT_LABELS[asp]}</p>
                    <p className="mt-1 text-2xl font-bold text-slate-900">
                      {formatCount((distribution.aspect_totals[asp] ?? 0))}
                    </p>
                    <p className="text-xs text-slate-600 mt-0.5">predictions</p>
                  </div>
                ))}
              </div>

              {(["F", "P", "C"] as const).map((asp) => {
                const terms = distribution.by_aspect[asp] ?? [];
                if (terms.length === 0) return null;
                const maxCount = terms[0]?.count ?? 1;
                return (
                  <div key={asp}>
                    <p className="text-sm font-semibold text-slate-700 mb-3">
                      {ASPECT_LABELS[asp]}
                      <span className="ml-2 text-xs font-normal text-slate-600">top {terms.length} terms</span>
                    </p>
                    <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
                      {terms.map((t) => (
                        <div key={t.go_id} className="flex items-center gap-3 border-b px-4 py-2.5 last:border-0">
                          <span className="font-mono text-xs text-blue-600 w-24 shrink-0">{t.go_id}</span>
                          <span className="text-xs text-slate-700 flex-1 truncate">{t.name ?? "—"}</span>
                          <div className="flex items-center gap-2 shrink-0">
                            <div className="w-24 h-1.5 rounded-full bg-slate-100 overflow-hidden">
                              <div
                                className="h-1.5 rounded-full bg-blue-400"
                                style={{ width: `${Math.round((t.count / maxCount) * 100)}%` }}
                              />
                            </div>
                            <span className="text-xs text-slate-500 w-12 text-right">{formatCount(t.count)}</span>
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
