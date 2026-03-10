"use client";

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import {
  getPredictionSet,
  getPredictionSetProteins,
  getProteinPredictions,
  getProteinAnnotations,
  getGoTermDistribution,
  ProteinAnnotation,
} from "@/lib/api";

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

type Prediction = { go_id: string; name: string | null; aspect: string | null; distance: number; ref_protein_accession: string; qualifier: string | null; evidence_code: string | null };

function ProteinDetail({
  accession,
  inDb,
  predictions,
  annotations,
  loading,
  onClose,
}: {
  accession: string;
  inDb: boolean;
  predictions: Prediction[];
  annotations: ProteinAnnotation[];
  loading: boolean;
  onClose: () => void;
}) {
  const annotatedGoIds = new Set(annotations.map((a) => a.go_id));
  const predictedGoIds = new Set(predictions.map((p) => p.go_id));

  const predByAspect: Record<string, Prediction[]> = { F: [], P: [], C: [], other: [] };
  for (const p of predictions) {
    const key = p.aspect && predByAspect[p.aspect] ? p.aspect : "other";
    predByAspect[key].push(p);
  }

  const annByAspect: Record<string, ProteinAnnotation[]> = { F: [], P: [], C: [], other: [] };
  for (const a of annotations) {
    const key = a.aspect && annByAspect[a.aspect] ? a.aspect : "other";
    annByAspect[key].push(a);
  }

  const aspects = (["F", "P", "C"] as const).filter(
    (asp) => predByAspect[asp].length > 0 || annByAspect[asp].length > 0
  );

  const totalMatches = predictions.filter((p) => annotatedGoIds.has(p.go_id)).length;

  return (
    <div className="mt-4 rounded-lg border bg-gray-50 p-4">
      <div className="flex items-center gap-3 mb-4">
        <span className="font-mono font-semibold text-gray-900">{accession}</span>
        {inDb && (
          <Link href={`/proteins/${accession}`} className="text-xs text-blue-500 hover:underline">
            View protein →
          </Link>
        )}
        {annotations.length > 0 && predictions.length > 0 && (
          <span className="text-xs text-green-700 font-medium ml-auto">
            {totalMatches} / {predictions.length} predicted match known annotations
          </span>
        )}
        <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-lg leading-none ml-2">×</button>
      </div>

      {loading && <p className="text-sm text-gray-400">Loading…</p>}

      {!loading && predictions.length === 0 && annotations.length === 0 && (
        <p className="text-sm text-gray-400">No data found.</p>
      )}

      {!loading && aspects.map((asp) => {
        const preds = predByAspect[asp];
        const anns = annByAspect[asp];
        return (
          <div key={asp} className="mb-5 last:mb-0">
            <div className="flex items-center gap-2 mb-2">
              <AspectBadge aspect={asp} />
              <span className="text-xs font-semibold text-gray-600">{ASPECT_LABELS[asp]}</span>
              <span className="text-xs text-gray-400 ml-1">{preds.length} predicted · {anns.length} known</span>
            </div>
            <div className="grid grid-cols-2 gap-3">
              {/* Predictions */}
              <div className="overflow-hidden rounded-md border bg-white">
                <div className="grid grid-cols-[90px_1fr_75px] gap-1 border-b bg-gray-50 px-3 py-1.5 text-xs font-semibold uppercase tracking-wide text-gray-400">
                  <div>GO ID</div><div>Name</div><div>Distance</div>
                </div>
                {preds.length === 0 ? (
                  <p className="px-3 py-3 text-xs text-gray-300">—</p>
                ) : preds.map((pred, i) => (
                  <div key={i} className={`grid grid-cols-[90px_1fr_75px] gap-1 border-b px-3 py-2 text-xs last:border-0 items-center ${annotatedGoIds.has(pred.go_id) ? "bg-green-50" : ""}`}>
                    <span className="font-mono text-blue-600">{pred.go_id}</span>
                    <span className="text-gray-700 truncate" title={pred.name ?? ""}>{pred.name ?? "—"}</span>
                    <span className="font-mono text-gray-500">{pred.distance.toFixed(4)}</span>
                  </div>
                ))}
              </div>

              {/* Known annotations */}
              <div className="overflow-hidden rounded-md border bg-white">
                <div className="grid grid-cols-[90px_1fr_60px] gap-1 border-b bg-gray-50 px-3 py-1.5 text-xs font-semibold uppercase tracking-wide text-gray-400">
                  <div>GO ID</div><div>Name</div><div>Evidence</div>
                </div>
                {anns.length === 0 ? (
                  <p className="px-3 py-3 text-xs text-gray-300">—</p>
                ) : anns.map((ann, i) => (
                  <div key={i} className={`grid grid-cols-[90px_1fr_60px] gap-1 border-b px-3 py-2 text-xs last:border-0 items-center ${predictedGoIds.has(ann.go_id) ? "bg-green-50" : ""}`}>
                    <span className="font-mono text-blue-600">{ann.go_id}</span>
                    <span className="text-gray-700 truncate" title={ann.name ?? ""}>{ann.name ?? "—"}</span>
                    <span className="text-gray-500">{ann.evidence_code ?? "—"}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default function PredictionSetDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id: setId } = use(params);
  const toast = useToast();
  const [activeTab, setActiveTab] = useState<Tab>("proteins");
  const [annotationSetId, setAnnotationSetId] = useState<string | null>(null);

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
      .then((ps) => setAnnotationSetId(ps.annotation_set_id))
      .catch(() => {});
  }, [setId]);

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
    if (activeTab === "proteins") loadProteins(0, "");
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
      <div className="mb-6">
        <Link href="/functional-annotation" className="text-sm text-gray-400 hover:text-gray-600">← Functional Annotation</Link>
        <h1 className="text-xl font-semibold mt-2">
          Prediction Set <span className="font-mono text-base text-gray-500">{shortId(setId)}…</span>
        </h1>
      </div>

      <div className="flex gap-1 border-b mb-6">
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

      {/* ── Proteins ── */}
      {activeTab === "proteins" && (
        <div>
          <div className="flex items-center gap-3 mb-4">
            <form onSubmit={handleProteinSearch} className="flex gap-2">
              <input
                type="text"
                value={proteinSearchInput}
                onChange={(e) => setProteinSearchInput(e.target.value)}
                placeholder="Filter by accession…"
                className="rounded-md border px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-56"
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
            <span className="ml-auto text-sm text-gray-400">{proteinTotal.toLocaleString()} proteins</span>
          </div>

          <div className="overflow-hidden rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[160px_90px_120px_90px_90px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>Accession</div>
              <div>Predicted</div>
              <div>Min Distance</div>
              <div>Known</div>
              <div>Matches</div>
            </div>

            {loadingProteins && Array.from({ length: 8 }).map((_, i) => <SkeletonTableRow key={i} cols={5} />)}

            {!loadingProteins && proteins.length === 0 && (
              <div className="px-4 py-12 text-center text-sm text-gray-400">No proteins found.</div>
            )}

            {!loadingProteins && proteins.map((p) => (
              <div key={p.accession}>
                <div
                  className={`grid grid-cols-[160px_90px_120px_90px_90px] gap-2 border-b px-4 py-3 text-sm items-center cursor-pointer transition-colors ${
                    selectedAccession === p.accession ? "bg-blue-50" : "hover:bg-gray-50"
                  }`}
                  onClick={() => selectProtein(p.accession, p.in_db)}
                >
                  <div className="flex items-center gap-2">
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
                  <div className={`text-sm ${p.annotation_count > 0 ? "text-gray-700" : "text-gray-300"}`}>
                    {p.annotation_count > 0 ? p.annotation_count : "—"}
                  </div>
                  <div className={`font-medium text-sm ${p.match_count > 0 ? "text-green-700" : "text-gray-300"}`}>
                    {p.match_count > 0 ? p.match_count : "—"}
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
                    <div className="overflow-hidden rounded-lg border bg-white shadow-sm">
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
