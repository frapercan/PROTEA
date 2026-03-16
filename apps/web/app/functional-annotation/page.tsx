"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import {
  listEmbeddingConfigs,
  launchPredictGoTerms,
  listPredictionSets,
  deletePredictionSet,
  listAnnotationSets,
  listOntologySnapshots,
  listQuerySets,
  EmbeddingConfig,
  PredictionSet,
  AnnotationSet,
  OntologySnapshot,
  QuerySet,
} from "@/lib/api";

type Tab = "predict" | "results";

function formatDate(iso?: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
}

function shortId(id: string) {
  return id.slice(0, 8);
}

const inputClass = "w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
const labelClass = "block text-sm font-medium text-gray-700 mb-1";

export default function FunctionalAnnotationPage() {
  const toast = useToast();
  const [activeTab, setActiveTab] = useState<Tab>("predict");

  // Shared data
  const [configs, setConfigs] = useState<EmbeddingConfig[]>([]);
  const [annotationSets, setAnnotationSets] = useState<AnnotationSet[]>([]);
  const [ontologySnapshots, setOntologySnapshots] = useState<OntologySnapshot[]>([]);
  const [querySets, setQuerySets] = useState<QuerySet[]>([]);
  const [predictionSets, setPredictionSets] = useState<PredictionSet[]>([]);
  const [loading, setLoading] = useState(true);

  // Predict form
  const [predConfigId, setPredConfigId] = useState("");
  const [predQuerySetId, setPredQuerySetId] = useState("");
  const [predAnnotationSetId, setPredAnnotationSetId] = useState("");
  const [predSnapshotId, setPredSnapshotId] = useState("");
  const [predLimitPerEntry, setPredLimitPerEntry] = useState(5);
  const [predBatchSize, setPredBatchSize] = useState(256);
  const [predDistanceThreshold, setPredDistanceThreshold] = useState("");
  const [predSearchBackend, setPredSearchBackend] = useState("numpy");
  const [predMetric, setPredMetric] = useState("cosine");
  const [predFaissIndex, setPredFaissIndex] = useState("Flat");
  const [predFaissNlist, setPredFaissNlist] = useState(100);
  const [predFaissNprobe, setPredFaissNprobe] = useState(10);
  const [predFaissHnswM, setPredFaissHnswM] = useState(32);
  const [predFaissHnswEf, setPredFaissHnswEf] = useState(64);
  const [predAspectSeparatedKnn, setPredAspectSeparatedKnn] = useState(true);
  const [predComputeAlignments, setPredComputeAlignments] = useState(false);
  const [predComputeTaxonomy, setPredComputeTaxonomy] = useState(false);
  const [predResult, setPredResult] = useState<{ id: string; status: string } | null>(null);
  const [predError, setPredError] = useState("");
  const [predSubmitting, setPredSubmitting] = useState(false);

  async function loadAll() {
    setLoading(true);
    try {
      const [cfgs, anns, snaps, qsets] = await Promise.all([
        listEmbeddingConfigs(),
        listAnnotationSets(),
        listOntologySnapshots(),
        listQuerySets(),
      ]);
      setConfigs(cfgs);
      setAnnotationSets(anns);
      setOntologySnapshots(snaps);
      setQuerySets(qsets);
      if (cfgs.length > 0) setPredConfigId(cfgs[0].id);
      if (anns.length > 0) setPredAnnotationSetId(anns[0].id);
      if (snaps.length > 0) setPredSnapshotId(snaps[0].id);
    } catch (e: any) {
      toast(e.message ?? "Failed to load data", "error");
    } finally {
      setLoading(false);
    }
  }

  async function loadResults() {
    try {
      setPredictionSets(await listPredictionSets());
    } catch (e: any) {
      toast(e.message ?? "Failed to load results", "error");
    }
  }

  useEffect(() => {
    loadAll();
  }, []);

  useEffect(() => {
    if (activeTab === "results") loadResults();
  }, [activeTab]);

  async function handlePredictSubmit(e: React.FormEvent) {
    e.preventDefault();
    setPredError("");
    setPredResult(null);
    setPredSubmitting(true);
    try {
      const result = await launchPredictGoTerms({
        embedding_config_id: predConfigId,
        annotation_set_id: predAnnotationSetId,
        ontology_snapshot_id: predSnapshotId,
        limit_per_entry: predLimitPerEntry,
        batch_size: predBatchSize,
        distance_threshold: predDistanceThreshold ? parseFloat(predDistanceThreshold) : null,
        query_set_id: predQuerySetId || null,
        search_backend: predSearchBackend,
        metric: predMetric,
        faiss_index_type: predFaissIndex,
        faiss_nlist: predFaissNlist,
        faiss_nprobe: predFaissNprobe,
        faiss_hnsw_m: predFaissHnswM,
        faiss_hnsw_ef_search: predFaissHnswEf,
        aspect_separated_knn: predAspectSeparatedKnn,
        compute_alignments: predComputeAlignments,
        compute_taxonomy: predComputeTaxonomy,
      });
      setPredResult(result);
      toast("Annotation job queued", "success");
    } catch (err: any) {
      setPredError(String(err));
      toast(String(err), "error");
    } finally {
      setPredSubmitting(false);
    }
  }

  async function handleDeleteResult(id: string) {
    const ps = predictionSets.find((p) => p.id === id);
    const count = ps?.prediction_count ?? 0;
    const msg = count > 0
      ? `Delete this annotation set and its ${count.toLocaleString()} GO term assignments? This cannot be undone.`
      : "Delete this annotation set?";
    if (!confirm(msg)) return;
    try {
      const r = await deletePredictionSet(id);
      setPredictionSets((prev) => prev.filter((p) => p.id !== id));
      toast(`Deleted (${r.predictions_deleted.toLocaleString()} assignments removed)`, "info");
    } catch (err: any) {
      toast(String(err), "error");
    }
  }

  const tabs: { key: Tab; label: string }[] = [
    { key: "predict", label: "Run Annotation" },
    { key: "results", label: "Results" },
  ];

  return (
    <>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-xl font-semibold">Functional Annotation</h1>
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

      {/* ── Run Annotation ── */}
      {activeTab === "predict" && (
        <div className="max-w-2xl">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-4">GO Term Annotation by Embedding Similarity</h2>
            {loading ? (
              <p className="text-sm text-gray-400">Loading…</p>
            ) : (
              <form onSubmit={handlePredictSubmit} className="space-y-4">
                <div>
                  <label className={labelClass}>Embedding Config</label>
                  <select value={predConfigId} onChange={(e) => setPredConfigId(e.target.value)} required className={inputClass}>
                    {configs.length === 0 && <option value="">— no configs available —</option>}
                    {configs.map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.description || c.model_name} ({shortId(c.id)})
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className={labelClass}>
                    Query Set <span className="font-normal text-gray-400">(optional — leave empty to annotate all)</span>
                  </label>
                  <select value={predQuerySetId} onChange={(e) => setPredQuerySetId(e.target.value)} className={inputClass}>
                    <option value="">— all sequences —</option>
                    {querySets.map((qs) => (
                      <option key={qs.id} value={qs.id}>
                        {qs.name} ({qs.entry_count} seqs)
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className={labelClass}>Annotation Set</label>
                  <select value={predAnnotationSetId} onChange={(e) => setPredAnnotationSetId(e.target.value)} required className={inputClass}>
                    {annotationSets.length === 0 && <option value="">— no annotation sets available —</option>}
                    {annotationSets.map((a) => (
                      <option key={a.id} value={a.id}>
                        {a.source} {a.source_version ? `(${a.source_version})` : ""} — {shortId(a.id)}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className={labelClass}>Ontology Snapshot</label>
                  <select value={predSnapshotId} onChange={(e) => setPredSnapshotId(e.target.value)} required className={inputClass}>
                    {ontologySnapshots.length === 0 && <option value="">— no snapshots available —</option>}
                    {ontologySnapshots.map((s) => (
                      <option key={s.id} value={s.id}>
                        {s.obo_version} — {shortId(s.id)}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <label className={labelClass}>Limit per Entry</label>
                    <input
                      type="number"
                      value={predLimitPerEntry}
                      onChange={(e) => { const v = parseInt(e.target.value, 10); if (!isNaN(v)) setPredLimitPerEntry(v); }}
                      min={1}
                      className={inputClass}
                    />
                  </div>
                  <div>
                    <label className={labelClass}>Batch Size</label>
                    <input
                      type="number"
                      value={predBatchSize}
                      onChange={(e) => setPredBatchSize(parseInt(e.target.value, 10))}
                      min={1}
                      className={inputClass}
                    />
                  </div>
                  <div>
                    <label className={labelClass}>Distance Threshold <span className="font-normal text-gray-400">(optional)</span></label>
                    <input
                      type="number"
                      step="any"
                      value={predDistanceThreshold}
                      onChange={(e) => setPredDistanceThreshold(e.target.value)}
                      placeholder="none"
                      className={inputClass}
                    />
                  </div>
                </div>

                {/* Feature engineering */}
                <div className="rounded-md border border-gray-200 bg-gray-50 p-4 space-y-3">
                  <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">KNN Strategy</p>
                  <div className="flex flex-col gap-2 mb-4">
                    <label className="flex items-start gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={predAspectSeparatedKnn}
                        onChange={(e) => setPredAspectSeparatedKnn(e.target.checked)}
                        className="mt-0.5 rounded"
                      />
                      <span className="text-sm text-gray-700">
                        Per-aspect KNN indices
                        <span className="ml-1.5 text-xs text-gray-400">Separate BPO / MFO / CCO reference indices — improves recall for each aspect independently</span>
                      </span>
                    </label>
                  </div>

                  <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">Feature Engineering <span className="font-normal normal-case text-gray-400">(opt-in — adds compute time)</span></p>
                  <div className="flex flex-col gap-2">
                    <label className="flex items-start gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={predComputeAlignments}
                        onChange={(e) => setPredComputeAlignments(e.target.checked)}
                        className="mt-0.5 rounded"
                      />
                      <span className="text-sm text-gray-700">
                        Sequence alignments
                        <span className="ml-1.5 text-xs text-gray-400">NW (global) + SW (local) via parasail/BLOSUM62</span>
                      </span>
                    </label>
                    <label className="flex items-start gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={predComputeTaxonomy}
                        onChange={(e) => setPredComputeTaxonomy(e.target.checked)}
                        className="mt-0.5 rounded"
                      />
                      <span className="text-sm text-gray-700">
                        Taxonomic distance
                        <span className="ml-1.5 text-xs text-gray-400">LCA, distance and relation via NCBI taxonomy</span>
                      </span>
                    </label>
                  </div>
                </div>

                {/* Search backend */}
                <div className="rounded-md border border-gray-200 bg-gray-50 p-4 space-y-3">
                  <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">Search Backend</p>
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className={labelClass}>Backend</label>
                      <select value={predSearchBackend} onChange={(e) => setPredSearchBackend(e.target.value)} className={inputClass}>
                        <option value="numpy">numpy — exact</option>
                        <option value="faiss">faiss — indexed</option>
                      </select>
                    </div>
                    <div>
                      <label className={labelClass}>Metric</label>
                      <select value={predMetric} onChange={(e) => setPredMetric(e.target.value)} className={inputClass}>
                        <option value="cosine">cosine</option>
                        <option value="l2">L2 (Euclidean²)</option>
                      </select>
                    </div>
                  </div>

                  {predSearchBackend === "faiss" && (
                    <>
                      <div>
                        <label className={labelClass}>Index Type</label>
                        <select value={predFaissIndex} onChange={(e) => setPredFaissIndex(e.target.value)} className={inputClass}>
                          <option value="Flat">Flat — exact</option>
                          <option value="IVFFlat">IVFFlat — approximate (&gt;100K refs)</option>
                          <option value="HNSW">HNSW — approximate, graph-based</option>
                        </select>
                      </div>
                      {predFaissIndex === "IVFFlat" && (
                        <div className="grid grid-cols-2 gap-3">
                          <div>
                            <label className={labelClass}>nlist</label>
                            <input type="number" value={predFaissNlist} onChange={(e) => setPredFaissNlist(parseInt(e.target.value, 10))} min={1} className={inputClass} />
                          </div>
                          <div>
                            <label className={labelClass}>nprobe</label>
                            <input type="number" value={predFaissNprobe} onChange={(e) => setPredFaissNprobe(parseInt(e.target.value, 10))} min={1} className={inputClass} />
                          </div>
                        </div>
                      )}
                      {predFaissIndex === "HNSW" && (
                        <div className="grid grid-cols-2 gap-3">
                          <div>
                            <label className={labelClass}>M</label>
                            <input type="number" value={predFaissHnswM} onChange={(e) => setPredFaissHnswM(parseInt(e.target.value, 10))} min={2} className={inputClass} />
                          </div>
                          <div>
                            <label className={labelClass}>efSearch</label>
                            <input type="number" value={predFaissHnswEf} onChange={(e) => setPredFaissHnswEf(parseInt(e.target.value, 10))} min={1} className={inputClass} />
                          </div>
                        </div>
                      )}
                    </>
                  )}
                </div>

                {predError && (
                  <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                    {predError}
                  </pre>
                )}

                {predResult && (
                  <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                    Job queued:{" "}
                    <Link href={`/jobs/${predResult.id}`} className="font-mono underline hover:text-green-900">
                      {predResult.id}
                    </Link>
                  </div>
                )}

                <div className="flex justify-end">
                  <button
                    type="submit"
                    disabled={predSubmitting || configs.length === 0}
                    className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
                  >
                    {predSubmitting ? "Launching…" : "Launch Annotation Job"}
                  </button>
                </div>
              </form>
            )}
          </div>
        </div>
      )}

      {/* ── Results ── */}
      {activeTab === "results" && (
        <div>
          <div className="flex items-center justify-between mb-3">
            <p className="text-sm text-gray-500">
              {predictionSets.length} annotation result{predictionSets.length !== 1 ? "s" : ""}
            </p>
            <button onClick={loadResults} className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
              Refresh
            </button>
          </div>

          <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[80px_100px_100px_100px_90px_120px_160px_60px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>ID</div>
              <div>Config</div>
              <div>Annotation Set</div>
              <div>Snapshot</div>
              <div>GO Terms</div>
              <div>Dist. Threshold</div>
              <div>Created</div>
              <div></div>
            </div>

            {predictionSets.map((ps) => (
              <div
                key={ps.id}
                className="grid grid-cols-[80px_100px_100px_100px_90px_120px_160px_60px] gap-2 border-b px-4 py-3 text-sm last:border-0 items-center"
              >
                <div className="font-mono text-xs">
                  <Link href={`/functional-annotation/${ps.id}`} className="text-blue-600 hover:underline" title={ps.id}>
                    {shortId(ps.id)}…
                  </Link>
                </div>
                <div className="font-mono text-xs text-gray-500" title={ps.embedding_config_id}>{shortId(ps.embedding_config_id)}</div>
                <div className="font-mono text-xs text-gray-500" title={ps.annotation_set_id}>{shortId(ps.annotation_set_id)}</div>
                <div className="font-mono text-xs text-gray-500" title={ps.ontology_snapshot_id}>{shortId(ps.ontology_snapshot_id)}</div>
                <div className="text-gray-700">{ps.prediction_count ?? 0}</div>
                <div className="text-gray-600">
                  {ps.distance_threshold != null ? ps.distance_threshold : <span className="text-gray-400">—</span>}
                </div>
                <div className="text-xs text-gray-400">{formatDate(ps.created_at)}</div>
                <div className="flex justify-end">
                  <button
                    onClick={() => handleDeleteResult(ps.id)}
                    className="rounded border border-red-200 px-2 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
                  >
                    Delete
                  </button>
                </div>
              </div>
            ))}

            {predictionSets.length === 0 && (
              <div className="px-4 py-8 text-center text-sm text-gray-400">
                No annotation results yet. Run an annotation job from the Run Annotation tab.
              </div>
            )}
          </div>
        </div>
      )}
    </>
  );
}
