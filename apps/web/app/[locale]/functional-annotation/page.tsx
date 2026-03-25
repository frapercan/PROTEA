"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useTranslations } from "next-intl";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import { ContextBanner } from "@/components/ContextBanner";
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
  const t = useTranslations("functionalAnnotation");
  const toast = useToast();
  const [activeTab, setActiveTab] = useState<Tab>("predict");

  const [configs, setConfigs] = useState<EmbeddingConfig[]>([]);
  const [annotationSets, setAnnotationSets] = useState<AnnotationSet[]>([]);
  const [ontologySnapshots, setOntologySnapshots] = useState<OntologySnapshot[]>([]);
  const [querySets, setQuerySets] = useState<QuerySet[]>([]);
  const [predictionSets, setPredictionSets] = useState<PredictionSet[]>([]);
  const [loading, setLoading] = useState(true);

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

  useEffect(() => { loadAll(); }, []);
  useEffect(() => { if (activeTab === "results") loadResults(); }, [activeTab]);

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
      toast(t("predictTab.launchAnnotationJob"), "success");
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
      ? t("resultsTab.deleteConfirm", { count: count.toLocaleString() })
      : t("resultsTab.deleteConfirmNoAssignments");
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
    { key: "predict", label: t("tabs.predict") },
    { key: "results", label: t("tabs.results") },
  ];

  return (
    <>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-xl font-semibold">{t("title")}</h1>
      </div>

      <ContextBanner
        title="Predict GO terms by embedding similarity"
        description="Uses KNN search to transfer GO annotations from similar proteins. Requires computed embeddings and a loaded annotation set."
        prerequisites={!loading ? [
          { label: `${configs.length} embedding config(s)`, met: configs.length > 0, href: "/embeddings" },
          { label: `${annotationSets.length} annotation set(s)`, met: annotationSets.length > 0, href: "/annotations" },
          { label: `${ontologySnapshots.length} ontology snapshot(s)`, met: ontologySnapshots.length > 0, href: "/annotations" },
        ] : undefined}
        nextStep={{ label: "Evaluation", href: "/evaluation" }}
      />

      <div className="flex gap-1 border-b mb-6 overflow-x-auto">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab.key
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* ── Run Annotation ── */}
      {activeTab === "predict" && (
        <div className="max-w-2xl">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-4">{t("predictTab.title")}</h2>
            {loading ? (
              <p className="text-sm text-gray-400">{t("predictTab.loading")}</p>
            ) : (
              <form onSubmit={handlePredictSubmit} className="space-y-4">
                <div>
                  <label className={labelClass}>{t("predictTab.configLabel")}</label>
                  <select value={predConfigId} onChange={(e) => setPredConfigId(e.target.value)} required className={inputClass}>
                    {configs.length === 0 && <option value="">{t("predictTab.noConfigs")}</option>}
                    {configs.map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.description || c.model_name} ({shortId(c.id)})
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className={labelClass}>
                    {t("predictTab.querySetLabel")} <span className="font-normal text-gray-400">{t("predictTab.querySetHelper")}</span>
                  </label>
                  <select value={predQuerySetId} onChange={(e) => setPredQuerySetId(e.target.value)} className={inputClass}>
                    <option value="">{t("predictTab.allSequences")}</option>
                    {querySets.map((qs) => (
                      <option key={qs.id} value={qs.id}>
                        {qs.name} ({qs.entry_count} seqs)
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className={labelClass}>{t("predictTab.annotationSetLabel")}</label>
                  <select value={predAnnotationSetId} onChange={(e) => setPredAnnotationSetId(e.target.value)} required className={inputClass}>
                    {annotationSets.length === 0 && <option value="">{t("predictTab.noAnnotationSets")}</option>}
                    {annotationSets.map((a) => (
                      <option key={a.id} value={a.id}>
                        {a.source} {a.source_version ? `(${a.source_version})` : ""} — {shortId(a.id)}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className={labelClass}>{t("predictTab.snapshotLabel")}</label>
                  <select value={predSnapshotId} onChange={(e) => setPredSnapshotId(e.target.value)} required className={inputClass}>
                    {ontologySnapshots.length === 0 && <option value="">{t("predictTab.noSnapshots")}</option>}
                    {ontologySnapshots.map((s) => (
                      <option key={s.id} value={s.id}>
                        {s.obo_version} — {shortId(s.id)}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                  <div>
                    <label className={labelClass}>{t("predictTab.limitPerEntryLabel")}</label>
                    <input
                      type="number"
                      value={predLimitPerEntry}
                      onChange={(e) => { const v = parseInt(e.target.value, 10); if (!isNaN(v)) setPredLimitPerEntry(v); }}
                      min={1}
                      className={inputClass}
                    />
                  </div>
                  <div>
                    <label className={labelClass}>{t("predictTab.batchSizeLabel")}</label>
                    <input
                      type="number"
                      value={predBatchSize}
                      onChange={(e) => setPredBatchSize(parseInt(e.target.value, 10))}
                      min={1}
                      className={inputClass}
                    />
                  </div>
                  <div>
                    <label className={labelClass}>{t("predictTab.distanceThresholdLabel")} <span className="font-normal text-gray-400">{t("predictTab.distanceThresholdHelper")}</span></label>
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

                {/* KNN Strategy + Feature Engineering */}
                <div className="rounded-md border border-gray-200 bg-gray-50 p-4 space-y-3">
                  <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">{t("predictTab.knnStrategy")}</p>
                  <div className="flex flex-col gap-2 mb-4">
                    <label className="flex items-start gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={predAspectSeparatedKnn}
                        onChange={(e) => setPredAspectSeparatedKnn(e.target.checked)}
                        className="mt-0.5 rounded"
                      />
                      <span className="text-sm text-gray-700">
                        {t("predictTab.aspectSeparatedKnn")}
                        <span className="ml-1.5 text-xs text-gray-400">{t("predictTab.aspectSeparatedKnnHelper")}</span>
                      </span>
                    </label>
                  </div>

                  <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">
                    {t("predictTab.featureEngineering")} <span className="font-normal normal-case text-gray-400">{t("predictTab.featureEngineeringHelper")}</span>
                  </p>
                  <div className="flex flex-col gap-2">
                    <label className="flex items-start gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={predComputeAlignments}
                        onChange={(e) => setPredComputeAlignments(e.target.checked)}
                        className="mt-0.5 rounded"
                      />
                      <span className="text-sm text-gray-700">
                        {t("predictTab.sequenceAlignments")}
                        <span className="ml-1.5 text-xs text-gray-400">{t("predictTab.sequenceAlignmentsHelper")}</span>
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
                        {t("predictTab.taxonomicDistance")}
                        <span className="ml-1.5 text-xs text-gray-400">{t("predictTab.taxonomicDistanceHelper")}</span>
                      </span>
                    </label>
                  </div>
                </div>

                {/* Search Backend */}
                <div className="rounded-md border border-gray-200 bg-gray-50 p-4 space-y-3">
                  <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">{t("predictTab.searchBackend")}</p>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    <div>
                      <label className={labelClass}>{t("predictTab.searchBackendLabel")}</label>
                      <select value={predSearchBackend} onChange={(e) => setPredSearchBackend(e.target.value)} className={inputClass}>
                        <option value="numpy">{t("predictTab.numpyBackend")}</option>
                        <option value="faiss">{t("predictTab.faissBackend")}</option>
                      </select>
                    </div>
                    <div>
                      <label className={labelClass}>{t("predictTab.metricLabel")}</label>
                      <select value={predMetric} onChange={(e) => setPredMetric(e.target.value)} className={inputClass}>
                        <option value="cosine">{t("predictTab.cosineSimilarity")}</option>
                        <option value="l2">{t("predictTab.euclideanDistance")}</option>
                      </select>
                    </div>
                  </div>

                  {predSearchBackend === "faiss" && (
                    <>
                      <div>
                        <label className={labelClass}>{t("predictTab.indexTypeLabel")}</label>
                        <select value={predFaissIndex} onChange={(e) => setPredFaissIndex(e.target.value)} className={inputClass}>
                          <option value="Flat">{t("predictTab.flatIndex")}</option>
                          <option value="IVFFlat">{t("predictTab.ivfflatIndex")}</option>
                          <option value="HNSW">{t("predictTab.hnswIndex")}</option>
                        </select>
                      </div>
                      {predFaissIndex === "IVFFlat" && (
                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                          <div>
                            <label className={labelClass}>{t("predictTab.nlistLabel")}</label>
                            <input type="number" value={predFaissNlist} onChange={(e) => setPredFaissNlist(parseInt(e.target.value, 10))} min={1} className={inputClass} />
                          </div>
                          <div>
                            <label className={labelClass}>{t("predictTab.nprobeLabel")}</label>
                            <input type="number" value={predFaissNprobe} onChange={(e) => setPredFaissNprobe(parseInt(e.target.value, 10))} min={1} className={inputClass} />
                          </div>
                        </div>
                      )}
                      {predFaissIndex === "HNSW" && (
                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                          <div>
                            <label className={labelClass}>{t("predictTab.mLabel")}</label>
                            <input type="number" value={predFaissHnswM} onChange={(e) => setPredFaissHnswM(parseInt(e.target.value, 10))} min={2} className={inputClass} />
                          </div>
                          <div>
                            <label className={labelClass}>{t("predictTab.efSearchLabel")}</label>
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
                    {predSubmitting ? t("predictTab.launching") : t("predictTab.launchAnnotationJob")}
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
              {t("resultsTab.refresh")}
            </button>
          </div>

          <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[80px_100px_100px_100px_90px_80px_50px_160px_60px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>{t("resultsTab.tableHeaders.id")}</div>
              <div>{t("resultsTab.tableHeaders.config")}</div>
              <div>{t("resultsTab.tableHeaders.annotationSet")}</div>
              <div>{t("resultsTab.tableHeaders.snapshot")}</div>
              <div>{t("resultsTab.tableHeaders.goTerms")}</div>
              <div>{t("resultsTab.tableHeaders.distanceThreshold")}</div>
              <div>{t("resultsTab.tableHeaders.k")}</div>
              <div>{t("resultsTab.tableHeaders.created")}</div>
              <div></div>
            </div>

            {predictionSets.map((ps) => (
              <div
                key={ps.id}
                className="grid grid-cols-[80px_100px_100px_100px_90px_80px_50px_160px_60px] gap-2 border-b px-4 py-3 text-sm last:border-0 items-center"
              >
                <div className="font-mono text-xs">
                  <Link href={`/functional-annotation/${ps.id}`} className="text-blue-600 hover:underline" title={ps.id}>
                    {shortId(ps.id)}…
                  </Link>
                </div>
                <div className="text-xs text-gray-700" title={ps.embedding_config_id}>{ps.embedding_config_name ?? shortId(ps.embedding_config_id)}</div>
                <div className="text-xs text-gray-700" title={ps.annotation_set_id}>{ps.annotation_set_label ?? shortId(ps.annotation_set_id)}</div>
                <div className="text-xs text-gray-700" title={ps.ontology_snapshot_id}>{ps.ontology_snapshot_version ?? shortId(ps.ontology_snapshot_id)}</div>
                <div className="text-gray-700">{ps.prediction_count ?? 0}</div>
                <div className="text-gray-600">
                  {ps.distance_threshold != null ? ps.distance_threshold : <span className="text-gray-400">—</span>}
                </div>
                <div className="text-gray-600">{ps.limit_per_entry}</div>
                <div className="text-xs text-gray-400">{formatDate(ps.created_at)}</div>
                <div className="flex justify-end">
                  <button
                    onClick={() => handleDeleteResult(ps.id)}
                    className="rounded border border-red-200 px-2 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
                  >
                    {t("resultsTab.delete")}
                  </button>
                </div>
              </div>
            ))}

            {predictionSets.length === 0 && (
              <div className="px-4 py-8 text-center text-sm text-gray-400">
                {t("resultsTab.noResults")}
              </div>
            )}
          </div>
        </div>
      )}
    </>
  );
}
