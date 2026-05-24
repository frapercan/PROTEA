"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useTranslations } from "next-intl";
import { Telescope } from "lucide-react";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import { ContextBanner } from "@/components/ContextBanner";
import { PipelinePrereqs, type PrereqStep } from "@/components/PipelinePrereqs";
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
const labelClass = "block text-sm font-medium text-slate-700 mb-1";

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
  const [predComputeRerankerFeatures, setPredComputeRerankerFeatures] = useState(true);
  const [predResult, setPredResult] = useState<{ id: string; status: string } | null>(null);
  const [predError, setPredError] = useState("");
  const [predSubmitting, setPredSubmitting] = useState(false);

  // FEAT-UX-RERANKER-AUTOLOCK: when the reranker feature bundle is on we MUST
  // also compute alignments + taxonomy (the booster's `feature_schema_sha` is
  // load-bearing at inference, see CLAUDE.md). We auto-check + lock both
  // checkboxes while the bundle is on, and restore the user's prior choices
  // when they turn the bundle off again.
  const priorAlignmentsRef = useRef<boolean>(predComputeAlignments);
  const priorTaxonomyRef = useRef<boolean>(predComputeTaxonomy);
  useEffect(() => {
    if (predComputeRerankerFeatures) {
      // Force both companions on. The "remember prior" effect below already
      // snapshotted whatever the user had selected while the bundle was off.
      if (!predComputeAlignments) setPredComputeAlignments(true);
      if (!predComputeTaxonomy) setPredComputeTaxonomy(true);
    } else {
      // Restore whatever the user had selected before the bundle was turned on.
      setPredComputeAlignments(priorAlignmentsRef.current);
      setPredComputeTaxonomy(priorTaxonomyRef.current);
    }
    // We intentionally depend only on the bundle toggle: changes to alignments
    // / taxonomy while the bundle is OFF are the user's free choice and we
    // record them via the dedicated effect below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [predComputeRerankerFeatures]);
  // Record the user's manual choices so we can restore them when the bundle is
  // toggled off. Only meaningful while the bundle is OFF (otherwise the values
  // are forced and would overwrite the saved prior).
  useEffect(() => {
    if (!predComputeRerankerFeatures) {
      priorAlignmentsRef.current = predComputeAlignments;
      priorTaxonomyRef.current = predComputeTaxonomy;
    }
  }, [predComputeAlignments, predComputeTaxonomy, predComputeRerankerFeatures]);

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
        compute_reranker_features: predComputeRerankerFeatures,
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

  // Resolve the currently-selected entities so the stepper can show their
  // human names rather than UUID prefixes. AnnotationSet still has no
  // name field on the backend (TODO: add a migration in a follow-up
  // slice, see PR body); surface source + version + count as a label.
  const selectedConfig = configs.find((c) => c.id === predConfigId);
  const selectedAnnotationSet = annotationSets.find((a) => a.id === predAnnotationSetId);
  const selectedQuerySet = querySets.find((qs) => qs.id === predQuerySetId);

  const configValue = selectedConfig
    ? `${selectedConfig.description || selectedConfig.model_name}${
        selectedConfig.embedding_count != null
          ? ` (${selectedConfig.embedding_count.toLocaleString()} ${t("onboarding.steps.embeddings.unit")})`
          : ""
      }`
    : null;

  const annotationValue = selectedAnnotationSet
    ? `${selectedAnnotationSet.source}${
        selectedAnnotationSet.source_version
          ? ` ${selectedAnnotationSet.source_version}`
          : ""
      }${
        selectedAnnotationSet.annotation_count != null
          ? ` (${selectedAnnotationSet.annotation_count.toLocaleString()} ${t("onboarding.steps.annotationSet.unit")})`
          : ""
      }`
    : null;

  // For the query-set step we treat "no query set picked" as a valid state
  // (it means: annotate all sequences in the DB). The CTA is therefore
  // optional and shows a hint instead of a hard error when the user has
  // not created any query sets yet.
  const queryValue = selectedQuerySet
    ? `${selectedQuerySet.name} (${selectedQuerySet.entry_count.toLocaleString()} ${t("onboarding.steps.querySet.unit")})`
    : querySets.length > 0
      ? t("onboarding.steps.querySet.allSequences")
      : null;

  const prereqSteps: PrereqStep[] = [
    {
      title: t("onboarding.steps.embeddings.title"),
      help: t("onboarding.steps.embeddings.help"),
      loading,
      value: configValue ?? undefined,
      emptyLabel: t("onboarding.steps.embeddings.emptyLabel"),
      emptyHref: "/embeddings",
      emptyCta: t("onboarding.steps.embeddings.cta"),
    },
    {
      title: t("onboarding.steps.annotationSet.title"),
      help: t("onboarding.steps.annotationSet.help"),
      loading,
      value: annotationValue ?? undefined,
      emptyLabel: t("onboarding.steps.annotationSet.emptyLabel"),
      emptyHref: "/annotations",
      emptyCta: t("onboarding.steps.annotationSet.cta"),
    },
    {
      title: t("onboarding.steps.querySet.title"),
      help: t("onboarding.steps.querySet.help"),
      loading,
      value: queryValue ?? undefined,
      emptyLabel: t("onboarding.steps.querySet.emptyLabel"),
      emptyHref: "/query-sets",
      emptyCta: t("onboarding.steps.querySet.cta"),
    },
  ];

  return (
    <>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-slate-900">{t("title")}</h1>
        <p className="mt-2 max-w-3xl text-sm leading-relaxed text-slate-600">
          {t("onboarding.intro")}
        </p>
      </div>

      <section
        aria-label={t("onboarding.prereqsAriaLabel")}
        className="mb-5 rounded-lg border border-stone-200 bg-stone-50 px-3 py-2.5"
      >
        <div className="mb-2 flex items-baseline justify-between gap-3">
          <h2 className="text-[11px] font-semibold uppercase tracking-wider text-stone-500">
            {t("onboarding.prereqsTitle")}
          </h2>
          <p className="text-[11px] text-stone-500">{t("onboarding.prereqsHelper")}</p>
        </div>
        <PipelinePrereqs steps={prereqSteps} />
      </section>

      <div className="flex gap-1 border-b mb-6 overflow-x-auto">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab.key
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-slate-500 hover:text-slate-700"
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
              <p className="text-sm text-slate-600">{t("predictTab.loading")}</p>
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
                    {t("predictTab.querySetLabel")} <span className="font-normal text-slate-600">{t("predictTab.querySetHelper")}</span>
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
                    <label className={labelClass}>{t("predictTab.distanceThresholdLabel")} <span className="font-normal text-slate-600">{t("predictTab.distanceThresholdHelper")}</span></label>
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
                <div className="rounded-md border border-slate-200 bg-slate-50 p-4 space-y-3">
                  <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">{t("predictTab.knnStrategy")}</p>
                  <div className="flex flex-col gap-2 mb-4">
                    <label className="flex items-start gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={predAspectSeparatedKnn}
                        onChange={(e) => setPredAspectSeparatedKnn(e.target.checked)}
                        className="mt-0.5 rounded"
                      />
                      <span className="text-sm text-slate-700">
                        {t("predictTab.aspectSeparatedKnn")}
                        <span className="ml-1.5 text-xs text-slate-600">{t("predictTab.aspectSeparatedKnnHelper")}</span>
                      </span>
                    </label>
                  </div>

                  <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                    {t("predictTab.featureEngineering")} <span className="font-normal normal-case text-slate-600">{t("predictTab.featureEngineeringHelper")}</span>
                  </p>
                  <div className="flex flex-col gap-2">
                    <label className="flex items-start gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={predComputeAlignments}
                        onChange={(e) => setPredComputeAlignments(e.target.checked)}
                        disabled={predComputeRerankerFeatures}
                        data-testid={predComputeRerankerFeatures ? "annotateform-feat-locked-by-reranker" : undefined}
                        title={predComputeRerankerFeatures ? t("predictTab.lockedByRerankerTooltip") : undefined}
                        aria-describedby={predComputeRerankerFeatures ? "pred-align-lock-note" : undefined}
                        className="mt-0.5 rounded"
                      />
                      <span className="text-sm text-slate-700">
                        {t("predictTab.sequenceAlignments")}
                        <span className="ml-1.5 text-xs text-slate-600">{t("predictTab.sequenceAlignmentsHelper")}</span>
                        {predComputeRerankerFeatures && (
                          <span
                            id="pred-align-lock-note"
                            className="ml-1.5 text-xs text-slate-600"
                          >
                            {t("predictTab.lockedByRerankerNote")}
                          </span>
                        )}
                      </span>
                    </label>
                    <label className="flex items-start gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={predComputeTaxonomy}
                        onChange={(e) => setPredComputeTaxonomy(e.target.checked)}
                        disabled={predComputeRerankerFeatures}
                        data-testid={predComputeRerankerFeatures ? "annotateform-feat-locked-by-reranker" : undefined}
                        title={predComputeRerankerFeatures ? t("predictTab.lockedByRerankerTooltip") : undefined}
                        aria-describedby={predComputeRerankerFeatures ? "pred-tax-lock-note" : undefined}
                        className="mt-0.5 rounded"
                      />
                      <span className="text-sm text-slate-700">
                        {t("predictTab.taxonomicDistance")}
                        <span className="ml-1.5 text-xs text-slate-600">{t("predictTab.taxonomicDistanceHelper")}</span>
                        {predComputeRerankerFeatures && (
                          <span
                            id="pred-tax-lock-note"
                            className="ml-1.5 text-xs text-slate-600"
                          >
                            {t("predictTab.lockedByRerankerNote")}
                          </span>
                        )}
                      </span>
                    </label>
                    <label className="flex items-start gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={predComputeRerankerFeatures}
                        onChange={(e) => setPredComputeRerankerFeatures(e.target.checked)}
                        className="mt-0.5 rounded"
                      />
                      <span className="text-sm text-slate-700">
                        {t("predictTab.rerankerFeatures")}
                        <span className="ml-1.5 text-xs text-slate-600">{t("predictTab.rerankerFeaturesHelper")}</span>
                      </span>
                    </label>
                  </div>

                  {/* Computed families summary panel.
                      Visual encoding: families that the active configuration
                      WILL emit are rendered with a single accent (blue-50 /
                      blue-700); families that the pipeline could compute but
                      the current toggles do not request appear muted
                      (stone-100 / stone-500). Family-kind (alignment vs
                      taxonomy vs reranker) is intentionally NOT color-coded:
                      the only signal that carries meaning here is
                      emitted-or-not. */}
                  <div className="mt-3 rounded-md border border-slate-200 bg-white px-3 py-2">
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500 mb-1.5">
                      {t("predictTab.computedFamiliesTitle")}
                    </p>
                    <ul className="flex flex-wrap gap-1.5">
                      {([
                        { name: "knn_base", emitted: true },
                        { name: "alignment_nw", emitted: predComputeAlignments },
                        { name: "alignment_sw", emitted: predComputeAlignments },
                        { name: "taxonomy_pair", emitted: predComputeTaxonomy },
                        { name: "taxonomy_voters", emitted: predComputeTaxonomy },
                        { name: "lineage", emitted: predComputeRerankerFeatures },
                        { name: "anc2vec_neighbor", emitted: predComputeRerankerFeatures },
                        { name: "anc2vec_query", emitted: predComputeRerankerFeatures },
                        { name: "emb_pca", emitted: predComputeRerankerFeatures },
                        { name: "annotation_meta", emitted: predComputeRerankerFeatures },
                      ] as const).map((fam) => (
                        <li key={fam.name}>
                          <code
                            className={`rounded border px-1.5 py-0.5 text-[11px] font-mono ${
                              fam.emitted
                                ? "bg-blue-50 border-blue-200 text-blue-700"
                                : "bg-stone-100 border-stone-200 text-stone-500 line-through decoration-stone-400/60"
                            }`}
                            title={fam.emitted ? undefined : "Not emitted by the current toggle configuration"}
                          >
                            {fam.name}
                          </code>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>

                {/* Search Backend */}
                <div className="rounded-md border border-slate-200 bg-slate-50 p-4 space-y-3">
                  <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">{t("predictTab.searchBackend")}</p>
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
          {predictionSets.length === 0 ? (
            // Empty-state hero: when no PredictionSets exist, the table
            // headers ("ID / Config / …") read as broken UI on top of zero
            // rows. Replace BOTH the desktop table and the mobile card grid
            // with a single onboarding panel that points the user back to
            // the Run Annotation tab. See FD-empty-state task (2026-05-24).
            <section
              aria-labelledby="results-empty-title"
              className="rounded-lg border border-stone-200 bg-white px-6 py-12 sm:px-10 sm:py-16 shadow-sm"
            >
              <div className="mx-auto flex max-w-xl flex-col items-center text-center">
                <span
                  aria-hidden
                  className="mb-5 inline-flex h-14 w-14 items-center justify-center rounded-full border border-stone-200 bg-stone-50 text-stone-500"
                >
                  <Telescope className="h-7 w-7" strokeWidth={1.5} />
                </span>
                <h3
                  id="results-empty-title"
                  className="text-lg font-semibold text-slate-900"
                >
                  {t("resultsTab.emptyHero.title")}
                </h3>
                <p className="mt-2 max-w-lg text-sm leading-relaxed text-slate-600">
                  {t("resultsTab.emptyHero.body")}
                </p>
                <div className="mt-6 flex flex-col items-center gap-3 sm:flex-row">
                  <button
                    type="button"
                    onClick={() => setActiveTab("predict")}
                    className="inline-flex items-center justify-center rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
                  >
                    {t("resultsTab.emptyHero.ctaPrimary")}
                  </button>
                  <button
                    type="button"
                    onClick={loadResults}
                    className="inline-flex items-center justify-center rounded-md border border-stone-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:bg-stone-50"
                  >
                    {t("resultsTab.refresh")}
                  </button>
                </div>
                <p className="mt-5 text-xs text-slate-500">
                  <Link
                    href="/benchmark"
                    className="text-blue-600 hover:text-blue-800 hover:underline"
                  >
                    {t("resultsTab.emptyHero.ctaSecondary")}
                  </Link>
                </p>
              </div>
            </section>
          ) : (
            <>
          <div className="flex items-center justify-between mb-3">
            <p className="text-sm text-slate-500">
              {t("resultsTab.resultCount", { count: predictionSets.length })}
            </p>
            <button onClick={loadResults} className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-slate-50">
              {t("resultsTab.refresh")}
            </button>
          </div>

          {/* Mobile card list */}
          <div className="lg:hidden space-y-2">
            {predictionSets.map((ps) => (
              <div
                key={ps.id}
                className="rounded-lg border bg-white p-3 shadow-sm"
              >
                <div className="flex items-start justify-between gap-2">
                  <Link
                    href={`/functional-annotation/${ps.id}`}
                    className="font-mono text-sm text-blue-600 hover:underline truncate"
                    title={ps.id}
                  >
                    {shortId(ps.id)}…
                  </Link>
                  <span className="text-xs text-slate-600 shrink-0">{formatDate(ps.created_at)}</span>
                </div>
                <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
                  <div>
                    <span className="block text-[10px] font-semibold uppercase tracking-wide text-slate-600">
                      {t("resultsTab.tableHeaders.config")}
                    </span>
                    <span className="text-slate-700 truncate block" title={ps.embedding_config_id}>
                      {ps.embedding_config_name ?? shortId(ps.embedding_config_id)}
                    </span>
                  </div>
                  <div>
                    <span className="block text-[10px] font-semibold uppercase tracking-wide text-slate-600">
                      {t("resultsTab.tableHeaders.annotationSet")}
                    </span>
                    <span className="text-slate-700 truncate block" title={ps.annotation_set_id}>
                      {ps.annotation_set_label ?? shortId(ps.annotation_set_id)}
                    </span>
                  </div>
                  <div>
                    <span className="block text-[10px] font-semibold uppercase tracking-wide text-slate-600">
                      {t("resultsTab.tableHeaders.snapshot")}
                    </span>
                    <span className="text-slate-700 truncate block" title={ps.ontology_snapshot_id}>
                      {ps.ontology_snapshot_version ?? shortId(ps.ontology_snapshot_id)}
                    </span>
                  </div>
                  <div>
                    <span className="block text-[10px] font-semibold uppercase tracking-wide text-slate-600">
                      {t("resultsTab.tableHeaders.goTerms")}
                    </span>
                    <span className="text-slate-700">{ps.prediction_count ?? 0}</span>
                  </div>
                  <div>
                    <span className="block text-[10px] font-semibold uppercase tracking-wide text-slate-600">
                      {t("resultsTab.tableHeaders.distanceThreshold")}
                    </span>
                    <span className="text-slate-700">
                      {ps.distance_threshold != null ? ps.distance_threshold : "—"}
                    </span>
                  </div>
                  <div>
                    <span className="block text-[10px] font-semibold uppercase tracking-wide text-slate-600">
                      {t("resultsTab.tableHeaders.k")}
                    </span>
                    <span className="text-slate-700">{ps.limit_per_entry}</span>
                  </div>
                </div>
                <div className="mt-3 flex justify-end">
                  <button
                    onClick={() => handleDeleteResult(ps.id)}
                    className="rounded border border-red-200 px-2 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
                  >
                    {t("resultsTab.delete")}
                  </button>
                </div>
              </div>
            ))}
          </div>

          {/* Desktop table */}
          <div className="hidden lg:block rounded-lg border bg-white shadow-sm overflow-hidden">
            <div className="grid grid-cols-[80px_100px_100px_100px_90px_80px_50px_160px_60px] gap-2 border-b bg-slate-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-600">
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

            <div className="overflow-x-auto">
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
                  <div className="text-xs text-slate-700" title={ps.embedding_config_id}>{ps.embedding_config_name ?? shortId(ps.embedding_config_id)}</div>
                  <div className="text-xs text-slate-700" title={ps.annotation_set_id}>{ps.annotation_set_label ?? shortId(ps.annotation_set_id)}</div>
                  <div className="text-xs text-slate-700" title={ps.ontology_snapshot_id}>{ps.ontology_snapshot_version ?? shortId(ps.ontology_snapshot_id)}</div>
                  <div className="text-slate-700">{ps.prediction_count ?? 0}</div>
                  <div className="text-slate-600">
                    {ps.distance_threshold != null ? ps.distance_threshold : <span className="text-slate-600">—</span>}
                  </div>
                  <div className="text-slate-600">{ps.limit_per_entry}</div>
                  <div className="text-xs text-slate-600">{formatDate(ps.created_at)}</div>
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
            </div>
          </div>
            </>
          )}
        </div>
      )}
    </>
  );
}
