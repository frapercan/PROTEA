"use client";

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { useToast } from "@/components/Toast";
import { useTranslations } from "next-intl";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import { Skeleton } from "@/components/Skeleton";
import { getProtein, getProteinAnnotations, getGoSubgraph, listOntologySnapshots, ProteinDetail, ProteinAnnotation, GoSubgraph } from "@/lib/api";
import dynamic from "next/dynamic";
const GoGraph = dynamic(() => import("@/components/GoGraph"), { ssr: false });

type Tab = "overview" | "annotations";

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

type AnnotationSource = { source: string; version: string | null };
type GroupedAnnotation = {
  go_id: string;
  name: string | null;
  aspect: string | null;
  evidence_codes: string[];
  qualifiers: string[];
  sources: AnnotationSource[];
};

// "GOA 2025-03" (version optional, null-safe).
function formatSource({ source, version }: AnnotationSource): string {
  return version ? `${source} ${version}` : source;
}

function Field({ label, value }: { label: string; value?: string | null }) {
  if (!value) return null;
  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-1">{label}</p>
      <p className="text-sm text-slate-800 whitespace-pre-wrap">{value}</p>
    </div>
  );
}

export default function ProteinDetailPage({ params }: { params: Promise<{ accession: string }> }) {
  const { accession } = use(params);
  const t = useTranslations("proteinDetail");
  const tToast = useTranslations("toasts");
  const toast = useToast();
  const [protein, setProtein] = useState<ProteinDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<Tab>("overview");
  const [annotations, setAnnotations] = useState<ProteinAnnotation[]>([]);
  const [loadingAnnotations, setLoadingAnnotations] = useState(false);
  const [annotationsLoaded, setAnnotationsLoaded] = useState(false);
  const [subgraph, setSubgraph] = useState<GoSubgraph | null>(null);
  const [loadingGraph, setLoadingGraph] = useState(false);
  const [showGraph, setShowGraph] = useState(false);

  useEffect(() => {
    getProtein(decodeURIComponent(accession))
      .then(setProtein)
      .catch((e: any) => toast(e.message ?? tToast("loadProteinFailed"), "error"))
      .finally(() => setLoading(false));
  }, [accession]);

  useEffect(() => {
    if (activeTab === "annotations" && !annotationsLoaded) {
      setLoadingAnnotations(true);
      getProteinAnnotations(decodeURIComponent(accession))
        .then((data) => { setAnnotations(data); setAnnotationsLoaded(true); })
        .catch((e: any) => toast(e.message ?? tToast("loadAnnotationsFailed"), "error"))
        .finally(() => setLoadingAnnotations(false));
    }
    if (activeTab !== "annotations") { setShowGraph(false); setSubgraph(null); }
  }, [activeTab]);

  if (loading) {
    return (
      <div className="mt-8 space-y-6">
        <div className="flex items-start gap-4">
          <div className="space-y-2">
            <Skeleton className="h-7 w-40" />
            <Skeleton className="h-4 w-56" />
          </div>
        </div>
        <Skeleton className="h-10 w-full" />
        <div className="grid gap-4 sm:grid-cols-2">
          <Skeleton className="h-24 w-full" />
          <Skeleton className="h-24 w-full" />
          <Skeleton className="h-24 w-full" />
          <Skeleton className="h-24 w-full" />
        </div>
      </div>
    );
  }
  if (!protein) return <p className="text-sm text-red-500 mt-8">Protein not found.</p>;

  const meta = protein.metadata;

  // Group annotations by aspect, then dedup by go_id (merging evidence codes
  // and provenance sources so the same GO term is shown once per aspect).
  const byAspect: Record<string, GroupedAnnotation[]> = { F: [], P: [], C: [], other: [] };
  const byAspectIndex: Record<string, Map<string, GroupedAnnotation>> = {
    F: new Map(), P: new Map(), C: new Map(), other: new Map(),
  };
  for (const ann of annotations) {
    const key = ann.aspect && byAspectIndex[ann.aspect] ? ann.aspect : "other";
    const index = byAspectIndex[key];
    const existing = index.get(ann.go_id);
    const source: AnnotationSource = {
      source: ann.annotation_set_source,
      version: ann.annotation_set_version,
    };
    if (existing) {
      if (ann.evidence_code && !existing.evidence_codes.includes(ann.evidence_code))
        existing.evidence_codes.push(ann.evidence_code);
      if (ann.qualifier && !existing.qualifiers.includes(ann.qualifier))
        existing.qualifiers.push(ann.qualifier);
      if (!existing.sources.some((s) => s.source === source.source && s.version === source.version))
        existing.sources.push(source);
    } else {
      const grouped: GroupedAnnotation = {
        go_id: ann.go_id,
        name: ann.name,
        aspect: ann.aspect,
        evidence_codes: ann.evidence_code ? [ann.evidence_code] : [],
        qualifiers: ann.qualifier ? [ann.qualifier] : [],
        sources: [source],
      };
      index.set(ann.go_id, grouped);
      byAspect[key].push(grouped);
    }
  }

  const tabs: { key: Tab; label: string }[] = [
    { key: "overview", label: t("tabs.overview") },
    { key: "annotations", label: `${t("tabs.annotations")}${protein.go_annotation_count > 0 ? ` (${protein.go_annotation_count.toLocaleString()})` : ""}` },
  ];

  return (
    <>
      {/* Header */}
      <div className="mb-6">
        <Breadcrumbs />
        <div className="flex items-start gap-4 mt-2">
          <div>
            <h1 className="text-2xl font-bold text-slate-900 font-mono">{protein.accession}</h1>
            {protein.entry_name && <p className="text-sm text-slate-500 mt-0.5">{protein.entry_name}</p>}
          </div>
          <div className="ml-auto flex flex-wrap gap-2">
            {protein.reviewed === true && (
              <span className="rounded-full bg-blue-50 px-3 py-1 text-xs font-medium text-blue-700 border border-blue-100">Swiss-Prot</span>
            )}
            {protein.reviewed === false && (
              <span className="rounded-full bg-slate-50 px-3 py-1 text-xs font-medium text-slate-500 border border-slate-200">TrEMBL</span>
            )}
            {!protein.is_canonical && (
              <span className="rounded-full bg-amber-50 px-3 py-1 text-xs font-medium text-amber-700 border border-amber-100">
                Isoform {protein.isoform_index}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Tabs */}
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

      {/* ── Overview ── */}
      {activeTab === "overview" && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left: core info + coverage */}
          <div className="space-y-4">
            <div className="rounded-lg border bg-white p-4 shadow-sm space-y-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">{t("overviewTab.identity")}</p>
              <div className="space-y-2 text-sm">
                {protein.gene_name && (
                  <div className="flex justify-between">
                    <span className="text-slate-500">{t("overviewTab.gene")}</span>
                    <span className="font-medium text-slate-900">{protein.gene_name}</span>
                  </div>
                )}
                {protein.organism && (
                  <div className="flex justify-between gap-4">
                    <span className="text-slate-500 shrink-0">{t("overviewTab.organism")}</span>
                    <span className="text-slate-800 text-right">{protein.organism}</span>
                  </div>
                )}
                {protein.taxonomy_id && (
                  <div className="flex justify-between">
                    <span className="text-slate-500">{t("overviewTab.taxonId")}</span>
                    <span className="font-mono text-[13px] text-slate-700">{protein.taxonomy_id}</span>
                  </div>
                )}
                {protein.length && (
                  <div className="flex justify-between">
                    <span className="text-slate-500">{t("overviewTab.length")}</span>
                    <span className="text-slate-800">{protein.length.toLocaleString()} {t("overviewTab.aa")}</span>
                  </div>
                )}
                {protein.sequence_id && (
                  <div className="flex justify-between">
                    <span className="text-slate-500">{t("overviewTab.sequenceId")}</span>
                    <span className="font-mono text-[13px] text-slate-600">{protein.sequence_id}</span>
                  </div>
                )}
                {!protein.is_canonical && (
                  <div className="flex justify-between">
                    <span className="text-slate-500">{t("overviewTab.canonical")}</span>
                    <Link href={`/proteins/${protein.canonical_accession}`} className="font-mono text-xs text-blue-600 hover:underline">
                      {protein.canonical_accession}
                    </Link>
                  </div>
                )}
              </div>
            </div>

            <div className="rounded-lg border bg-white p-4 shadow-sm space-y-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">{t("overviewTab.coverage")}</p>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-slate-500">{t("overviewTab.embeddings")}</span>
                  <span className={protein.embedding_count > 0 ? "text-green-600 font-medium" : "text-slate-600"}>
                    {protein.embedding_count > 0 ? `${protein.embedding_count} config${protein.embedding_count !== 1 ? "s" : ""}` : t("overviewTab.none")}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">{t("overviewTab.goAnnotations")}</span>
                  <button
                    onClick={() => setActiveTab("annotations")}
                    className={protein.go_annotation_count > 0 ? "text-green-600 font-medium hover:underline" : "text-slate-600 cursor-default"}
                  >
                    {protein.go_annotation_count > 0 ? protein.go_annotation_count.toLocaleString() : t("overviewTab.none")}
                  </button>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">{t("overviewTab.metadata")}</span>
                  <span className={meta ? "text-green-600 font-medium" : "text-slate-600"}>{meta ? t("overviewTab.yes") : t("overviewTab.none")}</span>
                </div>
              </div>
            </div>

            {protein.isoforms.length > 0 && (
              <div className="rounded-lg border bg-white p-4 shadow-sm">
                <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-3">{t("overviewTab.isoforms")}</p>
                <div className="space-y-1">
                  {protein.isoforms.map((iso) => (
                    <Link key={iso} href={`/proteins/${iso}`} className="block font-mono text-xs text-blue-600 hover:underline">
                      {iso}
                    </Link>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Right: functional metadata */}
          <div className="lg:col-span-2 space-y-4">
            {meta ? (
              <>
                {meta.function_cc && (
                  <div className="rounded-lg border bg-white p-4 shadow-sm">
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-2">{t("overviewTab.function")}</p>
                    <p className="text-sm text-slate-800 whitespace-pre-wrap">{meta.function_cc}</p>
                  </div>
                )}
                <div className="rounded-lg border bg-white p-4 shadow-sm space-y-4">
                  <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">{t("overviewTab.biochemistry")}</p>
                  <Field label={t("overviewTab.ecNumber")} value={meta.ec_number} />
                  <Field label={t("overviewTab.catalyticActivity")} value={meta.catalytic_activity} />
                  <Field label={t("overviewTab.cofactor")} value={meta.cofactor} />
                  <Field label={t("overviewTab.activityRegulation")} value={meta.activity_regulation} />
                  <Field label={t("overviewTab.pathway")} value={meta.pathway} />
                  <Field label={t("overviewTab.absorption")} value={meta.absorption} />
                  <Field label={t("overviewTab.kinetics")} value={meta.kinetics} />
                  <Field label={t("overviewTab.phDependence")} value={meta.ph_dependence} />
                  <Field label={t("overviewTab.redoxPotential")} value={meta.redox_potential} />
                  <Field label={t("overviewTab.temperatureDependence")} value={meta.temperature_dependence} />
                  <Field label={t("overviewTab.rheaId")} value={meta.rhea_id} />
                </div>
                {meta.keywords && (
                  <div className="rounded-lg border bg-white p-4 shadow-sm">
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-2">{t("overviewTab.keywords")}</p>
                    <div className="flex flex-wrap gap-1.5">
                      {meta.keywords.split(";").map((kw) => kw.trim()).filter(Boolean).map((kw) => (
                        <span key={kw} className="rounded bg-slate-100 px-2 py-0.5 text-[13px] text-slate-600">{kw}</span>
                      ))}
                    </div>
                  </div>
                )}
              </>
            ) : (
              <div className="rounded-lg border bg-white p-8 shadow-sm text-center text-sm text-slate-600">
                {t("overviewTab.noFunctionalMetadata")}
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── GO Annotations ── */}
      {activeTab === "annotations" && (
        <div>
          {loadingAnnotations && <p className="text-sm text-slate-600">Loading…</p>}

          {!loadingAnnotations && annotationsLoaded && annotations.length === 0 && (
            <div className="rounded-lg border bg-white p-8 text-center text-sm text-slate-600">
              {t("overviewTab.noGoAnnotations")}
            </div>
          )}

          {!loadingAnnotations && annotations.length > 0 && (
            <div className="space-y-6">
              {/* GO Graph button */}
              <div className="flex items-center gap-3">
                <button
                  onClick={async () => {
                    if (showGraph) { setShowGraph(false); return; }
                    setLoadingGraph(true);
                    setShowGraph(true);
                    try {
                      const snapshots = await listOntologySnapshots();
                      if (!snapshots.length) return;
                      const goIds = annotations.map((a) => a.go_id);
                      // Use the snapshot from the first annotation set
                      const snapshotId = snapshots[0].id;
                      setSubgraph(await getGoSubgraph(snapshotId, goIds, 3));
                    } catch (e: any) {
                      toast(e.message ?? tToast("loadGraphFailed"), "error");
                      setShowGraph(false);
                    } finally {
                      setLoadingGraph(false);
                    }
                  }}
                  className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-slate-50"
                >
                  {showGraph ? t("overviewTab.hideGoGraph") : t("overviewTab.showGoGraph")}
                </button>
                {loadingGraph && <span className="text-xs text-slate-600">{t("overviewTab.loadingGraph")}</span>}
              </div>

              {showGraph && subgraph && <GoGraph subgraph={subgraph} />}

              {/* Summary */}
              <div className="grid grid-cols-3 gap-3">
                {(["F", "P", "C"] as const).map((asp) => (
                  <div key={asp} className="rounded-lg border bg-white p-4 shadow-sm">
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">{t(`overviewTab.${asp === "F" ? "molecularFunction" : asp === "P" ? "biologicalProcess" : "cellularComponent"}`)}</p>
                    <p className="mt-1 text-2xl font-bold text-slate-900">{byAspect[asp].length}</p>
                    <p className="text-xs text-slate-600 mt-0.5">{t("overviewTab.annotations")}</p>
                  </div>
                ))}
              </div>

              {/* Per-aspect tables */}
              {(["F", "P", "C"] as const).map((asp) => {
                const terms = byAspect[asp];
                if (terms.length === 0) return null;
                return (
                  <div key={asp}>
                    <p className="text-sm font-semibold text-slate-700 mb-3">
                      {t(`overviewTab.${asp === "F" ? "molecularFunction" : asp === "P" ? "biologicalProcess" : "cellularComponent"}`)}
                      <span className="ml-2 text-xs font-normal text-slate-600">{terms.length} term{terms.length !== 1 ? "s" : ""}</span>
                    </p>
                    <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
                      <div className="grid grid-cols-[100px_1fr_80px_100px_100px] gap-2 border-b bg-slate-50 px-4 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                        <div>{t("overviewTab.goTableHeaders.goId")}</div>
                        <div>{t("overviewTab.goTableHeaders.name")}</div>
                        <div>{t("overviewTab.goTableHeaders.evidence")}</div>
                        <div>{t("overviewTab.goTableHeaders.qualifier")}</div>
                        <div>{t("overviewTab.goTableHeaders.source")}</div>
                      </div>
                      {terms.map((ann) => (
                        <div key={ann.go_id} className="grid grid-cols-[100px_1fr_80px_100px_100px] gap-2 border-b px-4 py-2.5 text-sm last:border-0 items-start">
                          <div className="font-mono text-xs text-blue-600 pt-0.5">{ann.go_id}</div>
                          <div className="text-xs text-slate-800 truncate" title={ann.name ?? ""}>{ann.name ?? "—"}</div>
                          <div className="flex flex-wrap gap-0.5">
                            {ann.evidence_codes.length === 0
                              ? <span className="text-[13px] text-slate-500">—</span>
                              : ann.evidence_codes.map((ec) => (
                                <span key={ec} className="rounded border border-slate-200 bg-slate-50 px-1 py-0.5 text-[10px] font-mono font-medium text-slate-600">{ec}</span>
                              ))}
                          </div>
                          <div className="text-[13px] text-slate-500">{ann.qualifiers.length > 0 ? ann.qualifiers.join(", ") : "—"}</div>
                          <div className="flex flex-wrap gap-0.5">
                            {ann.sources.map((s) => (
                              <span key={`${s.source}-${s.version ?? ""}`} className="rounded bg-slate-100 px-1.5 py-0.5 text-[11px] text-slate-600">{formatSource(s)}</span>
                            ))}
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
