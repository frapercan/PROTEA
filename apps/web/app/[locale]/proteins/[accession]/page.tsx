"use client";

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { useToast } from "@/components/Toast";
import { useTranslations } from "next-intl";
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
  if (!aspect) return <span className="text-gray-300 text-xs">—</span>;
  return (
    <span className={`rounded-full border px-2 py-0.5 text-xs font-medium ${ASPECT_COLORS[aspect] ?? "bg-gray-50 text-gray-600"}`}>
      {aspect}
    </span>
  );
}

function Field({ label, value }: { label: string; value?: string | null }) {
  if (!value) return null;
  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-1">{label}</p>
      <p className="text-sm text-gray-800 whitespace-pre-wrap">{value}</p>
    </div>
  );
}

export default function ProteinDetailPage({ params }: { params: Promise<{ accession: string }> }) {
  const { accession } = use(params);
  const t = useTranslations("proteinDetail");
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
      .catch((e: any) => toast(e.message ?? "Failed to load protein", "error"))
      .finally(() => setLoading(false));
  }, [accession]);

  useEffect(() => {
    if (activeTab === "annotations" && !annotationsLoaded) {
      setLoadingAnnotations(true);
      getProteinAnnotations(decodeURIComponent(accession))
        .then((data) => { setAnnotations(data); setAnnotationsLoaded(true); })
        .catch((e: any) => toast(e.message ?? "Failed to load annotations", "error"))
        .finally(() => setLoadingAnnotations(false));
    }
    if (activeTab !== "annotations") { setShowGraph(false); setSubgraph(null); }
  }, [activeTab]);

  if (loading) return <p className="text-sm text-gray-400 mt-8">Loading…</p>;
  if (!protein) return <p className="text-sm text-red-500 mt-8">Protein not found.</p>;

  const meta = protein.metadata;

  // Group annotations by aspect
  const byAspect: Record<string, ProteinAnnotation[]> = { F: [], P: [], C: [], other: [] };
  for (const ann of annotations) {
    const key = ann.aspect && byAspect[ann.aspect] ? ann.aspect : "other";
    byAspect[key].push(ann);
  }

  const tabs: { key: Tab; label: string }[] = [
    { key: "overview", label: t("tabs.overview") },
    { key: "annotations", label: `${t("tabs.annotations")}${protein.go_annotation_count > 0 ? ` (${protein.go_annotation_count.toLocaleString()})` : ""}` },
  ];

  return (
    <>
      {/* Header */}
      <div className="mb-6">
        <Link href="/proteins" className="text-sm text-gray-400 hover:text-gray-600">{t("backToProteins")}</Link>
        <div className="flex items-start gap-4 mt-2">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 font-mono">{protein.accession}</h1>
            {protein.entry_name && <p className="text-sm text-gray-500 mt-0.5">{protein.entry_name}</p>}
          </div>
          <div className="ml-auto flex flex-wrap gap-2">
            {protein.reviewed === true && (
              <span className="rounded-full bg-blue-50 px-3 py-1 text-xs font-medium text-blue-700 border border-blue-100">Swiss-Prot</span>
            )}
            {protein.reviewed === false && (
              <span className="rounded-full bg-gray-50 px-3 py-1 text-xs font-medium text-gray-500 border border-gray-200">TrEMBL</span>
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
                : "border-transparent text-gray-500 hover:text-gray-700"
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
              <p className="text-xs font-semibold uppercase tracking-wide text-gray-400">{t("overviewTab.identity")}</p>
              <div className="space-y-2 text-sm">
                {protein.gene_name && (
                  <div className="flex justify-between">
                    <span className="text-gray-500">{t("overviewTab.gene")}</span>
                    <span className="font-medium text-gray-900">{protein.gene_name}</span>
                  </div>
                )}
                {protein.organism && (
                  <div className="flex justify-between gap-4">
                    <span className="text-gray-500 shrink-0">{t("overviewTab.organism")}</span>
                    <span className="text-gray-800 text-right">{protein.organism}</span>
                  </div>
                )}
                {protein.taxonomy_id && (
                  <div className="flex justify-between">
                    <span className="text-gray-500">{t("overviewTab.taxonId")}</span>
                    <span className="font-mono text-xs text-gray-700">{protein.taxonomy_id}</span>
                  </div>
                )}
                {protein.length && (
                  <div className="flex justify-between">
                    <span className="text-gray-500">{t("overviewTab.length")}</span>
                    <span className="text-gray-800">{protein.length.toLocaleString()} {t("overviewTab.aa")}</span>
                  </div>
                )}
                {protein.sequence_id && (
                  <div className="flex justify-between">
                    <span className="text-gray-500">{t("overviewTab.sequenceId")}</span>
                    <span className="font-mono text-xs text-gray-600">{protein.sequence_id}</span>
                  </div>
                )}
                {!protein.is_canonical && (
                  <div className="flex justify-between">
                    <span className="text-gray-500">{t("overviewTab.canonical")}</span>
                    <Link href={`/proteins/${protein.canonical_accession}`} className="font-mono text-xs text-blue-600 hover:underline">
                      {protein.canonical_accession}
                    </Link>
                  </div>
                )}
              </div>
            </div>

            <div className="rounded-lg border bg-white p-4 shadow-sm space-y-3">
              <p className="text-xs font-semibold uppercase tracking-wide text-gray-400">{t("overviewTab.coverage")}</p>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-500">{t("overviewTab.embeddings")}</span>
                  <span className={protein.embedding_count > 0 ? "text-green-600 font-medium" : "text-gray-400"}>
                    {protein.embedding_count > 0 ? `${protein.embedding_count} config${protein.embedding_count !== 1 ? "s" : ""}` : t("overviewTab.none")}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">{t("overviewTab.goAnnotations")}</span>
                  <button
                    onClick={() => setActiveTab("annotations")}
                    className={protein.go_annotation_count > 0 ? "text-green-600 font-medium hover:underline" : "text-gray-400 cursor-default"}
                  >
                    {protein.go_annotation_count > 0 ? protein.go_annotation_count.toLocaleString() : t("overviewTab.none")}
                  </button>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">{t("overviewTab.metadata")}</span>
                  <span className={meta ? "text-green-600 font-medium" : "text-gray-400"}>{meta ? t("overviewTab.yes") : t("overviewTab.none")}</span>
                </div>
              </div>
            </div>

            {protein.isoforms.length > 0 && (
              <div className="rounded-lg border bg-white p-4 shadow-sm">
                <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-3">{t("overviewTab.isoforms")}</p>
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
                    <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-2">{t("overviewTab.function")}</p>
                    <p className="text-sm text-gray-800 whitespace-pre-wrap">{meta.function_cc}</p>
                  </div>
                )}
                <div className="rounded-lg border bg-white p-4 shadow-sm space-y-4">
                  <p className="text-xs font-semibold uppercase tracking-wide text-gray-400">{t("overviewTab.biochemistry")}</p>
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
                    <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-2">{t("overviewTab.keywords")}</p>
                    <div className="flex flex-wrap gap-1.5">
                      {meta.keywords.split(";").map((kw) => kw.trim()).filter(Boolean).map((kw) => (
                        <span key={kw} className="rounded bg-gray-100 px-2 py-0.5 text-xs text-gray-600">{kw}</span>
                      ))}
                    </div>
                  </div>
                )}
              </>
            ) : (
              <div className="rounded-lg border bg-white p-8 shadow-sm text-center text-sm text-gray-400">
                {t("overviewTab.noFunctionalMetadata")}
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── GO Annotations ── */}
      {activeTab === "annotations" && (
        <div>
          {loadingAnnotations && <p className="text-sm text-gray-400">Loading…</p>}

          {!loadingAnnotations && annotationsLoaded && annotations.length === 0 && (
            <div className="rounded-lg border bg-white p-8 text-center text-sm text-gray-400">
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
                      toast(e.message ?? "Failed to load graph", "error");
                      setShowGraph(false);
                    } finally {
                      setLoadingGraph(false);
                    }
                  }}
                  className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50"
                >
                  {showGraph ? t("overviewTab.hideGoGraph") : t("overviewTab.showGoGraph")}
                </button>
                {loadingGraph && <span className="text-xs text-gray-400">{t("overviewTab.loadingGraph")}</span>}
              </div>

              {showGraph && subgraph && <GoGraph subgraph={subgraph} />}

              {/* Summary */}
              <div className="grid grid-cols-3 gap-3">
                {(["F", "P", "C"] as const).map((asp) => (
                  <div key={asp} className="rounded-lg border bg-white p-4 shadow-sm">
                    <p className="text-xs font-semibold uppercase tracking-wide text-gray-400">{t(`overviewTab.${asp === "F" ? "molecularFunction" : asp === "P" ? "biologicalProcess" : "cellularComponent"}`)}</p>
                    <p className="mt-1 text-2xl font-bold text-gray-900">{byAspect[asp].length}</p>
                    <p className="text-xs text-gray-400 mt-0.5">{t("overviewTab.annotations")}</p>
                  </div>
                ))}
              </div>

              {/* Per-aspect tables */}
              {(["F", "P", "C"] as const).map((asp) => {
                const terms = byAspect[asp];
                if (terms.length === 0) return null;
                return (
                  <div key={asp}>
                    <p className="text-sm font-semibold text-gray-700 mb-3">
                      {t(`overviewTab.${asp === "F" ? "molecularFunction" : asp === "P" ? "biologicalProcess" : "cellularComponent"}`)}
                      <span className="ml-2 text-xs font-normal text-gray-400">{terms.length} term{terms.length !== 1 ? "s" : ""}</span>
                    </p>
                    <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
                      <div className="grid grid-cols-[100px_1fr_80px_100px_100px] gap-2 border-b bg-gray-50 px-4 py-2 text-xs font-semibold uppercase tracking-wide text-gray-500">
                        <div>{t("overviewTab.goTableHeaders.goId")}</div>
                        <div>{t("overviewTab.goTableHeaders.name")}</div>
                        <div>{t("overviewTab.goTableHeaders.evidence")}</div>
                        <div>{t("overviewTab.goTableHeaders.qualifier")}</div>
                        <div>{t("overviewTab.goTableHeaders.source")}</div>
                      </div>
                      {terms.map((ann, i) => (
                        <div key={i} className="grid grid-cols-[100px_1fr_80px_100px_100px] gap-2 border-b px-4 py-2.5 text-sm last:border-0 items-center">
                          <div className="font-mono text-xs text-blue-600">{ann.go_id}</div>
                          <div className="text-xs text-gray-800 truncate" title={ann.name ?? ""}>{ann.name ?? "—"}</div>
                          <div className="text-xs text-gray-500">{ann.evidence_code ?? "—"}</div>
                          <div className="text-xs text-gray-500">{ann.qualifier ?? "—"}</div>
                          <div className="text-xs text-gray-400">{ann.annotation_set_source}</div>
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
