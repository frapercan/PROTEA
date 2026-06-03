"use client";

import Link from "next/link";
import { use, useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import {
  getDataset,
  getDatasetStats,
  getDatasetDownloadUrl,
  listEmbeddingConfigs,
  listOntologySnapshots,
  type Dataset,
  type EmbeddingConfig,
  type OntologySnapshot,
  type DatasetAspectStats,
} from "@/lib/api";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import { Skeleton } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";

/**
 * Full provenance view of a frozen reranker dataset.
 *
 * Features:
 *  1. Download buttons for train.parquet, eval.parquet, manifest.json
 *  2. Aspect statistics card (BPO / MFO / CCO protein/GO-term/annotation counts)
 *  3. Ontology snapshot version linked to the annotations page
 *  4. Aspect distribution donut + training-window timeline (SVG, no extra lib)
 */

function copyToClipboard(text: string): Promise<void> {
  return navigator.clipboard.writeText(text);
}

// ---------------------------------------------------------------------------
// SVG Charts
// ---------------------------------------------------------------------------

const ASPECT_COLORS: Record<string, string> = {
  bpo: "#6366f1",
  mfo: "#10b981",
  cco: "#f59e0b",
};

function AspectDonut({ stats }: { stats: Record<string, { annotations: number }> }) {
  const entries = Object.entries(stats).filter(([, v]) => v.annotations > 0);
  if (entries.length === 0) return <p className="text-sm text-slate-400">No data</p>;

  const total = entries.reduce((s, [, v]) => s + v.annotations, 0);
  const CX = 80;
  const CY = 80;
  const R = 60;
  const INNER_R = 36;

  let cumAngle = -Math.PI / 2;
  const slices = entries.map(([key, val]) => {
    const angle = (val.annotations / total) * 2 * Math.PI;
    const start = cumAngle;
    cumAngle += angle;
    return { key, angle, start, end: cumAngle, pct: (val.annotations / total) * 100 };
  });

  function arc(startA: number, endA: number, outerR: number, innerR: number): string {
    const x1 = CX + outerR * Math.cos(startA);
    const y1 = CY + outerR * Math.sin(startA);
    const x2 = CX + outerR * Math.cos(endA);
    const y2 = CY + outerR * Math.sin(endA);
    const x3 = CX + innerR * Math.cos(endA);
    const y3 = CY + innerR * Math.sin(endA);
    const x4 = CX + innerR * Math.cos(startA);
    const y4 = CY + innerR * Math.sin(startA);
    const large = endA - startA > Math.PI ? 1 : 0;
    return `M ${x1} ${y1} A ${outerR} ${outerR} 0 ${large} 1 ${x2} ${y2} L ${x3} ${y3} A ${innerR} ${innerR} 0 ${large} 0 ${x4} ${y4} Z`;
  }

  return (
    <div className="flex items-center gap-4">
      <svg width={160} height={160} viewBox="0 0 160 160" aria-hidden="true">
        {slices.map((s) => (
          <path
            key={s.key}
            d={arc(s.start, s.end, R, INNER_R)}
            fill={ASPECT_COLORS[s.key] ?? "#94a3b8"}
            stroke="white"
            strokeWidth={2}
          />
        ))}
        <text x={CX} y={CY} textAnchor="middle" dominantBaseline="middle" className="text-[11px] font-mono fill-slate-600" fontSize={11}>
          {total.toLocaleString()}
        </text>
      </svg>
      <ul className="space-y-1">
        {slices.map((s) => (
          <li key={s.key} className="flex items-center gap-2 text-xs">
            <span
              className="inline-block h-3 w-3 rounded-sm flex-shrink-0"
              style={{ background: ASPECT_COLORS[s.key] ?? "#94a3b8" }}
            />
            <span className="font-medium uppercase text-slate-700">{s.key}</span>
            <span className="text-slate-500">{s.pct.toFixed(1)}%</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function TrainWindowTimeline({ pairs }: { pairs: string[] }) {
  if (pairs.length === 0) return <p className="text-sm text-slate-400">No training windows registered</p>;

  const versionSet = new Set<number>();
  pairs.forEach((p) => {
    const m = p.match(/v?(\d+)-v?(\d+)/);
    if (m) {
      versionSet.add(Number(m[1]));
      versionSet.add(Number(m[2]));
    }
  });
  const versions = Array.from(versionSet).sort((a, b) => a - b);
  if (versions.length < 2) return (
    <div className="flex flex-wrap gap-1">
      {pairs.map((p) => (
        <span key={p} className="rounded bg-indigo-100 px-2 py-0.5 text-xs font-mono text-indigo-800">{p}</span>
      ))}
    </div>
  );

  const vMin = versions[0];
  const vMax = versions[versions.length - 1];
  const range = vMax - vMin || 1;
  const W = 280;
  const H = 28;
  const PAD = 20;
  const trackW = W - PAD * 2;

  function xOf(v: number) {
    return PAD + ((v - vMin) / range) * trackW;
  }

  const barH = 10;
  const barY = (H - barH) / 2;

  return (
    <div className="overflow-x-auto">
      <svg width={W} height={H + 16} viewBox={`0 0 ${W} ${H + 16}`} aria-label="Training window coverage timeline">
        <rect x={PAD} y={barY} width={trackW} height={barH} rx={3} fill="#e2e8f0" />
        {pairs.map((p, i) => {
          const m = p.match(/v?(\d+)-v?(\d+)/);
          if (!m) return null;
          const x1 = xOf(Number(m[1]));
          const x2 = xOf(Number(m[2]));
          return (
            <rect
              key={i}
              x={x1}
              y={barY}
              width={Math.max(x2 - x1, 4)}
              height={barH}
              rx={3}
              fill="#6366f1"
              opacity={0.75}
            />
          );
        })}
        {versions.map((v) => (
          <g key={v}>
            <line x1={xOf(v)} y1={barY + barH} x2={xOf(v)} y2={barY + barH + 4} stroke="#94a3b8" strokeWidth={1} />
            <text x={xOf(v)} y={barY + barH + 13} textAnchor="middle" fontSize={9} fill="#94a3b8">
              v{v}
            </text>
          </g>
        ))}
      </svg>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function DatasetDetail({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const t = useTranslations("datasets.detail");
  const tList = useTranslations("datasets");
  const locale = useLocale();
  const toast = useToast();

  const [dataset, setDataset] = useState<Dataset | null>(null);
  const [embCfg, setEmbCfg] = useState<EmbeddingConfig | null>(null);
  const [snapshot, setSnapshot] = useState<OntologySnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [statsData, setStatsData] = useState<DatasetAspectStats | null>(null);
  const [statsLoading, setStatsLoading] = useState(false);
  const [statsError, setStatsError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getDataset(id)
      .then(async (d) => {
        if (cancelled) return;
        setDataset(d);
        const [configs, snapshots] = await Promise.all([
          listEmbeddingConfigs().catch(() => [] as EmbeddingConfig[]),
          listOntologySnapshots().catch(() => [] as OntologySnapshot[]),
        ]);
        if (cancelled) return;
        setEmbCfg(configs.find((c) => c.id === d.embedding_config_id) ?? null);
        setSnapshot(snapshots.find((s) => s.id === d.ontology_snapshot_id) ?? null);
        // Load aspect stats in parallel without blocking the main render.
        setStatsLoading(true);
        getDatasetStats(d.id)
          .then((s) => {
            if (!cancelled) setStatsData(s);
          })
          .catch((e: unknown) => {
            if (!cancelled) setStatsError(e instanceof Error ? e.message : String(e));
          })
          .finally(() => {
            if (!cancelled) setStatsLoading(false);
          });
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : String(e);
        setError(msg);
      });
    return () => {
      cancelled = true;
    };
  }, [id]);

  async function copy(value: string | null | undefined, fieldLabel: string) {
    if (!value) return;
    try {
      await copyToClipboard(value);
      toast(t("copied", { field: fieldLabel }), "info");
    } catch {
      toast(t("copyFailed"), "error");
    }
  }

  if (error) {
    return (
      <>
        <Breadcrumbs />
        <div className="max-w-4xl mx-auto px-4 sm:px-6 py-12">
          <div className="rounded-lg border border-red-200 bg-red-50 p-6">
            <p className="text-red-800 text-sm">{error}</p>
            <Link href={`/${locale}/datasets`} className="mt-2 inline-block text-sm text-red-700 underline">
              {t("backToList")}
            </Link>
          </div>
        </div>
      </>
    );
  }

  if (!dataset) {
    return (
      <>
        <Breadcrumbs />
        <div className="max-w-4xl mx-auto px-4 sm:px-6 py-8 space-y-4">
          <Skeleton className="h-8 w-2/3" />
          <Skeleton className="h-4 w-1/2" />
          <Skeleton className="h-64 rounded-lg" />
        </div>
      </>
    );
  }

  const importedByReference = dataset.operation === "import_by_reference";
  const aspectStats = statsData?.aspect_stats ?? {};
  const hasAspectStats = Object.keys(aspectStats).length > 0;

  return (
    <>
      <Breadcrumbs />
      <div className="max-w-5xl mx-auto px-4 sm:px-6 py-6 space-y-6">
        {/* Header */}
        <header className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-3">
          <div className="min-w-0">
            <h1 className="text-2xl font-bold text-slate-900 break-all font-mono">
              {dataset.name}
            </h1>
            <p className="text-sm text-slate-500 mt-1">
              {importedByReference ? t("importedByReference") : t("producedByExport")}
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <Link
              href={`/${locale}/datasets`}
              className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
            >
              {t("backToList")}
            </Link>
            {dataset.job_id && (
              <Link
                href={`/${locale}/jobs/${dataset.job_id}`}
                className="rounded-md border border-blue-200 bg-blue-50 px-3 py-1.5 text-sm font-medium text-blue-800 hover:bg-blue-100"
              >
                {t("viewJob")}
              </Link>
            )}
          </div>
        </header>

        {/* Stat strip */}
        <section className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <Stat label={tList("cols.k")} value={String(dataset.k)} />
          <Stat label={tList("cols.nTrain")} value={dataset.n_train_rows.toLocaleString()} />
          <Stat label={tList("cols.nEval")} value={dataset.n_eval_rows.toLocaleString()} />
          <Stat label={t("annotationSource")} value={dataset.annotation_source} />
        </section>

        {/* Provenance shas */}
        <section className="rounded-lg border bg-white shadow-sm">
          <h2 className="px-4 py-3 border-b text-sm font-semibold text-slate-800">
            {t("provenanceTitle")}
          </h2>
          <dl className="divide-y divide-slate-100">
            <ShaRow label={t("schemaSha")} value={dataset.schema_sha} onCopy={() => copy(dataset.schema_sha, t("schemaSha"))} help={t("schemaShaHelp")} />
            <ShaRow label={t("manifestSha")} value={dataset.manifest_sha} onCopy={() => copy(dataset.manifest_sha, t("manifestSha"))} help={t("manifestShaHelp")} />
            <ShaRow label={t("datasetId")} value={dataset.id} onCopy={() => copy(dataset.id, t("datasetId"))} />
            {dataset.producer_version && (
              <ShaRow label={t("producerVersion")} value={dataset.producer_version} onCopy={() => copy(dataset.producer_version!, t("producerVersion"))} />
            )}
            {dataset.producer_git_sha && (
              <ShaRow label={t("producerGitSha")} value={dataset.producer_git_sha} onCopy={() => copy(dataset.producer_git_sha!, t("producerGitSha"))} />
            )}
          </dl>
        </section>

        {/* Embedding config provenance */}
        <section className="rounded-lg border bg-white shadow-sm">
          <h2 className="px-4 py-3 border-b text-sm font-semibold text-slate-800 flex items-center justify-between">
            {t("embeddingProvenance")}
            {dataset.embedding_config_id && (
              <Link
                href={`/${locale}/embeddings`}
                className="text-xs font-normal text-blue-700 hover:underline"
              >
                {t("openEmbeddings")}
              </Link>
            )}
          </h2>
          {!dataset.embedding_config_id ? (
            <p className="px-4 py-6 text-sm text-slate-500">{t("embeddingMissing")}</p>
          ) : !embCfg ? (
            <div className="px-4 py-6 space-y-2">
              <Skeleton className="h-3 w-1/2" />
              <Skeleton className="h-3 w-1/3" />
              <p className="text-xs text-slate-400 mt-2">{t("embeddingNotResolved", { id: dataset.embedding_config_id })}</p>
            </div>
          ) : (
            <dl className="grid grid-cols-1 sm:grid-cols-2 gap-x-4 gap-y-2 px-4 py-3 text-sm">
              <KV label={t("model")} value={embCfg.model_name} mono />
              <KV label={t("backend")} value={embCfg.model_backend} mono />
              <KV label={t("pooling")} value={embCfg.pooling} mono />
              <KV label={t("layerAgg")} value={embCfg.layer_agg} mono />
              <KV label={t("layerIndices")} value={embCfg.layer_indices.join(", ")} mono />
              <KV label={t("normalise")} value={embCfg.normalize ? "true" : "false"} mono />
              <KV label={t("normaliseResidues")} value={embCfg.normalize_residues ? "true" : "false"} mono />
              <KV label={t("maxLength")} value={String(embCfg.max_length)} mono />
            </dl>
          )}
        </section>

        {/* Snapshot windows */}
        <section className="rounded-lg border bg-white shadow-sm">
          <h2 className="px-4 py-3 border-b text-sm font-semibold text-slate-800">
            {t("snapshotsTitle")}
          </h2>
          <dl className="grid grid-cols-1 sm:grid-cols-2 gap-x-4 gap-y-2 px-4 py-3 text-sm">
            <div>
              <dt className="text-[11px] font-medium uppercase tracking-wider text-slate-500">{t("ontologySnapshot")}</dt>
              <dd className="mt-0.5 text-sm text-slate-800 break-all font-mono text-[13px]">
                {snapshot ? (
                  <Link
                    href={`/${locale}/annotations`}
                    className="text-blue-700 hover:underline"
                    title={t("openOntologyPage")}
                  >
                    {snapshot.obo_version}
                  </Link>
                ) : (
                  dataset.ontology_snapshot_id ?? <span className="text-slate-400">—</span>
                )}
              </dd>
            </div>
            <KV
              label={tList("cols.test")}
              value={dataset.eval_snapshot_pair ?? "—"}
              mono
            />
            <div className="sm:col-span-2">
              <dt className="text-xs font-medium uppercase tracking-wider text-slate-500">{tList("cols.trainWindow")}</dt>
              <dd className="mt-1 flex flex-wrap gap-1">
                {dataset.train_snapshot_pairs.length === 0 ? (
                  <span className="text-slate-400 text-sm">—</span>
                ) : (
                  dataset.train_snapshot_pairs.map((p) => (
                    <span key={p} className="inline-flex items-center rounded-md bg-slate-100 px-2 py-0.5 text-xs font-mono text-slate-700">
                      {p}
                    </span>
                  ))
                )}
              </dd>
            </div>
          </dl>
        </section>

        {/* Charts row */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Aspect distribution donut */}
          <section className="rounded-lg border bg-white shadow-sm p-4">
            <h2 className="text-sm font-semibold text-slate-800 mb-3">{t("aspectDonut")}</h2>
            {statsLoading && <Skeleton className="h-32 rounded" />}
            {statsError && <p className="text-xs text-red-600">{t("aspectStatsError")}</p>}
            {!statsLoading && !statsError && hasAspectStats && (
              <AspectDonut stats={aspectStats} />
            )}
            {!statsLoading && !statsError && !hasAspectStats && (
              <p className="text-sm text-slate-400">No aspect data available</p>
            )}
          </section>

          {/* Training-window coverage timeline */}
          <section className="rounded-lg border bg-white shadow-sm p-4">
            <h2 className="text-sm font-semibold text-slate-800 mb-3">{t("trainWindowChart")}</h2>
            <TrainWindowTimeline pairs={dataset.train_snapshot_pairs} />
          </section>
        </div>

        {/* Aspect statistics card */}
        <section className="rounded-lg border bg-white shadow-sm">
          <h2 className="px-4 py-3 border-b text-sm font-semibold text-slate-800">
            {t("aspectStatsTitle")}
          </h2>
          {statsLoading && (
            <div className="px-4 py-4 space-y-2">
              <Skeleton className="h-4 w-2/3" />
              <Skeleton className="h-4 w-1/2" />
            </div>
          )}
          {statsError && (
            <p className="px-4 py-4 text-sm text-red-600">{t("aspectStatsError")}</p>
          )}
          {!statsLoading && !statsError && (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-slate-50">
                    <th className="px-4 py-2 text-left text-[11px] font-semibold uppercase tracking-wider text-slate-500">Aspect</th>
                    <th className="px-4 py-2 text-right text-[11px] font-semibold uppercase tracking-wider text-slate-500">{t("aspectProteins")}</th>
                    <th className="px-4 py-2 text-right text-[11px] font-semibold uppercase tracking-wider text-slate-500">{t("aspectGoTerms")}</th>
                    <th className="px-4 py-2 text-right text-[11px] font-semibold uppercase tracking-wider text-slate-500">{t("aspectAnnotations")}</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {(["bpo", "mfo", "cco"] as const).map((asp) => {
                    const s = aspectStats[asp];
                    return (
                      <tr key={asp} className="hover:bg-slate-50">
                        <td className="px-4 py-2">
                          <span
                            className="inline-flex items-center gap-1.5 font-mono font-semibold uppercase text-xs"
                            style={{ color: ASPECT_COLORS[asp] }}
                          >
                            <span
                              className="h-2 w-2 rounded-full inline-block"
                              style={{ background: ASPECT_COLORS[asp] }}
                            />
                            {t(asp as "bpo" | "mfo" | "cco")}
                          </span>
                        </td>
                        <td className="px-4 py-2 text-right font-mono text-slate-800">
                          {s ? s.proteins.toLocaleString() : <span className="text-slate-300">—</span>}
                        </td>
                        <td className="px-4 py-2 text-right font-mono text-slate-800">
                          {s ? s.go_terms.toLocaleString() : <span className="text-slate-300">—</span>}
                        </td>
                        <td className="px-4 py-2 text-right font-mono text-slate-800">
                          {s ? s.annotations.toLocaleString() : <span className="text-slate-300">—</span>}
                        </td>
                      </tr>
                    );
                  })}
                  {Object.keys(aspectStats).filter((k) => !["bpo", "mfo", "cco"].includes(k)).map((asp) => {
                    const s = aspectStats[asp];
                    return (
                      <tr key={asp} className="hover:bg-slate-50">
                        <td className="px-4 py-2 font-mono text-xs text-slate-600">{asp}</td>
                        <td className="px-4 py-2 text-right font-mono text-slate-800">{s.proteins.toLocaleString()}</td>
                        <td className="px-4 py-2 text-right font-mono text-slate-800">{s.go_terms.toLocaleString()}</td>
                        <td className="px-4 py-2 text-right font-mono text-slate-800">{s.annotations.toLocaleString()}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>

        {/* Artifact URIs + download buttons */}
        <section className="rounded-lg border bg-white shadow-sm">
          <h2 className="px-4 py-3 border-b text-sm font-semibold text-slate-800">
            {t("artifactsTitle")} <span className="text-xs font-normal text-slate-500">({dataset.storage_backend})</span>
          </h2>
          <dl className="divide-y divide-slate-100">
            <ShaRow label={t("keyPrefix")} value={dataset.key_prefix} onCopy={() => copy(dataset.key_prefix, t("keyPrefix"))} />
            <ArtifactRow
              label={t("manifestUri")}
              value={dataset.manifest_uri}
              onCopy={() => copy(dataset.manifest_uri, t("manifestUri"))}
              downloadUrl={getDatasetDownloadUrl(dataset.id, "manifest")}
              downloadLabel={t("downloadManifest")}
              truncate
            />
            <ArtifactRow
              label={t("trainUri")}
              value={dataset.train_uri}
              onCopy={() => copy(dataset.train_uri ?? "", t("trainUri"))}
              downloadUrl={dataset.train_uri ? getDatasetDownloadUrl(dataset.id, "train") : undefined}
              downloadLabel={t("downloadTrain")}
              truncate
            />
            <ArtifactRow
              label={t("evalUri")}
              value={dataset.eval_uri}
              onCopy={() => copy(dataset.eval_uri ?? "", t("evalUri"))}
              downloadUrl={dataset.eval_uri ? getDatasetDownloadUrl(dataset.id, "eval") : undefined}
              downloadLabel={t("downloadEval")}
              truncate
            />
          </dl>
        </section>

        {/* Raw payload + meta */}
        <section className="rounded-lg border bg-white shadow-sm">
          <h2 className="px-4 py-3 border-b text-sm font-semibold text-slate-800">
            {t("metaTitle")}
          </h2>
          <div className="px-4 py-3 space-y-3 text-sm">
            <details open>
              <summary className="cursor-pointer text-slate-600 hover:text-slate-900">
                {t("meta")}
              </summary>
              <pre className="mt-2 rounded bg-slate-50 p-3 text-xs overflow-auto border border-slate-100">
                {JSON.stringify(dataset.meta ?? {}, null, 2)}
              </pre>
            </details>
            {dataset.job_id && (
              <p className="text-xs text-slate-500">
                {t("payloadHint")}{" "}
                <Link href={`/${locale}/jobs/${dataset.job_id}`} className="text-blue-700 hover:underline">
                  {t("viewJob")}
                </Link>
              </p>
            )}
          </div>
        </section>

        <p className="text-xs text-slate-500">{t("footnote")}</p>
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border bg-white shadow-sm px-3 py-3">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">{label}</div>
      <div className="mt-1 text-lg font-semibold text-slate-900 tabular-nums break-all">{value}</div>
    </div>
  );
}

function KV({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div>
      <dt className="text-[11px] font-medium uppercase tracking-wider text-slate-500">{label}</dt>
      <dd className={`mt-0.5 text-sm text-slate-800 break-all ${mono ? "font-mono text-[13px]" : ""}`}>{value}</dd>
    </div>
  );
}

function ShaRow({
  label,
  value,
  onCopy,
  help,
  truncate = false,
}: {
  label: string;
  value: string | null;
  onCopy: () => void;
  help?: string;
  truncate?: boolean;
}) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-[180px_1fr_auto] gap-2 px-4 py-2.5">
      <dt className="text-xs font-medium uppercase tracking-wider text-slate-500 self-center">{label}</dt>
      <dd
        className={`text-sm font-mono text-slate-800 break-all self-center ${truncate ? "truncate" : ""}`}
        title={truncate && value ? value : undefined}
      >
        {value ?? <span className="text-slate-300">—</span>}
      </dd>
      {value && (
        <div className="flex items-center justify-end sm:col-start-3">
          <button
            type="button"
            onClick={onCopy}
            className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-medium text-slate-600 hover:bg-slate-50"
          >
            Copy
          </button>
        </div>
      )}
      {help && (
        <p className="sm:col-span-3 text-[11px] text-slate-500 mt-0.5">{help}</p>
      )}
    </div>
  );
}

function ArtifactRow({
  label,
  value,
  onCopy,
  downloadUrl,
  downloadLabel,
  truncate = false,
}: {
  label: string;
  value: string | null;
  onCopy: () => void;
  downloadUrl?: string;
  downloadLabel: string;
  truncate?: boolean;
}) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-[180px_1fr_auto] gap-2 px-4 py-2.5">
      <dt className="text-xs font-medium uppercase tracking-wider text-slate-500 self-center">{label}</dt>
      <dd
        className={`text-sm font-mono text-slate-800 break-all self-center ${truncate ? "truncate" : ""}`}
        title={truncate && value ? value : undefined}
      >
        {value ?? <span className="text-slate-300">—</span>}
      </dd>
      <div className="flex items-center justify-end gap-1.5 sm:col-start-3">
        {value && (
          <button
            type="button"
            onClick={onCopy}
            className="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] font-medium text-slate-600 hover:bg-slate-50"
          >
            Copy
          </button>
        )}
        {downloadUrl ? (
          <a
            href={downloadUrl}
            download
            title={downloadLabel}
            className="rounded-md border border-indigo-200 bg-indigo-50 px-2 py-1 text-[11px] font-medium text-indigo-700 hover:bg-indigo-100"
          >
            Download
          </a>
        ) : (
          <span className="rounded-md border border-slate-100 bg-slate-50 px-2 py-1 text-[11px] font-medium text-slate-300 cursor-not-allowed">
            Download
          </span>
        )}
      </div>
    </div>
  );
}
