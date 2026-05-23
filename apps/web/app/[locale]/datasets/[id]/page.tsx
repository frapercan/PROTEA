"use client";

import Link from "next/link";
import { use, useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import {
  getDataset,
  listEmbeddingConfigs,
  listOntologySnapshots,
  type Dataset,
  type EmbeddingConfig,
  type OntologySnapshot,
} from "@/lib/api";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import { Skeleton } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";

/**
 * Full provenance view of a frozen reranker dataset.
 *
 * Surfaces every field the lab needs to reproduce a dump: full schema_sha
 * + manifest_sha (copyable), embedding-config provenance (model, layer,
 * pooling, normalisation), ontology snapshot version, the train/eval
 * snapshot windows, artifact-store URIs, producer git-sha + version,
 * and a link back to the child ``export_research_dataset`` job that
 * produced it.
 *
 * The complete ``meta`` JSONB is rendered as collapsed JSON so the
 * registry stays a self-describing artefact (this is the same data the
 * lab's pull_dataset.py reads — no need to dig into the DB).
 */

function copyToClipboard(text: string): Promise<void> {
  return navigator.clipboard.writeText(text);
}

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

  useEffect(() => {
    let cancelled = false;
    getDataset(id)
      .then(async (d) => {
        if (cancelled) return;
        setDataset(d);
        // Look up embedding config + ontology snapshot once so we can
        // render the human-readable provenance block. Both endpoints are
        // cacheable so this is cheap even on repeat navigations.
        const [configs, snapshots] = await Promise.all([
          listEmbeddingConfigs().catch(() => [] as EmbeddingConfig[]),
          listOntologySnapshots().catch(() => [] as OntologySnapshot[]),
        ]);
        if (cancelled) return;
        setEmbCfg(configs.find((c) => c.id === d.embedding_config_id) ?? null);
        setSnapshot(snapshots.find((s) => s.id === d.ontology_snapshot_id) ?? null);
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
            <KV
              label={t("ontologySnapshot")}
              value={snapshot ? snapshot.obo_version : dataset.ontology_snapshot_id ?? "—"}
              mono
            />
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

        {/* Artifact URIs */}
        <section className="rounded-lg border bg-white shadow-sm">
          <h2 className="px-4 py-3 border-b text-sm font-semibold text-slate-800">
            {t("artifactsTitle")} <span className="text-xs font-normal text-slate-500">({dataset.storage_backend})</span>
          </h2>
          <dl className="divide-y divide-slate-100">
            <ShaRow label={t("keyPrefix")} value={dataset.key_prefix} onCopy={() => copy(dataset.key_prefix, t("keyPrefix"))} />
            <ShaRow label={t("manifestUri")} value={dataset.manifest_uri} onCopy={() => copy(dataset.manifest_uri, t("manifestUri"))} truncate />
            <ShaRow label={t("trainUri")} value={dataset.train_uri} onCopy={() => copy(dataset.train_uri ?? "", t("trainUri"))} truncate />
            <ShaRow label={t("evalUri")} value={dataset.eval_uri} onCopy={() => copy(dataset.eval_uri ?? "", t("evalUri"))} truncate />
          </dl>
        </section>

        {/* Raw payload + meta (the original export_research_dataset payload
            lives on the Job row, but the dataset row's meta carries the
            producer-side serialisation — surface both as JSON details). */}
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
