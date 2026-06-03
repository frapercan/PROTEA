"use client";

import { useEffect, useMemo, useState } from "react";
import { useTranslations } from "next-intl";
import {
  createDataset,
  importDatasetByReference,
  listEmbeddingConfigs,
  listOntologySnapshots,
  type CreateDatasetPayload,
  type EmbeddingConfig,
  type ImportDatasetByReferencePayload,
  type OntologySnapshot,
} from "@/lib/api";
import { HelpDot } from "@/components/Tooltip";

/**
 * Modal with two tabs for the two dataset-registration paths PROTEA
 * exposes:
 *
 *   1. Dispatch export — ``POST /v1/datasets`` enqueues a fresh
 *      ``export_research_dataset`` job. Defaults mirror the canonical
 *      multi-PLM v226 sweep recipe (project_multi_plm_v226_sweep_plan +
 *      project_pca_transductive_decision).
 *   2. Import from reference — ``POST /v1/datasets/import-by-reference``
 *      registers a Dataset row whose train/eval parquets + manifest.json
 *      already live in the artifact store (lab dump, salvage replay,
 *      cross-environment import). No job is enqueued; artefacts are not
 *      re-read or copied.
 *
 * The dialog is intentionally self-contained: it owns its own loading
 * state for embedding configs + ontology snapshots (shared across tabs)
 * and bubbles only ``onCreated(jobId, outputName)`` /
 * ``onImported(datasetId, outputName)`` / ``onClose()`` to the parent.
 */

const DEFAULT_TRAIN_VERSIONS = [160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 210, 215, 220, 226];
const DEFAULT_TEST_VERSIONS = [230];
const K_OPTIONS = [3, 5, 10] as const;

type DialogTab = "dispatch" | "import";

type Props = {
  open: boolean;
  onClose: () => void;
  onCreated?: (jobId: string, outputName: string) => void;
  /** Fired when the Import from reference tab registers a Dataset row. */
  onImported?: (datasetId: string, outputName: string) => void;
};

function parseVersionList(raw: string): number[] | string {
  const parts = raw
    .split(/[\s,]+/)
    .map((p) => p.trim())
    .filter(Boolean);
  if (parts.length === 0) return [];
  const out: number[] = [];
  for (const p of parts) {
    const n = Number(p);
    if (!Number.isInteger(n) || n < 0) return p; // return offending token
    out.push(n);
  }
  return out;
}

export function NewDatasetDialog({ open, onClose, onCreated, onImported }: Props) {
  const t = useTranslations("datasets.dispatch");
  const [embConfigs, setEmbConfigs] = useState<EmbeddingConfig[] | null>(null);
  const [snapshots, setSnapshots] = useState<OntologySnapshot[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<DialogTab>("dispatch");

  // Form state (Dispatch tab)
  const [outputName, setOutputName] = useState("");
  const [embeddingConfigId, setEmbeddingConfigId] = useState("");
  const [ontologySnapshotId, setOntologySnapshotId] = useState("");
  const [k, setK] = useState<number>(5);
  const [trainVersionsRaw, setTrainVersionsRaw] = useState(DEFAULT_TRAIN_VERSIONS.join(", "));
  const [testVersionsRaw, setTestVersionsRaw] = useState(DEFAULT_TEST_VERSIONS.join(", "));
  const [computeAlignments, setComputeAlignments] = useState(true);
  const [computeTaxonomy, setComputeTaxonomy] = useState(true);
  const [usePca, setUsePca] = useState(true);
  const [expandVotesToAncestors, setExpandVotesToAncestors] = useState(true);

  // Form state (Import-from-reference tab). The lab already wrote the
  // parquets + manifest.json out to the artifact store; this tab only
  // collects the URIs + content fingerprints PROTEA needs to register
  // a Dataset row pointing at them. ``embedding_config_id`` /
  // ``ontology_snapshot_id`` reuse the same dropdowns as the dispatch
  // tab so the FK gets resolved when present (NULL'd silently when
  // unresolvable, matching the backend contract).
  const [impName, setImpName] = useState("");
  const [impStorageBackend, setImpStorageBackend] = useState("local");
  const [impKeyPrefix, setImpKeyPrefix] = useState("");
  const [impTrainUri, setImpTrainUri] = useState("");
  const [impEvalUri, setImpEvalUri] = useState("");
  const [impManifestUri, setImpManifestUri] = useState("");
  const [impSchemaSha, setImpSchemaSha] = useState("");
  const [impManifestSha, setImpManifestSha] = useState("");
  const [impK, setImpK] = useState<number>(5);
  const [impAnnotationSource, setImpAnnotationSource] = useState("goa");
  const [impNTrainRows, setImpNTrainRows] = useState("");
  const [impNEvalRows, setImpNEvalRows] = useState("");
  const [impTrainPairsRaw, setImpTrainPairsRaw] = useState("");
  const [impEvalPair, setImpEvalPair] = useState("");
  const [impProducerVersion, setImpProducerVersion] = useState("");
  const [impProducerGitSha, setImpProducerGitSha] = useState("");
  const [impExternalSource, setImpExternalSource] = useState("");
  const [impForce, setImpForce] = useState(false);

  // Lazy-fetch reference data the first time the dialog opens.
  useEffect(() => {
    if (!open || embConfigs !== null) return;
    Promise.all([listEmbeddingConfigs(), listOntologySnapshots()])
      .then(([ec, os]) => {
        setEmbConfigs(ec);
        setSnapshots(os);
        if (ec.length > 0 && !embeddingConfigId) setEmbeddingConfigId(ec[0].id);
        if (os.length > 0 && !ontologySnapshotId) setOntologySnapshotId(os[0].id);
      })
      .catch((e) => setError(String(e)));
    // intentionally only re-fetch on first open
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  // Reset error when the dialog is reopened so a stale failure doesn't
  // greet the user on a fresh attempt.
  useEffect(() => {
    if (open) setError(null);
  }, [open]);

  // Suggest a sensible default ``output_name`` when an embedding config
  // is picked. Recipe pattern: ``bench-v1-K{k}-v{maxTrain}-lineage-{family}``
  // matches the project_multi_plm_v226_sweep_plan dataset naming locked
  // in for the EXP.13 grid.
  const suggestedName = useMemo(() => {
    if (!embeddingConfigId || !embConfigs) return "";
    const cfg = embConfigs.find((e) => e.id === embeddingConfigId);
    if (!cfg) return "";
    const tv = parseVersionList(trainVersionsRaw);
    const lastTrain = Array.isArray(tv) && tv.length > 0 ? Math.max(...tv) : 226;
    // family slug — strip vendor prefix, lowercase, replace separators.
    const slug = cfg.model_name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_|_$/g, "");
    return `bench-v1-K${k}-v${lastTrain}-lineage-${slug}`;
  }, [embeddingConfigId, k, trainVersionsRaw, embConfigs]);

  useEffect(() => {
    if (!outputName && suggestedName) setOutputName(suggestedName);
    // do NOT overwrite a name the user already typed
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [suggestedName]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (!outputName.trim()) { setError(t("errors.nameRequired")); return; }
    if (!embeddingConfigId) { setError(t("errors.embeddingRequired")); return; }
    if (!ontologySnapshotId) { setError(t("errors.snapshotRequired")); return; }

    const trainVersions = parseVersionList(trainVersionsRaw);
    if (typeof trainVersions === "string") {
      setError(t("errors.badVersionToken", { token: trainVersions }));
      return;
    }
    if (trainVersions.length < 2) { setError(t("errors.needTwoTrain")); return; }

    const testVersions = parseVersionList(testVersionsRaw);
    if (typeof testVersions === "string") {
      setError(t("errors.badVersionToken", { token: testVersions }));
      return;
    }
    if (testVersions.length < 1) { setError(t("errors.needOneTest")); return; }

    const payload: CreateDatasetPayload = {
      output_name: outputName.trim(),
      embedding_config_id: embeddingConfigId,
      ontology_snapshot_id: ontologySnapshotId,
      train_versions: trainVersions,
      test_versions: testVersions,
      k,
      search_backend: "faiss",
      annotation_source: "goa",
      compute_alignments: computeAlignments,
      compute_taxonomy: computeTaxonomy,
      use_embedding_pca: usePca,
      expand_votes_to_ancestors: expandVotesToAncestors,
    };

    setLoading(true);
    try {
      const res = await createDataset(payload);
      onCreated?.(res.job_id, outputName.trim());
      onClose();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  async function handleImport(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (!impName.trim()) { setError(t("errors.nameRequired")); return; }
    if (!impKeyPrefix.trim()) { setError(t("import.errors.keyPrefixRequired")); return; }
    if (!impManifestUri.trim()) { setError(t("import.errors.manifestUriRequired")); return; }
    if (!impSchemaSha.trim()) { setError(t("import.errors.schemaShaRequired")); return; }
    if (impK <= 0) { setError(t("import.errors.kPositive")); return; }

    // Train-pair tokens are free-form ``v{old}-v{new}`` strings; we only
    // split on whitespace/commas and trust the backend's regex check.
    const trainPairs = impTrainPairsRaw
      .split(/[\s,]+/)
      .map((p) => p.trim())
      .filter(Boolean);

    const parseRowCount = (raw: string): number | null => {
      if (!raw.trim()) return null;
      const n = Number(raw);
      if (!Number.isInteger(n) || n < 0) return null;
      return n;
    };
    const nTrainParsed = parseRowCount(impNTrainRows);
    const nEvalParsed = parseRowCount(impNEvalRows);
    if (impNTrainRows.trim() && nTrainParsed === null) {
      setError(t("import.errors.rowCountInvalid", { token: impNTrainRows.trim() }));
      return;
    }
    if (impNEvalRows.trim() && nEvalParsed === null) {
      setError(t("import.errors.rowCountInvalid", { token: impNEvalRows.trim() }));
      return;
    }

    const payload: ImportDatasetByReferencePayload = {
      name: impName.trim(),
      storage_backend: impStorageBackend.trim() || "local",
      key_prefix: impKeyPrefix.trim(),
      train_uri: impTrainUri.trim() || null,
      eval_uri: impEvalUri.trim() || null,
      manifest_uri: impManifestUri.trim(),
      schema_sha: impSchemaSha.trim(),
      manifest_sha: impManifestSha.trim() || null,
      k: impK,
      annotation_source: impAnnotationSource.trim() || "goa",
      n_train_rows: nTrainParsed ?? 0,
      n_eval_rows: nEvalParsed ?? 0,
      embedding_config_id: embeddingConfigId || null,
      ontology_snapshot_id: ontologySnapshotId || null,
      train_snapshot_pairs: trainPairs,
      eval_snapshot_pair: impEvalPair.trim() || null,
      producer_version: impProducerVersion.trim() || null,
      producer_git_sha: impProducerGitSha.trim() || null,
      external_source: impExternalSource.trim() || null,
      force: impForce,
    };

    setLoading(true);
    try {
      const res = await importDatasetByReference(payload);
      onImported?.(res.id, res.name);
      onClose();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  if (!open) return null;

  const inputClass =
    "w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
  const labelClass = "block text-xs font-medium uppercase tracking-wider text-slate-500 mb-1";

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="new-dataset-title"
        className="w-full max-w-2xl rounded-xl border bg-white shadow-xl flex flex-col max-h-[92vh]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b px-5 py-4">
          <div>
            <h2 id="new-dataset-title" className="text-base font-semibold text-slate-900">
              {tab === "dispatch" ? t("title") : t("import.title")}
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              {tab === "dispatch" ? t("subtitle") : t("import.subtitle")}
            </p>
          </div>
          <button
            onClick={onClose}
            aria-label={t("close")}
            className="text-slate-500 hover:text-slate-900 text-xl leading-none"
          >
            ×
          </button>
        </div>

        {/* Tab strip. The two paths register a Dataset row; the dispatch
            tab enqueues an export job, the import tab registers an
            already-staged artefact set without re-reading anything. */}
        <div
          role="tablist"
          aria-label={t("tabs.ariaLabel")}
          className="flex items-stretch border-b border-slate-200 bg-slate-50/60 px-2"
        >
          {(["dispatch", "import"] as DialogTab[]).map((id) => {
            const selected = tab === id;
            return (
              <button
                key={id}
                type="button"
                role="tab"
                aria-selected={selected}
                aria-controls={`dataset-tab-${id}`}
                onClick={() => { setTab(id); setError(null); }}
                className={`relative -mb-px px-4 py-2.5 text-sm font-medium transition-colors ${
                  selected
                    ? "text-blue-700 border-b-2 border-blue-600 bg-white"
                    : "text-slate-600 hover:text-slate-900"
                }`}
              >
                {t(`tabs.${id}`)}
              </button>
            );
          })}
        </div>

        {tab === "dispatch" && (
        <form
          id="dataset-tab-dispatch"
          role="tabpanel"
          aria-labelledby="dataset-tab-dispatch"
          onSubmit={handleSubmit}
          className="flex-1 overflow-y-auto p-5 space-y-5"
        >
          {/* Output name */}
          <div>
            <label className={labelClass} htmlFor="ds-name">
              {t("nameLabel")}<span className="text-red-500 ml-0.5">*</span>
            </label>
            <input
              id="ds-name"
              type="text"
              value={outputName}
              onChange={(e) => setOutputName(e.target.value)}
              placeholder={suggestedName || "bench-v1-K5-v226-lineage-…"}
              required
              className={`${inputClass} font-mono text-[13px]`}
            />
            {suggestedName && outputName !== suggestedName && (
              <button
                type="button"
                onClick={() => setOutputName(suggestedName)}
                className="mt-1 text-[11px] text-blue-600 hover:underline"
              >
                {t("useSuggested", { name: suggestedName })}
              </button>
            )}
          </div>

          {/* Embedding config + snapshot */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className={labelClass} htmlFor="ds-emb">
                {t("embeddingLabel")}<span className="text-red-500 ml-0.5">*</span>
              </label>
              <select
                id="ds-emb"
                value={embeddingConfigId}
                onChange={(e) => setEmbeddingConfigId(e.target.value)}
                required
                className={inputClass}
              >
                {embConfigs === null && <option value="">{t("loading")}</option>}
                {embConfigs?.length === 0 && <option value="">{t("noEmbeddings")}</option>}
                {embConfigs?.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.model_name} · {c.pooling}/{c.layer_agg} · L{c.layer_indices.join(",")}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className={labelClass} htmlFor="ds-snap">
                {t("snapshotLabel")}<span className="text-red-500 ml-0.5">*</span>
              </label>
              <select
                id="ds-snap"
                value={ontologySnapshotId}
                onChange={(e) => setOntologySnapshotId(e.target.value)}
                required
                className={inputClass}
              >
                {snapshots === null && <option value="">{t("loading")}</option>}
                {snapshots?.length === 0 && <option value="">{t("noSnapshots")}</option>}
                {snapshots?.map((s) => (
                  <option key={s.id} value={s.id}>
                    {s.obo_version}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {/* K + search backend (faiss only, displayed as read-only context) */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className={labelClass}>
                {t("kLabel")}
                <HelpDot text={t("kHelp")} />
              </label>
              <div role="group" aria-label="K" className="inline-flex rounded-lg bg-slate-100 p-0.5">
                {K_OPTIONS.map((n) => (
                  <button
                    type="button"
                    key={n}
                    onClick={() => setK(n)}
                    aria-pressed={k === n}
                    className={`px-3 py-1.5 text-xs font-semibold rounded-md transition-colors ${
                      k === n ? "bg-white text-slate-900 shadow-sm" : "text-slate-500 hover:text-slate-700"
                    }`}
                  >
                    K={n}
                  </button>
                ))}
              </div>
            </div>
            <div>
              <label className={labelClass}>
                {t("backendLabel")}
                <HelpDot text={t("backendHelp")} />
              </label>
              <div className="inline-flex items-center gap-2 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-xs font-mono text-slate-600">
                faiss
              </div>
            </div>
          </div>

          {/* Version windows */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className={labelClass} htmlFor="ds-train">
                {t("trainVersionsLabel")}
                <HelpDot text={t("trainVersionsHelp")} />
              </label>
              <input
                id="ds-train"
                type="text"
                value={trainVersionsRaw}
                onChange={(e) => setTrainVersionsRaw(e.target.value)}
                placeholder="160, 165, 170, 175, …"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
            <div>
              <label className={labelClass} htmlFor="ds-test">
                {t("testVersionsLabel")}
                <HelpDot text={t("testVersionsHelp")} />
              </label>
              <input
                id="ds-test"
                type="text"
                value={testVersionsRaw}
                onChange={(e) => setTestVersionsRaw(e.target.value)}
                placeholder="230"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
          </div>

          {/* Feature toggles */}
          <fieldset className="space-y-2 rounded-lg border border-slate-200 bg-slate-50/60 p-3">
            <legend className="px-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              {t("featuresLegend")}
            </legend>
            <Toggle
              checked={computeAlignments}
              onChange={setComputeAlignments}
              label={t("computeAlignments")}
              help={t("computeAlignmentsHelp")}
            />
            <Toggle
              checked={computeTaxonomy}
              onChange={setComputeTaxonomy}
              label={t("computeTaxonomy")}
              help={t("computeTaxonomyHelp")}
            />
            <Toggle
              checked={usePca}
              onChange={setUsePca}
              label={t("usePca")}
              help={t("usePcaHelp")}
            />
            <Toggle
              checked={expandVotesToAncestors}
              onChange={setExpandVotesToAncestors}
              label={t("expandVotes")}
              help={t("expandVotesHelp")}
            />
          </fieldset>

          {error && (
            <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
              {error}
            </pre>
          )}

          <div className="flex justify-end gap-2 border-t border-slate-100 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="rounded-md border border-slate-300 px-4 py-2 text-sm hover:bg-slate-50"
            >
              {t("cancel")}
            </button>
            <button
              type="submit"
              disabled={loading}
              className="rounded-md bg-blue-700 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-800 disabled:opacity-50"
            >
              {loading ? t("dispatching") : t("dispatch")}
            </button>
          </div>
        </form>
        )}

        {tab === "import" && (
        <form
          id="dataset-tab-import"
          role="tabpanel"
          aria-labelledby="dataset-tab-import"
          onSubmit={handleImport}
          className="flex-1 overflow-y-auto p-5 space-y-5"
        >
          <p className="text-xs text-slate-500 leading-relaxed">
            {t("import.intro")}
          </p>

          {/* Name + storage backend */}
          <div className="grid grid-cols-1 sm:grid-cols-[2fr_1fr] gap-4">
            <div>
              <label className={labelClass} htmlFor="imp-name">
                {t("nameLabel")}<span className="text-red-500 ml-0.5">*</span>
              </label>
              <input
                id="imp-name"
                type="text"
                value={impName}
                onChange={(e) => setImpName(e.target.value)}
                placeholder="bench-v1-K5-v226-lineage-prostt5"
                required
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-backend">
                {t("import.storageBackendLabel")}
                <HelpDot text={t("import.storageBackendHelp")} />
              </label>
              <select
                id="imp-backend"
                value={impStorageBackend}
                onChange={(e) => setImpStorageBackend(e.target.value)}
                className={inputClass}
              >
                <option value="local">local</option>
                <option value="minio">minio</option>
              </select>
            </div>
          </div>

          {/* Key prefix + manifest URI */}
          <div>
            <label className={labelClass} htmlFor="imp-prefix">
              {t("import.keyPrefixLabel")}<span className="text-red-500 ml-0.5">*</span>
              <HelpDot text={t("import.keyPrefixHelp")} />
            </label>
            <input
              id="imp-prefix"
              type="text"
              value={impKeyPrefix}
              onChange={(e) => setImpKeyPrefix(e.target.value)}
              placeholder="datasets/bench-v1-K5-v226-lineage-prostt5/"
              required
              className={`${inputClass} font-mono text-[13px]`}
            />
          </div>

          <div>
            <label className={labelClass} htmlFor="imp-manifest">
              {t("import.manifestUriLabel")}<span className="text-red-500 ml-0.5">*</span>
              <HelpDot text={t("import.manifestUriHelp")} />
            </label>
            <input
              id="imp-manifest"
              type="text"
              value={impManifestUri}
              onChange={(e) => setImpManifestUri(e.target.value)}
              placeholder="file:///…/manifest.json or s3://bucket/…"
              required
              className={`${inputClass} font-mono text-[13px]`}
            />
          </div>

          {/* Train + eval URIs */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className={labelClass} htmlFor="imp-train-uri">
                {t("import.trainUriLabel")}
                <HelpDot text={t("import.trainUriHelp")} />
              </label>
              <input
                id="imp-train-uri"
                type="text"
                value={impTrainUri}
                onChange={(e) => setImpTrainUri(e.target.value)}
                placeholder="file:///…/train.parquet"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-eval-uri">
                {t("import.evalUriLabel")}
                <HelpDot text={t("import.evalUriHelp")} />
              </label>
              <input
                id="imp-eval-uri"
                type="text"
                value={impEvalUri}
                onChange={(e) => setImpEvalUri(e.target.value)}
                placeholder="file:///…/eval.parquet"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
          </div>

          {/* Content fingerprints (load-bearing at inference) */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className={labelClass} htmlFor="imp-schema-sha">
                {t("import.schemaShaLabel")}<span className="text-red-500 ml-0.5">*</span>
                <HelpDot text={t("import.schemaShaHelp")} />
              </label>
              <input
                id="imp-schema-sha"
                type="text"
                value={impSchemaSha}
                onChange={(e) => setImpSchemaSha(e.target.value)}
                placeholder="6d97a624b8a7"
                required
                maxLength={16}
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-manifest-sha">
                {t("import.manifestShaLabel")}
                <HelpDot text={t("import.manifestShaHelp")} />
              </label>
              <input
                id="imp-manifest-sha"
                type="text"
                value={impManifestSha}
                onChange={(e) => setImpManifestSha(e.target.value)}
                placeholder="sha256 hex (optional for legacy dumps)"
                maxLength={64}
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
          </div>

          {/* K + annotation source + row counts */}
          <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
            <div>
              <label className={labelClass} htmlFor="imp-k">
                {t("kLabel")}<span className="text-red-500 ml-0.5">*</span>
              </label>
              <div role="group" aria-label="K" className="inline-flex rounded-lg bg-slate-100 p-0.5">
                {K_OPTIONS.map((n) => (
                  <button
                    type="button"
                    key={n}
                    onClick={() => setImpK(n)}
                    aria-pressed={impK === n}
                    className={`px-3 py-1.5 text-xs font-semibold rounded-md transition-colors ${
                      impK === n ? "bg-white text-slate-900 shadow-sm" : "text-slate-500 hover:text-slate-700"
                    }`}
                  >
                    K={n}
                  </button>
                ))}
              </div>
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-src">
                {t("import.annotationSourceLabel")}
              </label>
              <select
                id="imp-src"
                value={impAnnotationSource}
                onChange={(e) => setImpAnnotationSource(e.target.value)}
                className={inputClass}
              >
                <option value="goa">goa</option>
                <option value="quickgo">quickgo</option>
              </select>
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-n-train">
                {t("import.nTrainRowsLabel")}
              </label>
              <input
                id="imp-n-train"
                type="text"
                inputMode="numeric"
                value={impNTrainRows}
                onChange={(e) => setImpNTrainRows(e.target.value)}
                placeholder="24351779"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-n-eval">
                {t("import.nEvalRowsLabel")}
              </label>
              <input
                id="imp-n-eval"
                type="text"
                inputMode="numeric"
                value={impNEvalRows}
                onChange={(e) => setImpNEvalRows(e.target.value)}
                placeholder="1066859"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
          </div>

          {/* FK overlays (shared dropdowns) */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className={labelClass} htmlFor="imp-emb">
                {t("embeddingLabel")}
                <HelpDot text={t("import.embeddingHelp")} />
              </label>
              <select
                id="imp-emb"
                value={embeddingConfigId}
                onChange={(e) => setEmbeddingConfigId(e.target.value)}
                className={inputClass}
              >
                <option value="">{t("import.embeddingNotResolved")}</option>
                {embConfigs?.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.model_name} · {c.pooling}/{c.layer_agg} · L{c.layer_indices.join(",")}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-snap">
                {t("snapshotLabel")}
                <HelpDot text={t("import.snapshotHelp")} />
              </label>
              <select
                id="imp-snap"
                value={ontologySnapshotId}
                onChange={(e) => setOntologySnapshotId(e.target.value)}
                className={inputClass}
              >
                <option value="">{t("import.snapshotNotResolved")}</option>
                {snapshots?.map((s) => (
                  <option key={s.id} value={s.id}>
                    {s.obo_version}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {/* Snapshot pair provenance */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className={labelClass} htmlFor="imp-train-pairs">
                {t("import.trainPairsLabel")}
                <HelpDot text={t("import.trainPairsHelp")} />
              </label>
              <input
                id="imp-train-pairs"
                type="text"
                value={impTrainPairsRaw}
                onChange={(e) => setImpTrainPairsRaw(e.target.value)}
                placeholder="v220-v226, v215-v220"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-eval-pair">
                {t("import.evalPairLabel")}
                <HelpDot text={t("import.evalPairHelp")} />
              </label>
              <input
                id="imp-eval-pair"
                type="text"
                value={impEvalPair}
                onChange={(e) => setImpEvalPair(e.target.value)}
                placeholder="v226-v230"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
          </div>

          {/* Producer provenance */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <div>
              <label className={labelClass} htmlFor="imp-prod-ver">
                {t("import.producerVersionLabel")}
              </label>
              <input
                id="imp-prod-ver"
                type="text"
                value={impProducerVersion}
                onChange={(e) => setImpProducerVersion(e.target.value)}
                placeholder="0.8.0"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-prod-sha">
                {t("import.producerGitShaLabel")}
              </label>
              <input
                id="imp-prod-sha"
                type="text"
                value={impProducerGitSha}
                onChange={(e) => setImpProducerGitSha(e.target.value)}
                placeholder="059db19…"
                maxLength={40}
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
            <div>
              <label className={labelClass} htmlFor="imp-ext-src">
                {t("import.externalSourceLabel")}
                <HelpDot text={t("import.externalSourceHelp")} />
              </label>
              <input
                id="imp-ext-src"
                type="text"
                value={impExternalSource}
                onChange={(e) => setImpExternalSource(e.target.value)}
                placeholder="protea-reranker-lab@059db19"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>
          </div>

          {/* Force overwrite */}
          <label className="flex items-start gap-2 text-sm cursor-pointer select-none rounded-md border border-amber-200 bg-amber-50/60 p-3">
            <input
              type="checkbox"
              checked={impForce}
              onChange={(e) => setImpForce(e.target.checked)}
              className="mt-0.5"
            />
            <span className="flex-1">
              <span className="font-medium text-amber-900">{t("import.forceLabel")}</span>
              <span className="block text-xs text-amber-700 leading-snug">{t("import.forceHelp")}</span>
            </span>
          </label>

          {error && (
            <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
              {error}
            </pre>
          )}

          <div className="flex justify-end gap-2 border-t border-slate-100 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="rounded-md border border-slate-300 px-4 py-2 text-sm hover:bg-slate-50"
            >
              {t("cancel")}
            </button>
            <button
              type="submit"
              disabled={loading}
              className="rounded-md bg-emerald-700 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-800 disabled:opacity-50"
            >
              {loading ? t("import.importing") : t("import.importButton")}
            </button>
          </div>
        </form>
        )}
      </div>
    </div>
  );
}

function Toggle({
  checked,
  onChange,
  label,
  help,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  label: string;
  help: string;
}) {
  return (
    <label className="flex items-start gap-2 text-sm cursor-pointer select-none">
      <input
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        className="mt-0.5"
      />
      <span className="flex-1">
        <span className="font-medium text-slate-800">{label}</span>
        <span className="block text-xs text-slate-500 leading-snug">{help}</span>
      </span>
    </label>
  );
}
