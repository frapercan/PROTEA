"use client";

import { useEffect, useMemo, useState } from "react";
import { useTranslations } from "next-intl";
import {
  createDataset,
  listEmbeddingConfigs,
  listOntologySnapshots,
  type CreateDatasetPayload,
  type EmbeddingConfig,
  type OntologySnapshot,
} from "@/lib/api";
import { HelpDot } from "@/components/Tooltip";

/**
 * Modal form that dispatches a new ``export_research_dataset`` job via
 * ``POST /v1/datasets``. Defaults mirror the canonical multi-PLM v226
 * sweep recipe (project_multi_plm_v226_sweep_plan +
 * project_pca_transductive_decision): test_versions=[230],
 * train_versions=[160..226 step 5], faiss + all toggles on, K=5.
 *
 * Operators can edit any field before submit. The dialog is intentionally
 * self-contained: it owns its own loading state for embedding configs +
 * ontology snapshots and bubbles only ``onCreated(jobId, outputName)`` /
 * ``onClose()`` to the parent.
 */

const DEFAULT_TRAIN_VERSIONS = [160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 210, 215, 220, 226];
const DEFAULT_TEST_VERSIONS = [230];
const K_OPTIONS = [3, 5, 10] as const;

type Props = {
  open: boolean;
  onClose: () => void;
  onCreated?: (jobId: string, outputName: string) => void;
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

export function NewDatasetDialog({ open, onClose, onCreated }: Props) {
  const t = useTranslations("datasets.dispatch");
  const [embConfigs, setEmbConfigs] = useState<EmbeddingConfig[] | null>(null);
  const [snapshots, setSnapshots] = useState<OntologySnapshot[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Form state
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
              {t("title")}
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">{t("subtitle")}</p>
          </div>
          <button
            onClick={onClose}
            aria-label={t("close")}
            className="text-slate-500 hover:text-slate-900 text-xl leading-none"
          >
            ×
          </button>
        </div>

        <form onSubmit={handleSubmit} className="flex-1 overflow-y-auto p-5 space-y-5">
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
