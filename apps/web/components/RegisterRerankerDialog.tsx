"use client";

import { useEffect, useState } from "react";
import { useTranslations } from "next-intl";
import {
  importRerankerByReference,
  listDatasets,
  type Dataset,
  type RerankerImportByReferenceBody,
  type RerankerImportResponse,
} from "@/lib/api";

/**
 * Register a booster that is already in the artifact store via
 * ``POST /reranker-models/import-by-reference``. Useful when the lab
 * uploads ``model.txt`` to MinIO directly (faster, no double-hop) and
 * just needs PROTEA to record the URI + provenance.
 *
 * The dialog parses ``run.json`` from a textarea so operators can paste
 * the file contents straight from ``runs/<run_id>/run.json``. The
 * ``spec.yaml`` text is also pasted verbatim (no parsing here:
 * ``_extract_cell_from_spec`` happens server-side).
 */

type Props = {
  open: boolean;
  onClose: () => void;
  onRegistered?: (model: RerankerImportResponse) => void;
};

export function RegisterRerankerDialog({ open, onClose, onRegistered }: Props) {
  const t = useTranslations("reranker.registerDialog");
  const [datasets, setDatasets] = useState<Dataset[] | null>(null);

  const [artifactUri, setArtifactUri] = useState("");
  const [specYaml, setSpecYaml] = useState("");
  const [runJsonText, setRunJsonText] = useState("");
  const [name, setName] = useState("");
  const [datasetId, setDatasetId] = useState("");
  const [externalSource, setExternalSource] = useState("");
  const [predictionSetId, setPredictionSetId] = useState("");
  const [evaluationSetId, setEvaluationSetId] = useState("");
  const [force, setForce] = useState(false);

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setError(null);
    setSubmitting(false);
    if (datasets === null) {
      listDatasets({ limit: 200 })
        .then(setDatasets)
        .catch(() => setDatasets([]));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  if (!open) return null;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (!artifactUri.trim()) {
      setError(t("errors.uriRequired"));
      return;
    }
    if (!specYaml.trim()) {
      setError(t("errors.specRequired"));
      return;
    }

    let run: Record<string, unknown>;
    try {
      run = JSON.parse(runJsonText) as Record<string, unknown>;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(t("errors.runJsonInvalid", { message: msg }));
      return;
    }

    const body: RerankerImportByReferenceBody = {
      artifact_uri: artifactUri.trim(),
      spec_yaml: specYaml,
      run,
      name: name.trim() || null,
      dataset_id: datasetId.trim() || null,
      external_source: externalSource.trim() || null,
      prediction_set_id: predictionSetId.trim() || null,
      evaluation_set_id: evaluationSetId.trim() || null,
      force,
    };

    setSubmitting(true);
    try {
      const res = await importRerankerByReference(body);
      onRegistered?.(res);
      onClose();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setSubmitting(false);
    }
  }

  const inputClass =
    "w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
  const textareaClass = `${inputClass} font-mono text-[12px] leading-relaxed`;
  const labelClass = "block text-xs font-medium uppercase tracking-wider text-slate-500 mb-1";

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="register-reranker-title"
        className="w-full max-w-3xl rounded-xl border bg-white shadow-xl flex flex-col max-h-[92vh]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b px-5 py-4">
          <div>
            <h2 id="register-reranker-title" className="text-base font-semibold text-slate-900">
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
          {/* Artifact URI */}
          <div>
            <label className={labelClass} htmlFor="rr-uri">
              {t("artifactUriLabel")}<span className="text-red-500 ml-0.5">*</span>
            </label>
            <input
              id="rr-uri"
              type="text"
              value={artifactUri}
              onChange={(e) => setArtifactUri(e.target.value)}
              placeholder="s3://protea-rerankers/runs/<run_id>/model.txt"
              required
              className={`${inputClass} font-mono text-[13px]`}
            />
            <p className="mt-1 text-[11px] text-slate-500">{t("artifactUriHelp")}</p>
          </div>

          {/* Spec yaml */}
          <div>
            <label className={labelClass} htmlFor="rr-spec">
              {t("specYamlLabel")}<span className="text-red-500 ml-0.5">*</span>
            </label>
            <textarea
              id="rr-spec"
              value={specYaml}
              onChange={(e) => setSpecYaml(e.target.value)}
              rows={6}
              required
              placeholder={"name: my-run\ntraining:\n  cell: nk-bpo\nfeature_families: [embedding, alignment, taxonomy]\n"}
              className={textareaClass}
            />
            <p className="mt-1 text-[11px] text-slate-500">{t("specYamlHelp")}</p>
          </div>

          {/* Run JSON */}
          <div>
            <label className={labelClass} htmlFor="rr-run">
              {t("runJsonLabel")}<span className="text-red-500 ml-0.5">*</span>
            </label>
            <textarea
              id="rr-run"
              value={runJsonText}
              onChange={(e) => setRunJsonText(e.target.value)}
              rows={8}
              required
              placeholder={'{\n  "run_id": "...",\n  "metrics": {"fmax": 0.7291},\n  "features": {"families_enabled": ["embedding", "alignment"], "feature_count": 52},\n  "dataset": {"name": "bench-v1-K5-v226-lineage-esm2_650m", "schema_sha": "..."}\n}'}
              className={textareaClass}
            />
            <p className="mt-1 text-[11px] text-slate-500">{t("runJsonHelp")}</p>
          </div>

          {/* Name + force */}
          <div className="grid grid-cols-1 sm:grid-cols-[1fr_auto] gap-4 items-end">
            <div>
              <label className={labelClass} htmlFor="rr-name">
                {t("nameLabel")}
              </label>
              <input
                id="rr-name"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder={t("namePlaceholder")}
                className={`${inputClass} font-mono text-[13px]`}
              />
              <p className="mt-1 text-[11px] text-slate-500">{t("nameHelp")}</p>
            </div>
            <label className="flex items-center gap-2 text-sm pb-2 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={force}
                onChange={(e) => setForce(e.target.checked)}
                className="rounded"
              />
              <span className="text-slate-700">{t("forceLabel")}</span>
            </label>
          </div>

          {/* Provenance overrides */}
          <fieldset className="space-y-3 rounded-lg border border-slate-200 p-3">
            <legend className="px-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              {t("provenanceLegend")}
            </legend>

            <div>
              <label className={labelClass} htmlFor="rr-dataset">
                {t("datasetLabel")}
              </label>
              {datasets && datasets.length > 0 ? (
                <select
                  id="rr-dataset"
                  value={datasetId}
                  onChange={(e) => setDatasetId(e.target.value)}
                  className={inputClass}
                >
                  <option value="">{t("datasetAuto")}</option>
                  {datasets.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.name}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  id="rr-dataset"
                  type="text"
                  value={datasetId}
                  onChange={(e) => setDatasetId(e.target.value)}
                  placeholder={t("datasetPlaceholder")}
                  className={`${inputClass} font-mono text-[13px]`}
                />
              )}
            </div>

            <div>
              <label className={labelClass} htmlFor="rr-external">
                {t("externalSourceLabel")}
              </label>
              <input
                id="rr-external"
                type="text"
                value={externalSource}
                onChange={(e) => setExternalSource(e.target.value)}
                placeholder="protea-reranker-lab@<git-sha>"
                className={`${inputClass} font-mono text-[13px]`}
              />
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <div>
                <label className={labelClass} htmlFor="rr-pred">
                  {t("predictionSetLabel")}
                </label>
                <input
                  id="rr-pred"
                  type="text"
                  value={predictionSetId}
                  onChange={(e) => setPredictionSetId(e.target.value)}
                  placeholder="00000000-0000-…"
                  className={`${inputClass} font-mono text-[12px]`}
                />
              </div>
              <div>
                <label className={labelClass} htmlFor="rr-eval">
                  {t("evaluationSetLabel")}
                </label>
                <input
                  id="rr-eval"
                  type="text"
                  value={evaluationSetId}
                  onChange={(e) => setEvaluationSetId(e.target.value)}
                  placeholder="00000000-0000-…"
                  className={`${inputClass} font-mono text-[12px]`}
                />
              </div>
            </div>
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
              disabled={submitting}
              className="rounded-md bg-blue-700 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-800 disabled:opacity-50"
            >
              {submitting ? t("registering") : t("register")}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
