"use client";

import { useEffect, useRef, useState } from "react";
import { useTranslations } from "next-intl";
import {
  importRerankerMultipart,
  listDatasets,
  type Dataset,
  type RerankerImportOverrides,
  type RerankerImportResponse,
} from "@/lib/api";

/**
 * Multipart upload form for ``POST /reranker-models/import``.
 *
 * The lab produces three artefacts under ``runs/<run_id>/``:
 * ``model.txt`` (LightGBM booster), ``spec.yaml`` (ExperimentSpec) and
 * ``run.json`` (metrics + provenance). All three are required; the
 * server uploads ``model.txt`` to the configured artifact store and
 * inserts a ``RerankerModel`` row whose provenance is derived from
 * ``run.json`` + ``spec.yaml``.
 *
 * Operators may override the auto-derived row name and optionally bind
 * the booster to a local ``Dataset`` / ``PredictionSet`` /
 * ``EvaluationSet`` UUID. The "force" toggle replaces an existing
 * row with the same name (server returns 409 otherwise).
 */

type Props = {
  open: boolean;
  onClose: () => void;
  onImported?: (model: RerankerImportResponse) => void;
};

export function ImportRerankerDialog({ open, onClose, onImported }: Props) {
  const t = useTranslations("reranker.importDialog");
  const [datasets, setDatasets] = useState<Dataset[] | null>(null);

  // File inputs (controlled via refs so reopening the dialog clears them)
  const modelRef = useRef<HTMLInputElement>(null);
  const specRef = useRef<HTMLInputElement>(null);
  const runRef = useRef<HTMLInputElement>(null);

  // Field state
  const [name, setName] = useState("");
  const [datasetId, setDatasetId] = useState("");
  const [externalSource, setExternalSource] = useState("");
  const [predictionSetId, setPredictionSetId] = useState("");
  const [evaluationSetId, setEvaluationSetId] = useState("");
  const [force, setForce] = useState(false);

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Reset on open and lazy-load datasets the first time
  useEffect(() => {
    if (!open) return;
    setError(null);
    setSubmitting(false);
    if (datasets === null) {
      listDatasets({ limit: 200 })
        .then(setDatasets)
        .catch(() => setDatasets([])); // failure → manual UUID entry still works
    }
    // intentionally only on open
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  if (!open) return null;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    const modelFile = modelRef.current?.files?.[0] ?? null;
    const specYaml = specRef.current?.files?.[0] ?? null;
    const runJson = runRef.current?.files?.[0] ?? null;

    if (!modelFile || !specYaml || !runJson) {
      setError(t("errors.filesRequired"));
      return;
    }

    const overrides: RerankerImportOverrides = {
      name: name.trim() || null,
      dataset_id: datasetId.trim() || null,
      external_source: externalSource.trim() || null,
      prediction_set_id: predictionSetId.trim() || null,
      evaluation_set_id: evaluationSetId.trim() || null,
      force,
    };

    setSubmitting(true);
    try {
      const res = await importRerankerMultipart(
        { modelFile, specYaml, runJson },
        overrides,
      );
      onImported?.(res);
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
  const labelClass = "block text-xs font-medium uppercase tracking-wider text-slate-500 mb-1";

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="import-reranker-title"
        className="w-full max-w-2xl rounded-xl border bg-white shadow-xl flex flex-col max-h-[92vh]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b px-5 py-4">
          <div>
            <h2 id="import-reranker-title" className="text-base font-semibold text-slate-900">
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
          {/* File uploads */}
          <fieldset className="space-y-3 rounded-lg border border-slate-200 bg-slate-50/60 p-3">
            <legend className="px-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              {t("filesLegend")}
            </legend>
            <p className="text-xs text-slate-600 leading-snug">{t("filesHelp")}</p>

            <div>
              <label className={labelClass} htmlFor="ir-model-file">
                {t("modelFileLabel")}<span className="text-red-500 ml-0.5">*</span>
              </label>
              <input
                id="ir-model-file"
                ref={modelRef}
                type="file"
                accept=".txt,.model,application/octet-stream,text/plain"
                required
                className="block w-full text-sm text-slate-700 file:mr-3 file:rounded-md file:border-0 file:bg-blue-50 file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-blue-700 hover:file:bg-blue-100"
              />
              <p className="mt-1 text-[11px] text-slate-500">{t("modelFileHelp")}</p>
            </div>

            <div>
              <label className={labelClass} htmlFor="ir-spec-file">
                {t("specFileLabel")}<span className="text-red-500 ml-0.5">*</span>
              </label>
              <input
                id="ir-spec-file"
                ref={specRef}
                type="file"
                accept=".yaml,.yml,application/x-yaml,text/yaml,text/plain"
                required
                className="block w-full text-sm text-slate-700 file:mr-3 file:rounded-md file:border-0 file:bg-blue-50 file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-blue-700 hover:file:bg-blue-100"
              />
              <p className="mt-1 text-[11px] text-slate-500">{t("specFileHelp")}</p>
            </div>

            <div>
              <label className={labelClass} htmlFor="ir-run-file">
                {t("runFileLabel")}<span className="text-red-500 ml-0.5">*</span>
              </label>
              <input
                id="ir-run-file"
                ref={runRef}
                type="file"
                accept=".json,application/json"
                required
                className="block w-full text-sm text-slate-700 file:mr-3 file:rounded-md file:border-0 file:bg-blue-50 file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-blue-700 hover:file:bg-blue-100"
              />
              <p className="mt-1 text-[11px] text-slate-500">{t("runFileHelp")}</p>
            </div>
          </fieldset>

          {/* Name override + force */}
          <div className="grid grid-cols-1 sm:grid-cols-[1fr_auto] gap-4 items-end">
            <div>
              <label className={labelClass} htmlFor="ir-name">
                {t("nameLabel")}
              </label>
              <input
                id="ir-name"
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
            <p className="text-xs text-slate-600 leading-snug">{t("provenanceHelp")}</p>

            <div>
              <label className={labelClass} htmlFor="ir-dataset">
                {t("datasetLabel")}
              </label>
              {datasets && datasets.length > 0 ? (
                <select
                  id="ir-dataset"
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
                  id="ir-dataset"
                  type="text"
                  value={datasetId}
                  onChange={(e) => setDatasetId(e.target.value)}
                  placeholder={t("datasetPlaceholder")}
                  className={`${inputClass} font-mono text-[13px]`}
                />
              )}
              <p className="mt-1 text-[11px] text-slate-500">{t("datasetHelp")}</p>
            </div>

            <div>
              <label className={labelClass} htmlFor="ir-external">
                {t("externalSourceLabel")}
              </label>
              <input
                id="ir-external"
                type="text"
                value={externalSource}
                onChange={(e) => setExternalSource(e.target.value)}
                placeholder="protea-reranker-lab@<git-sha>"
                className={`${inputClass} font-mono text-[13px]`}
              />
              <p className="mt-1 text-[11px] text-slate-500">{t("externalSourceHelp")}</p>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <div>
                <label className={labelClass} htmlFor="ir-pred">
                  {t("predictionSetLabel")}
                </label>
                <input
                  id="ir-pred"
                  type="text"
                  value={predictionSetId}
                  onChange={(e) => setPredictionSetId(e.target.value)}
                  placeholder="00000000-0000-…"
                  className={`${inputClass} font-mono text-[12px]`}
                />
              </div>
              <div>
                <label className={labelClass} htmlFor="ir-eval">
                  {t("evaluationSetLabel")}
                </label>
                <input
                  id="ir-eval"
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
              {submitting ? t("importing") : t("import")}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
