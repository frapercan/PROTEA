"use client";

import { useEffect, useState } from "react";
import { useTranslations } from "next-intl";
import {
  createJob,
  listEmbeddingConfigs,
  listQuerySets,
  type EmbeddingConfig,
  type QuerySet,
} from "@/lib/api";

/**
 * Quick-action dialog that dispatches a ``compute_embeddings`` job onto
 * the ``protea.embeddings`` queue. The dialog mirrors the inline form
 * on ``/embeddings`` (Compute tab) but is launched from a header CTA
 * so operators can re-queue a run without navigating tabs and losing
 * the configs-table scroll position.
 *
 * Defaults: select the first available embedding config, all sequences
 * (no query-set filter), queue batch 100, model batch 1, ``cuda``,
 * ``skip_existing=true``. These match the page's defaults so the dialog
 * is a strict superset of the page form (parent can preselect an
 * embedding-config id via ``initialConfigId``).
 */

type Props = {
  open: boolean;
  onClose: () => void;
  onLaunched?: (job: { id: string; status: string }) => void;
  initialConfigId?: string | null;
};

export function ComputeEmbeddingsDialog({
  open,
  onClose,
  onLaunched,
  initialConfigId,
}: Props) {
  const t = useTranslations("embeddings.computeDialog");
  const [configs, setConfigs] = useState<EmbeddingConfig[] | null>(null);
  const [querySets, setQuerySets] = useState<QuerySet[] | null>(null);

  const [configId, setConfigId] = useState(initialConfigId ?? "");
  const [querySetId, setQuerySetId] = useState("");
  const [queueBatchSize, setQueueBatchSize] = useState(100);
  const [batchSize, setBatchSize] = useState(1);
  const [device, setDevice] = useState("cuda");
  const [skipExisting, setSkipExisting] = useState(true);

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setError(null);
    setSubmitting(false);
    // Lazy-load reference data only on first open
    if (configs === null) {
      Promise.all([listEmbeddingConfigs(), listQuerySets()])
        .then(([cfgs, qsets]) => {
          setConfigs(cfgs);
          setQuerySets(qsets);
          if (!configId && cfgs.length > 0) setConfigId(initialConfigId ?? cfgs[0].id);
        })
        .catch((e) => setError(String(e)));
    } else if (initialConfigId && initialConfigId !== configId) {
      setConfigId(initialConfigId);
    }
    // intentionally only fire on open / initialConfigId change
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, initialConfigId]);

  if (!open) return null;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    const effectiveConfigId = configId || configs?.[0]?.id || "";
    if (!effectiveConfigId) {
      setError(t("errors.configRequired"));
      return;
    }

    setSubmitting(true);
    try {
      const result = await createJob({
        operation: "compute_embeddings",
        queue_name: "protea.embeddings",
        payload: {
          embedding_config_id: effectiveConfigId,
          sequences_per_job: queueBatchSize,
          batch_size: batchSize,
          device,
          skip_existing: skipExisting,
          ...(querySetId ? { query_set_id: querySetId } : {}),
        },
      });
      onLaunched?.(result);
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
  const devicePresets = ["cpu", "cuda", "cuda:0", "cuda:1"] as const;
  const deviceIsCustom = !devicePresets.includes(device as (typeof devicePresets)[number]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="compute-embeddings-title"
        className="w-full max-w-xl rounded-xl border bg-white shadow-xl flex flex-col max-h-[92vh]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b px-5 py-4">
          <div>
            <h2 id="compute-embeddings-title" className="text-base font-semibold text-slate-900">
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

        <form onSubmit={handleSubmit} className="flex-1 overflow-y-auto p-5 space-y-4">
          {/* Config */}
          <div>
            <label className={labelClass} htmlFor="ce-config">
              {t("configLabel")}<span className="text-red-500 ml-0.5">*</span>
            </label>
            <select
              id="ce-config"
              value={configId}
              onChange={(e) => setConfigId(e.target.value)}
              required
              className={inputClass}
            >
              {configs === null && <option value="">{t("loading")}</option>}
              {configs && configs.length === 0 && <option value="">{t("noConfigs")}</option>}
              {configs?.map((c) => (
                <option key={c.id} value={c.id}>
                  {(c.description || c.model_name)} ({c.id.slice(0, 8)})
                </option>
              ))}
            </select>
          </div>

          {/* Query set */}
          <div>
            <label className={labelClass} htmlFor="ce-qset">
              {t("querySetLabel")}{" "}
              <span className="font-normal normal-case text-slate-500 lowercase">
                {t("querySetHelper")}
              </span>
            </label>
            <select
              id="ce-qset"
              value={querySetId}
              onChange={(e) => setQuerySetId(e.target.value)}
              className={inputClass}
            >
              <option value="">{t("allSequences")}</option>
              {querySets?.map((qs) => (
                <option key={qs.id} value={qs.id}>
                  {qs.name} ({qs.entry_count} seqs)
                </option>
              ))}
            </select>
          </div>

          {/* Batch sizes */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div>
              <label className={labelClass} htmlFor="ce-qbatch">
                {t("queueBatchSizeLabel")}{" "}
                <span className="font-normal normal-case text-slate-500 lowercase">
                  {t("queueBatchSizeHelper")}
                </span>
              </label>
              <input
                id="ce-qbatch"
                type="number"
                value={queueBatchSize}
                onChange={(e) => {
                  const v = parseInt(e.target.value, 10);
                  if (!isNaN(v)) setQueueBatchSize(v);
                }}
                min={1}
                className={inputClass}
              />
            </div>
            <div>
              <label className={labelClass} htmlFor="ce-mbatch">
                {t("modelBatchSizeLabel")}{" "}
                <span className="font-normal normal-case text-slate-500 lowercase">
                  {t("modelBatchSizeHelper")}
                </span>
              </label>
              <input
                id="ce-mbatch"
                type="number"
                value={batchSize}
                onChange={(e) => {
                  const v = parseInt(e.target.value, 10);
                  if (!isNaN(v)) setBatchSize(v);
                }}
                min={1}
                className={inputClass}
              />
            </div>
          </div>

          {/* Device */}
          <div>
            <label className={labelClass} htmlFor="ce-device">
              {t("deviceLabel")}
            </label>
            <select
              id="ce-device"
              value={deviceIsCustom ? "custom" : device}
              onChange={(e) => {
                if (e.target.value !== "custom") setDevice(e.target.value);
              }}
              className={inputClass}
            >
              <option value="cpu">{t("deviceCpu")}</option>
              <option value="cuda">{t("deviceCuda")}</option>
              <option value="cuda:0">{t("deviceCuda0")}</option>
              <option value="cuda:1">{t("deviceCuda1")}</option>
              <option value="custom">{t("deviceCustom")}</option>
            </select>
            {deviceIsCustom && (
              <input
                type="text"
                value={device}
                onChange={(e) => setDevice(e.target.value)}
                placeholder="e.g. cuda:2"
                className={`${inputClass} mt-1 font-mono text-[13px]`}
              />
            )}
          </div>

          <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
            <input
              type="checkbox"
              checked={skipExisting}
              onChange={(e) => setSkipExisting(e.target.checked)}
              className="rounded"
            />
            <span className="text-slate-700">{t("skipExisting")}</span>
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
              disabled={submitting || (configs !== null && configs.length === 0)}
              className="rounded-md bg-blue-700 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-800 disabled:opacity-50"
            >
              {submitting ? t("launching") : t("launch")}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
