"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useTranslations } from "next-intl";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import {
  listEmbeddingConfigs,
  createEmbeddingConfig,
  deleteEmbeddingConfig,
  createJob,
  listQuerySets,
  EmbeddingConfig,
  QuerySet,
} from "@/lib/api";

type ModelPreset = {
  value: string;
  label: string;
  layers: number;
  defaultMaxLength: number;
};

const MODEL_PRESETS: Record<string, ModelPreset[]> = {
  esm: [
    { value: "facebook/esm2_t6_8M_UR50D",    label: "ESM-2 8M  (6 layers)",   layers: 6,  defaultMaxLength: 1022 },
    { value: "facebook/esm2_t12_35M_UR50D",   label: "ESM-2 35M (12 layers)",  layers: 12, defaultMaxLength: 1022 },
    { value: "facebook/esm2_t30_150M_UR50D",  label: "ESM-2 150M (30 layers)", layers: 30, defaultMaxLength: 1022 },
    { value: "facebook/esm2_t33_650M_UR50D",  label: "ESM-2 650M (33 layers) — recommended", layers: 33, defaultMaxLength: 1022 },
    { value: "facebook/esm2_t36_3B_UR50D",    label: "ESM-2 3B  (36 layers)",  layers: 36, defaultMaxLength: 1022 },
  ],
  esm3c: [
    { value: "esmc_300m", label: "ESMC 300M", layers: 30, defaultMaxLength: 2048 },
    { value: "esmc_600m", label: "ESMC 600M", layers: 48, defaultMaxLength: 2048 },
  ],
  t5: [
    { value: "Rostlab/prot_t5_xl_uniref50",          label: "ProT5-XL (standard, recommended)", layers: 24, defaultMaxLength: 1024 },
    { value: "Rostlab/prot_t5_xl_half_uniref50-enc", label: "ProT5-XL half (FP16 encoder)",     layers: 24, defaultMaxLength: 1024 },
    { value: "Rostlab/ProstT5",                      label: "ProstT5 (3Di + AA)",               layers: 24, defaultMaxLength: 1024 },
  ],
  auto: [
    { value: "facebook/esm2_t33_650M_UR50D", label: "ESM-2 650M (auto backend)", layers: 33, defaultMaxLength: 1022 },
  ],
};

type Tab = "configs" | "compute";

function formatDate(iso?: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
}

function shortId(id: string) {
  return id.slice(0, 8);
}

export default function EmbeddingsPage() {
  const t = useTranslations("embeddings");
  const [activeTab, setActiveTab] = useState<Tab>("configs");
  const toast = useToast();

  // Shared data
  const [configs, setConfigs] = useState<EmbeddingConfig[]>([]);
  const [querySets, setQuerySets] = useState<QuerySet[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // New config form state
  const [showConfigForm, setShowConfigForm] = useState(false);
  const [cfgBackend, setCfgBackend] = useState("esm");
  const [cfgModelPreset, setCfgModelPreset] = useState(MODEL_PRESETS.esm[3].value);
  const [cfgModelCustom, setCfgModelCustom] = useState("");
  const cfgModelName = cfgModelPreset === "__custom__" ? cfgModelCustom : cfgModelPreset;
  const [cfgLayerIndices, setCfgLayerIndices] = useState("0");
  const [cfgLayerAgg, setCfgLayerAgg] = useState("mean");
  const [cfgPooling, setCfgPooling] = useState("mean");
  const [cfgNormalizeResidues, setCfgNormalizeResidues] = useState(false);
  const [cfgNormalize, setCfgNormalize] = useState(true);
  const [cfgMaxLength, setCfgMaxLength] = useState(1022);
  const [cfgUseChunking, setCfgUseChunking] = useState(false);
  const [cfgChunkSize, setCfgChunkSize] = useState(512);
  const [cfgChunkOverlap, setCfgChunkOverlap] = useState(0);
  const [cfgDescription, setCfgDescription] = useState("");
  const [cfgError, setCfgError] = useState("");
  const [cfgSubmitting, setCfgSubmitting] = useState(false);

  // Compute form state
  const [cmpConfigId, setCmpConfigId] = useState("");
  const [cmpQuerySetId, setCmpQuerySetId] = useState("");
  const [cmpQueueBatchSize, setCmpQueueBatchSize] = useState(100);
  const [cmpBatchSize, setCmpBatchSize] = useState(8);
  const [cmpDevice, setCmpDevice] = useState("cuda");
  const [cmpSkipExisting, setCmpSkipExisting] = useState(true);
  const [cmpResult, setCmpResult] = useState<{ id: string; status: string } | null>(null);
  const [cmpError, setCmpError] = useState("");
  const [cmpSubmitting, setCmpSubmitting] = useState(false);

  async function loadAll() {
    setLoading(true);
    setError("");
    try {
      const [cfgs, qsets] = await Promise.all([
        listEmbeddingConfigs(),
        listQuerySets(),
      ]);
      setConfigs(cfgs);
      setQuerySets(qsets);
      if (cfgs.length > 0 && !cmpConfigId) setCmpConfigId(cfgs[0].id);
    } catch (e: any) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadAll();
  }, []);

  async function handleCreateConfig(e: React.FormEvent) {
    e.preventDefault();
    setCfgError("");
    const layerIndices = cfgLayerIndices
      .split(",")
      .map((s) => parseInt(s.trim(), 10))
      .filter((n) => !isNaN(n));
    if (layerIndices.length === 0) {
      setCfgError("Layer indices must be a comma-separated list of integers.");
      return;
    }
    setCfgSubmitting(true);
    try {
      const created = await createEmbeddingConfig({
        model_name: cfgModelName,
        model_backend: cfgBackend,
        layer_indices: layerIndices,
        layer_agg: cfgLayerAgg,
        pooling: cfgPooling,
        normalize_residues: cfgNormalizeResidues,
        normalize: cfgNormalize,
        max_length: cfgMaxLength,
        use_chunking: cfgUseChunking,
        chunk_size: cfgChunkSize,
        chunk_overlap: cfgChunkOverlap,
        description: cfgDescription || null,
      });
      setConfigs((prev) => [created, ...prev]);
      setShowConfigForm(false);
      setCfgModelPreset(MODEL_PRESETS.esm[3].value);
      setCfgModelCustom("");
      setCfgLayerIndices("0");
      setCfgDescription("");
      toast("Embedding config created", "success");
    } catch (err: any) {
      setCfgError(String(err));
    } finally {
      setCfgSubmitting(false);
    }
  }

  async function handleDeleteConfig(id: string) {
    const cfg = configs.find((c) => c.id === id);
    const count = cfg?.embedding_count ?? 0;
    const msg = count > 0
      ? t("configsTab.deleteConfirm", { count: count.toLocaleString() })
      : t("configsTab.deleteConfirmNoEmbeddings");
    if (!confirm(msg)) return;
    try {
      await deleteEmbeddingConfig(id);
      setConfigs((prev) => prev.filter((c) => c.id !== id));
      toast("Config deleted", "info");
    } catch (err: any) {
      setError(String(err));
      toast(String(err), "error");
    }
  }

  async function handleComputeSubmit(e: React.FormEvent) {
    e.preventDefault();
    setCmpError("");
    setCmpResult(null);
    const effectiveConfigId = cmpConfigId || configs[0]?.id || "";
    if (!effectiveConfigId) {
      setCmpError("Select an embedding config.");
      return;
    }
    setCmpSubmitting(true);
    try {
      const result = await createJob({
        operation: "compute_embeddings",
        queue_name: "protea.embeddings",
        payload: {
          embedding_config_id: effectiveConfigId,
          sequences_per_job: cmpQueueBatchSize,
          batch_size: cmpBatchSize,
          device: cmpDevice,
          skip_existing: cmpSkipExisting,
          ...(cmpQuerySetId ? { query_set_id: cmpQuerySetId } : {}),
        },
      });
      setCmpResult(result);
      toast("Compute job queued", "success");
    } catch (err: any) {
      setCmpError(String(err));
      toast(String(err), "error");
    } finally {
      setCmpSubmitting(false);
    }
  }

  const tabs: { key: Tab; label: string }[] = [
    { key: "configs", label: t("tabs.configs") },
    { key: "compute", label: t("tabs.compute") },
  ];

  const inputClass =
    "w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
  const labelClass = "block text-sm font-medium text-gray-700 mb-1";

  return (
    <>
      <div className="flex flex-wrap items-center gap-3 mb-4">
        <h1 className="text-xl font-semibold">{t("title")}</h1>
      </div>

      {error && (
        <pre className="mb-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </pre>
      )}

      {/* Tab bar */}
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

      {/* ── Configs Tab ── */}
      {activeTab === "configs" && (
        <div>
          <div className="flex items-center justify-between mb-3">
            <p className="text-sm text-gray-500">{t("configsTab.configs", { count: configs.length })}</p>
            <button
              onClick={() => setShowConfigForm((v) => !v)}
              className="rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700"
            >
              {showConfigForm ? t("configsTab.cancel") : t("configsTab.newConfig")}
            </button>
          </div>

          {showConfigForm && (
            <div className="mb-6 rounded-lg border bg-white p-5 shadow-sm">
              <h2 className="text-base font-semibold mb-4">{t("configsTab.newConfigForm.title")}</h2>
              <form onSubmit={handleCreateConfig} className="space-y-4">
                {/* Layer indexing convention warning */}
                <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                  {t("configsTab.newConfigForm.layerIndexingWarning")}
                </div>

                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                  <div>
                    <label className={labelClass}>{t("configsTab.newConfigForm.modelBackendLabel")}</label>
                    <select
                      value={cfgBackend}
                      onChange={(e) => {
                        const b = e.target.value;
                        setCfgBackend(b);
                        const presets = MODEL_PRESETS[b] ?? [];
                        const first = presets[0];
                        if (first) {
                          setCfgModelPreset(first.value);
                          setCfgMaxLength(first.defaultMaxLength);
                        } else {
                          setCfgModelPreset("__custom__");
                        }
                        setCfgLayerIndices("0");
                      }}
                      className={inputClass}
                    >
                      <option value="esm">{t("configsTab.newConfigForm.modelBackendEsm")}</option>
                      <option value="esm3c">{t("configsTab.newConfigForm.modelBackendEsm3c")}</option>
                      <option value="t5">{t("configsTab.newConfigForm.modelBackendT5")}</option>
                      <option value="auto">{t("configsTab.newConfigForm.modelBackendAuto")}</option>
                    </select>
                  </div>
                  <div>
                    <label className={labelClass}>{t("configsTab.newConfigForm.modelLabel")}</label>
                    <select
                      value={cfgModelPreset}
                      onChange={(e) => {
                        const v = e.target.value;
                        setCfgModelPreset(v);
                        const preset = (MODEL_PRESETS[cfgBackend] ?? []).find((p) => p.value === v);
                        if (preset) setCfgMaxLength(preset.defaultMaxLength);
                      }}
                      className={inputClass}
                    >
                      {(MODEL_PRESETS[cfgBackend] ?? []).map((p) => (
                        <option key={p.value} value={p.value}>{p.label}</option>
                      ))}
                      <option value="__custom__">Custom model ID…</option>
                    </select>
                    {cfgModelPreset === "__custom__" && (
                      <input
                        type="text"
                        value={cfgModelCustom}
                        onChange={(e) => setCfgModelCustom(e.target.value)}
                        placeholder={t("configsTab.newConfigForm.customModelPlaceholder")}
                        required
                        className={`${inputClass} mt-1 font-mono text-xs`}
                      />
                    )}
                    {cfgModelPreset !== "__custom__" && (
                      <p className="mt-1 font-mono text-xs text-gray-400 truncate" title={cfgModelPreset}>
                        {cfgModelPreset}
                      </p>
                    )}
                  </div>
                  <div>
                    <label className={labelClass}>
                      {t("configsTab.newConfigForm.layerIndicesLabel")}{" "}
                      <span className="font-normal text-gray-400">{t("configsTab.newConfigForm.layerIndicesHelper")}</span>
                    </label>
                    <input
                      type="text"
                      value={cfgLayerIndices}
                      onChange={(e) => setCfgLayerIndices(e.target.value)}
                      placeholder={t("configsTab.newConfigForm.layerIndicesPlaceholder")}
                      required
                      className={inputClass}
                    />
                  </div>
                  <div>
                    <label className={labelClass}>{t("configsTab.newConfigForm.layerAggLabel")}</label>
                    <select value={cfgLayerAgg} onChange={(e) => setCfgLayerAgg(e.target.value)} className={inputClass}>
                      <option value="mean">{t("configsTab.newConfigForm.layerAggMean")}</option>
                      <option value="last">{t("configsTab.newConfigForm.layerAggLast")}</option>
                      <option value="concat">{t("configsTab.newConfigForm.layerAggConcat")}</option>
                    </select>
                  </div>
                  <div>
                    <label className={labelClass}>{t("configsTab.newConfigForm.poolingLabel")}</label>
                    <select value={cfgPooling} onChange={(e) => setCfgPooling(e.target.value)} className={inputClass}>
                      <option value="mean">{t("configsTab.newConfigForm.poolingMean")}</option>
                      <option value="max">{t("configsTab.newConfigForm.poolingMax")}</option>
                      <option value="mean_max">{t("configsTab.newConfigForm.poolingMeanMax")}</option>
                      <option value="cls">{t("configsTab.newConfigForm.poolingCls")}</option>
                    </select>
                  </div>
                  <div>
                    <label className={labelClass}>{t("configsTab.newConfigForm.maxLengthLabel")}</label>
                    <input
                      type="number"
                      value={cfgMaxLength}
                      onChange={(e) => { const v = parseInt(e.target.value, 10); if (!isNaN(v)) setCfgMaxLength(v); }}
                      min={1}
                      className={inputClass}
                    />
                  </div>
                  <div className="sm:col-span-2">
                    <label className={labelClass}>{t("configsTab.newConfigForm.descriptionLabel")}</label>
                    <input
                      type="text"
                      value={cfgDescription}
                      onChange={(e) => setCfgDescription(e.target.value)}
                      className={inputClass}
                    />
                  </div>

                  {/* Normalisation */}
                  <div className="flex items-center gap-2">
                    <input
                      id="cfg-norm-residues"
                      type="checkbox"
                      checked={cfgNormalizeResidues}
                      onChange={(e) => setCfgNormalizeResidues(e.target.checked)}
                      className="rounded"
                    />
                    <label htmlFor="cfg-norm-residues" className="text-sm text-gray-700 cursor-pointer">
                      {t("configsTab.newConfigForm.normalizeResidues")}
                    </label>
                  </div>
                  <div className="flex items-center gap-2">
                    <input
                      id="cfg-normalize"
                      type="checkbox"
                      checked={cfgNormalize}
                      onChange={(e) => setCfgNormalize(e.target.checked)}
                      className="rounded"
                    />
                    <label htmlFor="cfg-normalize" className="text-sm text-gray-700 cursor-pointer">
                      {t("configsTab.newConfigForm.normalizeFinal")}
                    </label>
                  </div>

                  {/* Chunking */}
                  <div className="sm:col-span-2 flex items-center gap-2">
                    <input
                      id="cfg-chunking"
                      type="checkbox"
                      checked={cfgUseChunking}
                      onChange={(e) => setCfgUseChunking(e.target.checked)}
                      className="rounded"
                    />
                    <label htmlFor="cfg-chunking" className="text-sm text-gray-700 cursor-pointer">
                      {t("configsTab.newConfigForm.enableChunking")}
                    </label>
                  </div>
                  {cfgUseChunking && (
                    <>
                      <div>
                        <label className={labelClass}>{t("configsTab.newConfigForm.chunkSizeLabel")}</label>
                        <input
                          type="number"
                          value={cfgChunkSize}
                          onChange={(e) => { const v = parseInt(e.target.value, 10); if (!isNaN(v)) setCfgChunkSize(v); }}
                          min={1}
                          className={inputClass}
                        />
                      </div>
                      <div>
                        <label className={labelClass}>{t("configsTab.newConfigForm.chunkOverlapLabel")}</label>
                        <input
                          type="number"
                          value={cfgChunkOverlap}
                          onChange={(e) => { const v = parseInt(e.target.value, 10); if (!isNaN(v)) setCfgChunkOverlap(v); }}
                          min={0}
                          className={inputClass}
                        />
                      </div>
                    </>
                  )}
                </div>

                {cfgError && (
                  <p className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
                    {cfgError}
                  </p>
                )}

                <div className="flex justify-end gap-2">
                  <button
                    type="button"
                    onClick={() => setShowConfigForm(false)}
                    className="rounded-md border px-4 py-2 text-sm hover:bg-gray-50"
                  >
                    {t("configsTab.cancel")}
                  </button>
                  <button
                    type="submit"
                    disabled={cfgSubmitting}
                    className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
                  >
                    {cfgSubmitting ? t("configsTab.newConfigForm.creating") : t("configsTab.newConfigForm.createConfig")}
                  </button>
                </div>
              </form>
            </div>
          )}

          {loading ? (
            <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
              {Array.from({ length: 3 }).map((_, i) => <SkeletonTableRow key={i} cols={9} />)}
            </div>
          ) : (
            <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
              <div className="grid grid-cols-[1fr_140px_80px_100px_80px_80px_60px_160px_60px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
                <div>{t("configsTab.tableHeaders.description")}</div>
                <div>{t("configsTab.tableHeaders.model")}</div>
                <div>{t("configsTab.tableHeaders.backend")}</div>
                <div>{t("configsTab.tableHeaders.layers")}</div>
                <div>{t("configsTab.tableHeaders.agg")}</div>
                <div>{t("configsTab.tableHeaders.pool")}</div>
                <div>{t("configsTab.tableHeaders.norm")}</div>
                <div>{t("configsTab.tableHeaders.created")}</div>
                <div></div>
              </div>
              {configs.map((c) => (
                <div
                  key={c.id}
                  className="grid grid-cols-[1fr_140px_80px_100px_80px_80px_60px_160px_60px] gap-2 border-b px-4 py-3 text-sm last:border-0 items-center"
                >
                  <div className="text-gray-700 truncate" title={c.description ?? c.model_name}>
                    {c.description || <span className="text-gray-400 italic">—</span>}
                  </div>
                  <div className="font-mono text-xs text-gray-500 truncate" title={c.model_name}>{c.model_name}</div>
                  <div className="text-gray-600">{c.model_backend}</div>
                  <div className="font-mono text-xs text-gray-500">[{c.layer_indices.join(", ")}]</div>
                  <div className="text-gray-600">{c.layer_agg}</div>
                  <div className="text-gray-600">{c.pooling}</div>
                  <div className="text-gray-600">{c.normalize ? "yes" : "no"}</div>
                  <div className="text-xs text-gray-400">{formatDate(c.created_at)}</div>
                  <div>
                    <button
                      onClick={() => handleDeleteConfig(c.id)}
                      className="text-gray-400 hover:text-red-600 transition-colors"
                      title="Delete config"
                    >
                      ✕
                    </button>
                  </div>
                </div>
              ))}
              {configs.length === 0 && (
                <div className="px-4 py-8 text-center text-sm text-gray-400">
                  {t("configsTab.noConfigs")}{" "}
                  <button onClick={() => setShowConfigForm(true)} className="text-blue-600 underline">
                    ↑
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* ── Compute Tab ── */}
      {activeTab === "compute" && (
        <div className="max-w-2xl">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-4">{t("computeTab.title")}</h2>
            {loading ? (
              <p className="text-sm text-gray-400">{t("computeTab.loading")}</p>
            ) : (
              <form onSubmit={handleComputeSubmit} className="space-y-4">
                <div>
                  <label className={labelClass}>{t("computeTab.configLabel")}</label>
                  <select
                    value={cmpConfigId || configs[0]?.id || ""}
                    onChange={(e) => setCmpConfigId(e.target.value)}
                    required
                    className={inputClass}
                  >
                    {configs.length === 0 && (
                      <option value="">{t("computeTab.noConfigs")}</option>
                    )}
                    {configs.map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.description || c.model_name} ({shortId(c.id)})
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className={labelClass}>
                    {t("computeTab.querySetLabel")} <span className="font-normal text-gray-400">{t("computeTab.querySetHelper")}</span>
                  </label>
                  <select
                    value={cmpQuerySetId}
                    onChange={(e) => setCmpQuerySetId(e.target.value)}
                    className={inputClass}
                  >
                    <option value="">{t("computeTab.allSequences")}</option>
                    {querySets.map((qs) => (
                      <option key={qs.id} value={qs.id}>
                        {qs.name} ({qs.entry_count} seqs)
                      </option>
                    ))}
                  </select>
                </div>

                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className={labelClass}>
                      {t("computeTab.queueBatchSizeLabel")}{" "}
                      <span className="font-normal text-gray-400">{t("computeTab.queueBatchSizeHelper")}</span>
                    </label>
                    <input
                      type="number"
                      value={cmpQueueBatchSize}
                      onChange={(e) => { const v = parseInt(e.target.value, 10); if (!isNaN(v)) setCmpQueueBatchSize(v); }}
                      min={1}
                      className={inputClass}
                    />
                  </div>
                  <div>
                    <label className={labelClass}>
                      {t("computeTab.modelBatchSizeLabel")}{" "}
                      <span className="font-normal text-gray-400">{t("computeTab.modelBatchSizeHelper")}</span>
                    </label>
                    <input
                      type="number"
                      value={cmpBatchSize}
                      onChange={(e) => { const v = parseInt(e.target.value, 10); if (!isNaN(v)) setCmpBatchSize(v); }}
                      min={1}
                      className={inputClass}
                    />
                  </div>
                </div>

                <div>
                  <label className={labelClass}>{t("computeTab.deviceLabel")}</label>
                  <select
                    value={["cpu", "cuda", "cuda:0", "cuda:1"].includes(cmpDevice) ? cmpDevice : "custom"}
                    onChange={(e) => {
                      if (e.target.value !== "custom") setCmpDevice(e.target.value);
                    }}
                    className={inputClass}
                  >
                    <option value="cpu">{t("computeTab.deviceCpu")}</option>
                    <option value="cuda">{t("computeTab.deviceCuda")}</option>
                    <option value="cuda:0">{t("computeTab.deviceCuda0")}</option>
                    <option value="cuda:1">{t("computeTab.deviceCuda1")}</option>
                    <option value="custom">{t("computeTab.deviceCustom")}</option>
                  </select>
                  {(cmpDevice === "custom" || !["cpu", "cuda", "cuda:0", "cuda:1"].includes(cmpDevice)) && (
                    <input
                      type="text"
                      value={cmpDevice}
                      onChange={(e) => setCmpDevice(e.target.value)}
                      placeholder="e.g. cuda:2"
                      className={`${inputClass} mt-1`}
                    />
                  )}
                </div>

                <div className="flex items-center gap-2">
                  <input
                    id="cmp-skip-existing"
                    type="checkbox"
                    checked={cmpSkipExisting}
                    onChange={(e) => setCmpSkipExisting(e.target.checked)}
                    className="rounded"
                  />
                  <label htmlFor="cmp-skip-existing" className="text-sm text-gray-700 cursor-pointer">
                    {t("computeTab.skipExisting")}
                  </label>
                </div>

                {cmpError && (
                  <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                    {cmpError}
                  </pre>
                )}

                {cmpResult && (
                  <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                    Job queued:{" "}
                    <Link href={`/jobs/${cmpResult.id}`} className="font-mono underline hover:text-green-900">
                      {cmpResult.id}
                    </Link>
                  </div>
                )}

                <div className="flex justify-end">
                  <button
                    type="submit"
                    disabled={cmpSubmitting || configs.length === 0}
                    className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
                  >
                    {cmpSubmitting ? t("computeTab.launching") : t("computeTab.launchComputeJob")}
                  </button>
                </div>
              </form>
            )}
          </div>
        </div>
      )}
    </>
  );
}
