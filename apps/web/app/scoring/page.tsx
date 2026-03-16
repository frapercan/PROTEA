"use client";

/**
 * Scoring Configs management page.
 *
 * Allows users to:
 *   - View all stored ScoringConfigs with their signal and evidence weights.
 *   - Load the four built-in preset configs in one click.
 *   - Create new configs by tuning signal sliders, formula, and optionally
 *     overriding per-evidence-code quality weights.
 *   - Delete configs that are no longer needed.
 *
 * The scoring system has two independent layers of configuration:
 *
 *   1. **Signal weights** — how much each composite signal contributes to
 *      the weighted average (embedding similarity, NW/SW identity, evidence
 *      quality signal, taxonomic proximity).
 *
 *   2. **Evidence-code weights** — per-GO-evidence-code quality multipliers
 *      that define what each code tier is worth.  The system ships with
 *      sensible defaults (EXP/IDA → 1.0, ISS/IBA → 0.7, IEA → 0.3 …).
 *      Leaving the override null means "use the system defaults".
 */

import { useEffect, useState } from "react";
import { useToast } from "@/components/Toast";
import {
  listScoringConfigs,
  createScoringConfig,
  deleteScoringConfig,
  createPresetScoringConfigs,
  ScoringConfig,
} from "@/lib/api";

// ── Signal definitions ────────────────────────────────────────────────────────

const SIGNALS: { key: string; label: string; hint: string }[] = [
  {
    key: "embedding_similarity",
    label: "Embedding similarity",
    hint: "1 − cosine_distance / 2 — always available, no flags required.",
  },
  {
    key: "identity_nw",
    label: "Identity NW",
    hint: "Needleman-Wunsch global sequence identity [0, 1]. Requires compute_alignments=True.",
  },
  {
    key: "identity_sw",
    label: "Identity SW",
    hint: "Smith-Waterman local sequence identity [0, 1]. Requires compute_alignments=True.",
  },
  {
    key: "evidence_weight",
    label: "Evidence weight",
    hint: "Quality of the reference annotation's GO evidence code, resolved via the evidence-weight table below.",
  },
  {
    key: "taxonomic_proximity",
    label: "Taxonomic proximity",
    hint: "1 / (1 + taxonomic_distance) — requires compute_taxonomy=True.",
  },
];

const DEFAULT_SIGNAL_WEIGHTS: Record<string, number> = {
  embedding_similarity: 1.0,
  identity_nw: 0.0,
  identity_sw: 0.0,
  evidence_weight: 0.0,
  taxonomic_proximity: 0.0,
};

// ── Evidence-code definitions ─────────────────────────────────────────────────
// Mirrors DEFAULT_EVIDENCE_WEIGHTS in scoring_config.py (single source of truth
// for defaults; this table is used to initialise the form sliders).

const EVIDENCE_CODE_GROUPS: {
  label: string;
  description: string;
  codes: { code: string; label: string }[];
}[] = [
  {
    label: "Experimental",
    description:
      "Annotations backed by direct experimental evidence. Highest confidence tier.",
    codes: [
      { code: "EXP", label: "Inferred from Experiment" },
      { code: "IDA", label: "Direct Assay" },
      { code: "IPI", label: "Physical Interaction" },
      { code: "IMP", label: "Mutant Phenotype" },
      { code: "IGI", label: "Genetic Interaction" },
      { code: "IEP", label: "Expression Pattern" },
      { code: "HTP", label: "High-Throughput (umbrella)" },
      { code: "HDA", label: "HT Direct Assay" },
      { code: "HMP", label: "HT Mutant Phenotype" },
      { code: "HGI", label: "HT Genetic Interaction" },
      { code: "HEP", label: "HT Expression Pattern" },
      { code: "IC",  label: "Inferred by Curator" },
      { code: "TAS", label: "Traceable Author Statement" },
    ],
  },
  {
    label: "Computational / Phylogenetic",
    description:
      "Annotations derived from sequence similarity, orthology, or phylogenetic inference.",
    codes: [
      { code: "ISS", label: "Sequence or Structural Similarity" },
      { code: "ISO", label: "Sequence Orthology" },
      { code: "ISA", label: "Sequence Alignment" },
      { code: "ISM", label: "Sequence Model" },
      { code: "IGC", label: "Genomic Context" },
      { code: "IBA", label: "Biological aspect of Ancestor" },
      { code: "IBD", label: "Biological aspect of Descendant" },
      { code: "IKR", label: "Key Residues" },
      { code: "IRD", label: "Rapid Divergence" },
      { code: "RCA", label: "Reviewed Computational Analysis" },
    ],
  },
  {
    label: "Electronic",
    description:
      "Automated annotations (IEA) or non-traceable author statements (NAS). Lower confidence.",
    codes: [
      { code: "NAS", label: "Non-traceable Author Statement" },
      { code: "IEA", label: "Inferred from Electronic Annotation" },
    ],
  },
  {
    label: "No data",
    description: "Placeholder code indicating no biological data is available.",
    codes: [{ code: "ND", label: "No biological Data" }],
  },
];

/** System defaults — matches DEFAULT_EVIDENCE_WEIGHTS in scoring_config.py. */
const SYSTEM_EVIDENCE_DEFAULTS: Record<string, number> = {
  EXP: 1.0, IDA: 1.0, IPI: 1.0, IMP: 1.0, IGI: 1.0, IEP: 1.0,
  HTP: 1.0, HDA: 1.0, HMP: 1.0, HGI: 1.0, HEP: 1.0, IC: 1.0, TAS: 1.0,
  ISS: 0.7, ISO: 0.7, ISA: 0.7, ISM: 0.7, IGC: 0.7,
  IBA: 0.7, IBD: 0.7, IKR: 0.7, IRD: 0.7, RCA: 0.7,
  NAS: 0.5,
  IEA: 0.3,
  ND: 0.1,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function allCodes(): string[] {
  return EVIDENCE_CODE_GROUPS.flatMap((g) => g.codes.map((c) => c.code));
}

// ── WeightBar ─────────────────────────────────────────────────────────────────

function WeightBar({
  label,
  value,
  hint,
}: {
  label: string;
  value: number;
  hint: string;
}) {
  return (
    <div className="flex items-center gap-2" title={hint}>
      <span className="text-xs text-gray-500 w-40 shrink-0">{label}</span>
      <div className="flex-1 h-1.5 rounded-full bg-gray-100 overflow-hidden">
        <div
          className="h-1.5 rounded-full bg-blue-400 transition-all"
          style={{ width: `${value * 100}%` }}
        />
      </div>
      <span
        className={`font-mono text-xs w-8 text-right ${
          value > 0 ? "text-gray-700" : "text-gray-300"
        }`}
      >
        {value.toFixed(2)}
      </span>
    </div>
  );
}

// ── ConfigCard ────────────────────────────────────────────────────────────────

function ConfigCard({
  config,
  onDelete,
}: {
  config: ScoringConfig;
  onDelete: (id: string) => void;
}) {
  const [deleting, setDeleting] = useState(false);
  const [showEvidenceWeights, setShowEvidenceWeights] = useState(false);

  async function handleDelete() {
    if (!confirm(`Delete scoring config "${config.name}"?`)) return;
    setDeleting(true);
    try {
      await deleteScoringConfig(config.id);
      onDelete(config.id);
    } catch {
      setDeleting(false);
    }
  }

  const hasCustomEvidence = config.evidence_weights != null;

  return (
    <div className="rounded-lg border bg-white p-4 shadow-sm">
      {/* Header */}
      <div className="flex items-start justify-between gap-3 mb-3">
        <div>
          <span className="font-semibold text-gray-900">{config.name}</span>
          <span className="ml-2 rounded bg-blue-50 border border-blue-100 px-1.5 py-0.5 text-xs font-mono text-blue-700">
            {config.formula}
          </span>
          {hasCustomEvidence && (
            <span className="ml-1.5 rounded bg-amber-50 border border-amber-100 px-1.5 py-0.5 text-xs text-amber-700">
              custom evidence weights
            </span>
          )}
          {config.description && (
            <p className="mt-0.5 text-xs text-gray-400">{config.description}</p>
          )}
        </div>
        <button
          onClick={handleDelete}
          disabled={deleting}
          className="text-xs text-red-400 hover:text-red-600 disabled:opacity-40 shrink-0"
        >
          {deleting ? "…" : "Delete"}
        </button>
      </div>

      {/* Signal weights */}
      <div className="space-y-1.5">
        {SIGNALS.map(({ key, label, hint }) => (
          <WeightBar key={key} label={label} value={config.weights[key] ?? 0} hint={hint} />
        ))}
      </div>

      {/* Evidence weights */}
      <div className="mt-3 border-t pt-2.5">
        <button
          onClick={() => setShowEvidenceWeights((v) => !v)}
          className="text-xs text-gray-400 hover:text-gray-600"
        >
          {showEvidenceWeights ? "▲" : "▶"} Evidence-code weights{" "}
          {hasCustomEvidence ? "(custom)" : "(system defaults)"}
        </button>

        {showEvidenceWeights && (
          <div className="mt-3 space-y-4">
            {EVIDENCE_CODE_GROUPS.map((group) => (
              <div key={group.label}>
                <p className="text-xs font-semibold text-gray-500 mb-1.5">
                  {group.label}
                </p>
                <div className="space-y-1">
                  {group.codes.map(({ code, label }) => {
                    const val =
                      config.evidence_weights?.[code] ??
                      SYSTEM_EVIDENCE_DEFAULTS[code] ??
                      0.5;
                    const isOverridden = config.evidence_weights?.[code] != null;
                    return (
                      <div key={code} className="flex items-center gap-2">
                        <span
                          className={`font-mono text-xs w-10 shrink-0 ${
                            isOverridden ? "text-amber-700 font-semibold" : "text-gray-500"
                          }`}
                        >
                          {code}
                        </span>
                        <span className="text-xs text-gray-400 w-52 shrink-0 truncate" title={label}>
                          {label}
                        </span>
                        <div className="flex-1 h-1.5 rounded-full bg-gray-100 overflow-hidden">
                          <div
                            className={`h-1.5 rounded-full transition-all ${
                              isOverridden ? "bg-amber-400" : "bg-gray-300"
                            }`}
                            style={{ width: `${val * 100}%` }}
                          />
                        </div>
                        <span className="font-mono text-xs w-8 text-right text-gray-600">
                          {val.toFixed(2)}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <p className="mt-2 text-xs text-gray-300">
        Created {new Date(config.created_at).toLocaleDateString()}
      </p>
    </div>
  );
}

// ── NewConfigForm ─────────────────────────────────────────────────────────────

function NewConfigForm({ onCreated }: { onCreated: (c: ScoringConfig) => void }) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [formula, setFormula] = useState("linear");
  const [weights, setWeights] = useState<Record<string, number>>({
    ...DEFAULT_SIGNAL_WEIGHTS,
  });
  const [description, setDescription] = useState("");

  // Evidence weights — null means "use system defaults"; toggling the
  // checkbox allocates the override dict with a copy of the defaults.
  const [useCustomEvidence, setUseCustomEvidence] = useState(false);
  const [evidenceWeights, setEvidenceWeights] = useState<Record<string, number>>({
    ...SYSTEM_EVIDENCE_DEFAULTS,
  });

  const [saving, setSaving] = useState(false);
  const toast = useToast();

  function reset() {
    setName("");
    setFormula("linear");
    setWeights({ ...DEFAULT_SIGNAL_WEIGHTS });
    setDescription("");
    setUseCustomEvidence(false);
    setEvidenceWeights({ ...SYSTEM_EVIDENCE_DEFAULTS });
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) return;
    setSaving(true);
    try {
      const c = await createScoringConfig({
        name: name.trim(),
        formula,
        weights,
        evidence_weights: useCustomEvidence ? evidenceWeights : null,
        description: description || undefined,
      });
      onCreated(c);
      reset();
      setOpen(false);
      toast("Scoring config created", "success");
    } catch (err: any) {
      toast(err.message ?? "Failed to create config", "error");
    } finally {
      setSaving(false);
    }
  }

  function setGroupWeight(codes: string[], value: number) {
    setEvidenceWeights((prev) => {
      const next = { ...prev };
      for (const code of codes) next[code] = value;
      return next;
    });
  }

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="rounded-md border border-dashed border-gray-300 bg-white px-4 py-3 text-sm text-gray-500 hover:border-blue-400 hover:text-blue-600 w-full text-center"
      >
        + New scoring config
      </button>
    );
  }

  return (
    <form
      onSubmit={handleSubmit}
      className="rounded-lg border bg-white p-5 shadow-sm space-y-5"
    >
      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <span className="text-sm font-semibold text-gray-700">New config</span>
        <button
          type="button"
          onClick={() => { setOpen(false); reset(); }}
          className="text-gray-400 hover:text-gray-600 text-lg leading-none"
        >
          ×
        </button>
      </div>

      {/* ── Name + formula ── */}
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Name</label>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="my_config"
            className="w-full rounded border px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            required
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Formula</label>
          <select
            value={formula}
            onChange={(e) => setFormula(e.target.value)}
            className="w-full rounded border px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="linear">linear</option>
            <option value="evidence_weighted">evidence_weighted</option>
          </select>
        </div>
      </div>

      {formula === "evidence_weighted" && (
        <p className="text-xs text-blue-700 bg-blue-50 rounded px-2.5 py-2">
          <strong>evidence_weighted</strong>: the weighted average is multiplied
          by the resolved evidence quality at the end — even when the
          &ldquo;Evidence weight&rdquo; signal slider is set to 0. This
          down-ranks IEA-sourced predictions regardless of embedding strength.
        </p>
      )}

      <div>
        <label className="block text-xs font-medium text-gray-600 mb-1">
          Description <span className="font-normal text-gray-400">(optional)</span>
        </label>
        <input
          type="text"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          placeholder="What this config is for…"
          className="w-full rounded border px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      {/* ── Signal weights ── */}
      <div>
        <label className="block text-xs font-medium text-gray-600 mb-2">
          Signal weights
        </label>
        <div className="space-y-3">
          {SIGNALS.map(({ key, label, hint }) => {
            const val = weights[key] ?? 0;
            return (
              <div key={key} className="flex items-center gap-3" title={hint}>
                <span className="text-xs text-gray-600 w-44 shrink-0">{label}</span>
                <input
                  type="range"
                  min="0"
                  max="1"
                  step="0.05"
                  value={val}
                  onChange={(e) =>
                    setWeights((prev) => ({ ...prev, [key]: parseFloat(e.target.value) }))
                  }
                  className="flex-1 accent-blue-500"
                />
                <span className="font-mono text-xs text-gray-700 w-10 text-right">
                  {val.toFixed(2)}
                </span>
              </div>
            );
          })}
        </div>
      </div>

      {/* ── Evidence-code weights ── */}
      <div className="border rounded-md p-3 space-y-3">
        <div className="flex items-center gap-2">
          <input
            id="custom-evidence"
            type="checkbox"
            checked={useCustomEvidence}
            onChange={(e) => setUseCustomEvidence(e.target.checked)}
            className="accent-blue-500"
          />
          <label htmlFor="custom-evidence" className="text-xs font-medium text-gray-700 cursor-pointer">
            Override per-evidence-code quality weights
          </label>
        </div>

        {!useCustomEvidence && (
          <p className="text-xs text-gray-400">
            Using system defaults — EXP/IDA → 1.0 · ISS/IBA → 0.7 · IEA → 0.3 · ND → 0.1
          </p>
        )}

        {useCustomEvidence && (
          <div className="space-y-5">
            {EVIDENCE_CODE_GROUPS.map((group) => (
              <div key={group.label}>
                <div className="flex items-center justify-between mb-1.5">
                  <div>
                    <p className="text-xs font-semibold text-gray-600">{group.label}</p>
                    <p className="text-xs text-gray-400">{group.description}</p>
                  </div>
                  {/* Group-level shortcuts */}
                  <div className="flex gap-1 shrink-0">
                    {[0, 0.5, 1].map((v) => (
                      <button
                        key={v}
                        type="button"
                        onClick={() =>
                          setGroupWeight(group.codes.map((c) => c.code), v)
                        }
                        className="rounded border px-1.5 py-0.5 text-xs text-gray-500 hover:bg-gray-50"
                      >
                        {v === 0 ? "Off" : v === 1 ? "Max" : "0.5"}
                      </button>
                    ))}
                  </div>
                </div>
                <div className="space-y-2">
                  {group.codes.map(({ code, label }) => {
                    const val = evidenceWeights[code] ?? SYSTEM_EVIDENCE_DEFAULTS[code] ?? 0.5;
                    const isDefault =
                      Math.abs(val - (SYSTEM_EVIDENCE_DEFAULTS[code] ?? 0.5)) < 0.001;
                    return (
                      <div key={code} className="flex items-center gap-2">
                        <span
                          className={`font-mono text-xs w-10 shrink-0 ${
                            isDefault ? "text-gray-500" : "text-amber-700 font-semibold"
                          }`}
                          title={label}
                        >
                          {code}
                        </span>
                        <span className="text-xs text-gray-400 w-48 shrink-0 truncate" title={label}>
                          {label}
                        </span>
                        <input
                          type="range"
                          min="0"
                          max="1"
                          step="0.05"
                          value={val}
                          onChange={(e) =>
                            setEvidenceWeights((prev) => ({
                              ...prev,
                              [code]: parseFloat(e.target.value),
                            }))
                          }
                          className="flex-1 accent-blue-500"
                        />
                        <span
                          className={`font-mono text-xs w-8 text-right ${
                            isDefault ? "text-gray-500" : "text-amber-700 font-semibold"
                          }`}
                        >
                          {val.toFixed(2)}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}

            <button
              type="button"
              onClick={() => setEvidenceWeights({ ...SYSTEM_EVIDENCE_DEFAULTS })}
              className="text-xs text-gray-400 hover:text-gray-600 underline"
            >
              Reset all to system defaults
            </button>
          </div>
        )}
      </div>

      {/* ── Actions ── */}
      <div className="flex gap-2 pt-1">
        <button
          type="submit"
          disabled={saving || !name.trim()}
          className="rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-40"
        >
          {saving ? "Saving…" : "Save config"}
        </button>
        <button
          type="button"
          onClick={() => { setOpen(false); reset(); }}
          className="rounded-md border px-4 py-2 text-sm text-gray-600 hover:bg-gray-50"
        >
          Cancel
        </button>
      </div>
    </form>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function ScoringPage() {
  const [configs, setConfigs] = useState<ScoringConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingPresets, setLoadingPresets] = useState(false);
  const toast = useToast();

  useEffect(() => {
    listScoringConfigs()
      .then(setConfigs)
      .catch(() => toast("Failed to load scoring configs", "error"))
      .finally(() => setLoading(false));
  }, []);

  async function handleLoadPresets() {
    setLoadingPresets(true);
    try {
      const result = await createPresetScoringConfigs();
      const updated = await listScoringConfigs();
      setConfigs(updated);
      toast(
        result.created.length > 0
          ? `Presets created: ${result.created.join(", ")}`
          : "All presets already exist",
        result.created.length > 0 ? "success" : "info",
      );
    } catch (err: any) {
      toast(err.message ?? "Failed to load presets", "error");
    } finally {
      setLoadingPresets(false);
    }
  }

  return (
    <>
      {/* ── Page header ── */}
      <div className="mb-6">
        <div className="flex items-center justify-between gap-4 mb-1">
          <h1 className="text-xl font-semibold text-gray-900">Scoring Configs</h1>
          <button
            onClick={handleLoadPresets}
            disabled={loadingPresets}
            className="rounded-md border bg-white px-3 py-1.5 text-sm text-gray-700 shadow-sm hover:bg-gray-50 disabled:opacity-40 shrink-0"
          >
            {loadingPresets ? "Loading…" : "Load presets"}
          </button>
        </div>
        <p className="text-sm text-gray-500">
          A ScoringConfig defines how raw prediction signals are combined into a
          single [0, 1] confidence score — without re-running the KNN pipeline.
          Two independent layers: <strong>signal weights</strong> (which signals
          matter and how much) and <strong>evidence-code weights</strong> (the
          quality value assigned to each GO evidence tier).
        </p>
      </div>

      {/* ── Reference card ── */}
      <div className="mb-6 rounded-lg border bg-white p-4 shadow-sm">
        <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-3">
          Available signals
        </p>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 mb-4">
          {SIGNALS.map(({ key, label, hint }) => (
            <div key={key} className="flex items-start gap-2">
              <span className="rounded bg-gray-100 px-1.5 py-0.5 font-mono text-xs text-gray-700 shrink-0">
                {label}
              </span>
              <span className="text-xs text-gray-500">{hint}</span>
            </div>
          ))}
        </div>
        <div className="border-t pt-3 grid grid-cols-1 gap-1.5 sm:grid-cols-2 text-xs text-gray-500">
          <div>
            <strong className="text-gray-700">linear:</strong>{" "}
            Σ(w_i · s_i) / Σ(w_i) over all active (w_i &gt; 0 and signal available) signals.
          </div>
          <div>
            <strong className="text-gray-700">evidence_weighted:</strong>{" "}
            Same as linear, then multiplied by the resolved evidence weight — down-ranks IEA
            even when other signals are strong.
          </div>
        </div>
      </div>

      {loading && <p className="text-sm text-gray-400">Loading…</p>}

      {!loading && (
        <div className="space-y-3">
          <NewConfigForm onCreated={(c) => setConfigs((prev) => [...prev, c])} />

          {configs.length === 0 && (
            <p className="text-sm text-gray-400 text-center py-8">
              No configs yet. Load the presets or create one above.
            </p>
          )}

          {configs.map((c) => (
            <ConfigCard
              key={c.id}
              config={c}
              onDelete={(id) => setConfigs((prev) => prev.filter((x) => x.id !== id))}
            />
          ))}
        </div>
      )}
    </>
  );
}
