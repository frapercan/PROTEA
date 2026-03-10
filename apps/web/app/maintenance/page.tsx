"use client";

import { useEffect, useState } from "react";
import {
  previewVacuumSequences,
  runVacuumSequences,
  previewVacuumEmbeddings,
  runVacuumEmbeddings,
  type VacuumSequencesPreview,
  type VacuumEmbeddingsPreview,
} from "@/lib/api";
import { useToast } from "@/components/Toast";

function StatRow({ label, value, highlight }: { label: string; value: number | null; highlight?: boolean }) {
  return (
    <div className="flex justify-between items-center py-1.5 border-b border-gray-100 last:border-0">
      <span className="text-sm text-gray-600">{label}</span>
      <span className={`text-sm font-mono font-semibold ${highlight && value ? "text-amber-600" : "text-gray-800"}`}>
        {value === null ? "—" : value.toLocaleString()}
      </span>
    </div>
  );
}

function VacuumCard({
  title,
  description,
  stats,
  orphanLabel,
  orphanValue,
  totalValue,
  onPreview,
  onVacuum,
  loading,
  vacuuming,
}: {
  title: string;
  description: string;
  stats: React.ReactNode;
  orphanLabel: string;
  orphanValue: number | null;
  totalValue: number | null;
  onPreview: () => void;
  onVacuum: () => void;
  loading: boolean;
  vacuuming: boolean;
}) {
  const hasOrphans = orphanValue !== null && orphanValue > 0;
  const pct = totalValue ? Math.round(((orphanValue ?? 0) / totalValue) * 100) : 0;

  return (
    <div className="border border-gray-200 rounded-lg p-5 bg-white shadow-sm">
      <div className="flex items-start justify-between gap-4 mb-3">
        <div>
          <h2 className="font-semibold text-gray-900">{title}</h2>
          <p className="text-xs text-gray-500 mt-0.5">{description}</p>
        </div>
        {orphanValue !== null && (
          <span
            className={`text-xs font-semibold px-2 py-0.5 rounded-full whitespace-nowrap ${
              hasOrphans ? "bg-amber-100 text-amber-700" : "bg-green-100 text-green-700"
            }`}
          >
            {hasOrphans ? `${orphanValue.toLocaleString()} to clean` : "Clean"}
          </span>
        )}
      </div>

      {orphanValue !== null && totalValue !== null && totalValue > 0 && (
        <div className="mb-3">
          <div className="flex justify-between text-xs text-gray-400 mb-1">
            <span>{orphanLabel}</span>
            <span>{pct}%</span>
          </div>
          <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full transition-all ${hasOrphans ? "bg-amber-400" : "bg-green-400"}`}
              style={{ width: `${pct}%` }}
            />
          </div>
        </div>
      )}

      <div className="mb-4">{stats}</div>

      <div className="flex gap-2">
        <button
          onClick={onPreview}
          disabled={loading}
          className="px-3 py-1.5 text-sm border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50 transition-colors"
        >
          {loading ? "Loading…" : "Refresh"}
        </button>
        <button
          onClick={onVacuum}
          disabled={vacuuming || !hasOrphans}
          className="px-3 py-1.5 text-sm bg-amber-500 text-white rounded hover:bg-amber-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          {vacuuming ? "Cleaning…" : `Vacuum ${orphanValue !== null ? `(${orphanValue.toLocaleString()})` : ""}`}
        </button>
      </div>
    </div>
  );
}

export default function MaintenancePage() {
  const toast = useToast();

  const [seqPreview, setSeqPreview] = useState<VacuumSequencesPreview | null>(null);
  const [seqLoading, setSeqLoading] = useState(false);
  const [seqVacuuming, setSeqVacuuming] = useState(false);

  const [embPreview, setEmbPreview] = useState<VacuumEmbeddingsPreview | null>(null);
  const [embLoading, setEmbLoading] = useState(false);
  const [embVacuuming, setEmbVacuuming] = useState(false);

  async function loadSeqPreview() {
    setSeqLoading(true);
    try {
      setSeqPreview(await previewVacuumSequences());
    } catch (e: any) {
      toast(e.message ?? "Failed to load sequence stats", "error");
    } finally {
      setSeqLoading(false);
    }
  }

  async function loadEmbPreview() {
    setEmbLoading(true);
    try {
      setEmbPreview(await previewVacuumEmbeddings());
    } catch (e: any) {
      toast(e.message ?? "Failed to load embedding stats", "error");
    } finally {
      setEmbLoading(false);
    }
  }

  async function doVacuumSequences() {
    setSeqVacuuming(true);
    try {
      const r = await runVacuumSequences();
      toast(`Deleted ${r.deleted_sequences.toLocaleString()} orphan sequence(s)`, "success");
      await loadSeqPreview();
      await loadEmbPreview(); // seq deletion cascades to embeddings
    } catch (e: any) {
      toast(e.message ?? "Vacuum failed", "error");
    } finally {
      setSeqVacuuming(false);
    }
  }

  async function doVacuumEmbeddings() {
    setEmbVacuuming(true);
    try {
      const r = await runVacuumEmbeddings();
      toast(`Deleted ${r.deleted_embeddings.toLocaleString()} unindexed embedding(s)`, "success");
      await loadEmbPreview();
    } catch (e: any) {
      toast(e.message ?? "Vacuum failed", "error");
    } finally {
      setEmbVacuuming(false);
    }
  }

  useEffect(() => {
    async function init() {
      setSeqLoading(true);
      setEmbLoading(true);
      try {
        setSeqPreview(await previewVacuumSequences());
      } catch {
        // silently skip on initial load
      } finally {
        setSeqLoading(false);
      }
      try {
        setEmbPreview(await previewVacuumEmbeddings());
      } catch {
        // silently skip on initial load
      } finally {
        setEmbLoading(false);
      }
    }
    init();
  }, []);

  return (
    <main className="max-w-2xl mx-auto px-4 py-8 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Maintenance</h1>
        <p className="text-sm text-gray-500 mt-1">
          Clean up orphaned data that accumulates over time. All operations are safe to run while the system is active.
        </p>
      </div>

      <VacuumCard
        title="Orphan Sequences"
        description="Sequences with no Protein and no QuerySet entry pointing to them. Deleting them also cascades to their embeddings."
        orphanLabel="Orphan sequences"
        orphanValue={seqPreview?.orphan_sequences ?? null}
        totalValue={seqPreview?.total_sequences ?? null}
        stats={
          <>
            <StatRow label="Total sequences" value={seqPreview?.total_sequences ?? null} />
            <StatRow label="Referenced sequences" value={seqPreview?.referenced_sequences ?? null} />
            <StatRow label="Orphan sequences" value={seqPreview?.orphan_sequences ?? null} highlight />
          </>
        }
        onPreview={loadSeqPreview}
        onVacuum={doVacuumSequences}
        loading={seqLoading}
        vacuuming={seqVacuuming}
      />

      <VacuumCard
        title="Unindexed Embeddings"
        description="Embeddings for sequences not in the protein reference database (e.g. query proteins after prediction). Safe to delete once predictions have been generated."
        orphanLabel="Unindexed embeddings"
        orphanValue={embPreview?.unindexed_embeddings ?? null}
        totalValue={embPreview?.total_embeddings ?? null}
        stats={
          <>
            <StatRow label="Total embeddings" value={embPreview?.total_embeddings ?? null} />
            <StatRow label="Indexed embeddings" value={embPreview?.indexed_embeddings ?? null} />
            <StatRow label="Unindexed embeddings" value={embPreview?.unindexed_embeddings ?? null} highlight />
          </>
        }
        onPreview={loadEmbPreview}
        onVacuum={doVacuumEmbeddings}
        loading={embLoading}
        vacuuming={embVacuuming}
      />
    </main>
  );
}
