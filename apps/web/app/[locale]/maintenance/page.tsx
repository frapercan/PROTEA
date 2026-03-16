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
import { useTranslations } from "next-intl";

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
  labelClean,
  labelToClean,
  labelRefresh,
  labelVacuum,
  labelCleaning,
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
  labelClean: string;
  labelToClean: string;
  labelRefresh: string;
  labelVacuum: string;
  labelCleaning: string;
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
            {hasOrphans ? labelToClean : labelClean}
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
          {loading ? "Loading…" : labelRefresh}
        </button>
        <button
          onClick={onVacuum}
          disabled={vacuuming || !hasOrphans}
          className="px-3 py-1.5 text-sm bg-amber-500 text-white rounded hover:bg-amber-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          {vacuuming ? labelCleaning : `${labelVacuum} ${orphanValue !== null ? `(${orphanValue.toLocaleString()})` : ""}`}
        </button>
      </div>
    </div>
  );
}

export default function MaintenancePage() {
  const t = useTranslations("maintenance");
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
        <h1 className="text-2xl font-bold text-gray-900">{t("title")}</h1>
        <p className="text-sm text-gray-500 mt-1">{t("description")}</p>
      </div>

      <VacuumCard
        title={t("orphanSequences.title")}
        description={t("orphanSequences.description")}
        orphanLabel={t("orphanSequences.orphanLabel")}
        orphanValue={seqPreview?.orphan_sequences ?? null}
        totalValue={seqPreview?.total_sequences ?? null}
        labelClean={t("orphanSequences.clean")}
        labelToClean={t("orphanSequences.toClean", { count: seqPreview?.orphan_sequences ?? 0 })}
        labelRefresh={t("orphanSequences.refresh")}
        labelVacuum={t("orphanSequences.vacuum")}
        labelCleaning={t("unindexedEmbeddings.cleaning")}
        stats={
          <>
            <StatRow label={t("orphanSequences.totalSequences")} value={seqPreview?.total_sequences ?? null} />
            <StatRow label={t("orphanSequences.referencedSequences")} value={seqPreview?.referenced_sequences ?? null} />
            <StatRow label={t("orphanSequences.orphanLabel")} value={seqPreview?.orphan_sequences ?? null} highlight />
          </>
        }
        onPreview={loadSeqPreview}
        onVacuum={doVacuumSequences}
        loading={seqLoading}
        vacuuming={seqVacuuming}
      />

      <VacuumCard
        title={t("unindexedEmbeddings.title")}
        description={t("unindexedEmbeddings.description")}
        orphanLabel={t("unindexedEmbeddings.orphanLabel")}
        orphanValue={embPreview?.unindexed_embeddings ?? null}
        totalValue={embPreview?.total_embeddings ?? null}
        labelClean={t("unindexedEmbeddings.clean")}
        labelToClean={t("unindexedEmbeddings.toClean", { count: embPreview?.unindexed_embeddings ?? 0 })}
        labelRefresh={t("unindexedEmbeddings.refresh")}
        labelVacuum={t("unindexedEmbeddings.vacuum")}
        labelCleaning={t("unindexedEmbeddings.cleaning")}
        stats={
          <>
            <StatRow label={t("unindexedEmbeddings.totalEmbeddings")} value={embPreview?.total_embeddings ?? null} />
            <StatRow label={t("unindexedEmbeddings.indexedEmbeddings")} value={embPreview?.indexed_embeddings ?? null} />
            <StatRow label={t("unindexedEmbeddings.orphanLabel")} value={embPreview?.unindexed_embeddings ?? null} highlight />
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
