"use client";

import Link from "next/link";
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
import { useLocale, useTranslations } from "next-intl";
import { useHasRole, useRole } from "@/lib/useRole";

function StatRow({ label, value, highlight }: { label: string; value: number | null; highlight?: boolean }) {
  return (
    <div className="flex justify-between items-center py-1.5 border-b border-slate-100 last:border-0">
      <span className="text-sm text-slate-600">{label}</span>
      <span className={`text-sm font-mono font-semibold ${highlight && value ? "text-amber-600" : "text-slate-800"}`}>
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
    <div className="border border-slate-200 rounded-lg p-5 bg-white shadow-sm">
      <div className="flex items-start justify-between gap-4 mb-3">
        <div>
          <h2 className="font-semibold text-slate-900">{title}</h2>
          <p className="text-xs text-slate-500 mt-0.5">{description}</p>
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
          <div className="flex justify-between text-xs text-slate-600 mb-1">
            <span>{orphanLabel}</span>
            <span>{pct}%</span>
          </div>
          <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
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
          className="px-3 py-1.5 text-sm border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-50 transition-colors"
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
  const tToast = useTranslations("toasts");
  const toast = useToast();
  const locale = useLocale();

  // UX-ADMIN-AUDIT P0-MNT-2 (2026-05-26): vacuum operations are
  // destructive (drop orphan sequences + unindexed embeddings). The
  // middleware already gates this route to ``operator``, but the
  // audit asks for an explicit admin client gate so viewer and
  // operator users see a 403 panel instead of the controls. Backend
  // endpoints (POST /v1/admin/maintenance/*) gate by role as well
  // per the auth contract.
  const role = useRole();
  const isAdmin = useHasRole("admin");
  const [hydrated, setHydrated] = useState(false);
  useEffect(() => {
    setHydrated(true);
  }, []);

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
      toast(e.message ?? tToast("loadSequenceStatsFailed"), "error");
    } finally {
      setSeqLoading(false);
    }
  }

  async function loadEmbPreview() {
    setEmbLoading(true);
    try {
      setEmbPreview(await previewVacuumEmbeddings());
    } catch (e: any) {
      toast(e.message ?? tToast("loadEmbeddingStatsFailed"), "error");
    } finally {
      setEmbLoading(false);
    }
  }

  async function doVacuumSequences() {
    setSeqVacuuming(true);
    try {
      const r = await runVacuumSequences();
      toast(tToast("sequencesVacuumed", { count: r.deleted_sequences.toLocaleString() }), "success");
      await loadSeqPreview();
      await loadEmbPreview(); // seq deletion cascades to embeddings
    } catch (e: any) {
      toast(e.message ?? tToast("vacuumFailed"), "error");
    } finally {
      setSeqVacuuming(false);
    }
  }

  async function doVacuumEmbeddings() {
    setEmbVacuuming(true);
    try {
      const r = await runVacuumEmbeddings();
      toast(tToast("embeddingsVacuumed", { count: r.deleted_embeddings.toLocaleString() }), "success");
      await loadEmbPreview();
    } catch (e: any) {
      toast(e.message ?? tToast("vacuumFailed"), "error");
    } finally {
      setEmbVacuuming(false);
    }
  }

  useEffect(() => {
    // Only fetch previews after hydration confirms an admin session.
    // Otherwise the vacuum endpoints would 403 the viewer's first paint
    // and clutter the console with auth errors.
    if (!hydrated || !isAdmin) return;
    async function init() {
      setSeqLoading(true);
      setEmbLoading(true);
      // Fire both previews in parallel. They are independent endpoints.
      const [seqRes, embRes] = await Promise.allSettled([
        previewVacuumSequences(),
        previewVacuumEmbeddings(),
      ]);
      if (seqRes.status === "fulfilled") setSeqPreview(seqRes.value);
      setSeqLoading(false);
      if (embRes.status === "fulfilled") setEmbPreview(embRes.value);
      setEmbLoading(false);
    }
    init();
  }, [hydrated, isAdmin]);

  if (!hydrated) {
    return (
      <main
        className="max-w-2xl mx-auto px-4 py-8 space-y-4"
        aria-busy
        data-testid="maintenance-loading"
      >
        <div className="h-7 w-48 animate-pulse rounded bg-slate-200" />
        <div className="h-4 w-80 animate-pulse rounded bg-slate-200/70" />
        <div className="h-48 animate-pulse rounded-xl bg-slate-100" />
      </main>
    );
  }

  if (!isAdmin) {
    return <ForbiddenPanel locale={locale} role={role} />;
  }

  return (
    <main className="max-w-2xl mx-auto px-4 py-8 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">{t("title")}</h1>
        <p className="text-sm text-slate-500 mt-1">{t("description")}</p>
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

function ForbiddenPanel({ locale, role }: { locale: string; role: string }) {
  // UX-ADMIN-AUDIT P0-MNT-2: mirrors the api-keys / dlq ForbiddenPanel
  // shape so non-admins consistently land on a "Admin only" message
  // with a Back-home affordance, instead of being shown vacuum buttons
  // that will 403 on click.
  const t = useTranslations("maintenance.forbidden");
  return (
    <main
      className="mx-auto max-w-xl px-4 sm:px-6 py-12"
      data-testid="maintenance-forbidden"
    >
      <div
        role="alert"
        className="rounded-xl border border-slate-200 bg-white p-6 text-center shadow-sm"
      >
        <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-slate-100 text-slate-600">
          <svg className="h-6 w-6" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24" aria-hidden>
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
          </svg>
        </div>
        <h1 className="text-lg font-semibold text-slate-900">{t("title")}</h1>
        <p className="mt-1 text-sm text-slate-600">{t("body")}</p>
        <p className="mt-2 text-xs text-slate-500">
          {t("roleLabel")}: <span className="font-mono">{role}</span>
        </p>
        <Link
          href={`/${locale}`}
          className="mt-5 inline-flex items-center gap-1.5 rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800"
        >
          {t("backHome")}
        </Link>
      </div>
    </main>
  );
}

