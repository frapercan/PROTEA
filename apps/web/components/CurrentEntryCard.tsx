"use client";

// The protein as UniProt describes it today, and which release says so.
//
// The metadata a protein page shows is its current UniProt entry: knowledge from
// after every cut the platform evaluates on. The card names the release that
// served it and says that it is shown, never used to predict or evaluate on an
// earlier cut, so nobody reads it as what was known then.

import { useTranslations } from "next-intl";

import type { CurrentEntry } from "@/lib/api";
import { formatCount } from "@/lib/format";

function Row({ label, value }: { label: string; value?: string | null }) {
  if (!value) return null;
  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-slate-600 mb-1">{label}</p>
      <p className="text-sm text-slate-800 whitespace-pre-wrap">{value}</p>
    </div>
  );
}

export function CurrentEntryCard({ entry }: { entry: CurrentEntry }) {
  const t = useTranslations("proteinDetail.overviewTab");
  const note = entry.uniprot_release
    ? t("currentEntryNote", { release: entry.uniprot_release, date: entry.uniprot_release_date ?? "" })
    : t("unknownRelease");
  return (
    <div className="rounded-lg border bg-white p-4 shadow-sm space-y-3">
      <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">{t("currentEntry")}</p>
      <p className="text-xs text-slate-500" role="note">
        {note}
      </p>
      <Row label={t("entryName")} value={entry.entry_name} />
      <Row label={t("proteinNames")} value={entry.protein_names} />
      <Row label={t("gene")} value={entry.gene_names} />
      <Row label={t("organism")} value={entry.organism_name} />
      <Row label={t("taxonId")} value={entry.organism_id != null ? String(entry.organism_id) : null} />
      <Row
        label={t("length")}
        value={entry.length != null ? `${formatCount(entry.length)} ${t("aa")}` : null}
      />
      <Row
        label={t("reviewedEntry")}
        value={entry.reviewed == null ? null : entry.reviewed ? t("yes") : t("no")}
      />
    </div>
  );
}
