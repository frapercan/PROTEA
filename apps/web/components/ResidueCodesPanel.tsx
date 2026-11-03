"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";

import { AwaitingData } from "@/components/AwaitingData";
import { errorText, listResidueCodes, type ResidueCodeSummary } from "@/lib/api";
import { formatCount, formatDateTime, formatDecimal } from "@/lib/format";

/**
 * How far each residue vocabulary's per-residue codes reach (ADR-D51).
 *
 * WHY THIS EXISTS. A corpus pass of `encode_residue_vocabulary` runs for most
 * of a day on the GPU node, and the job that started it is the wrong place to
 * watch it: on 2026-11-03 a coordinator that ran the code from before #1091
 * closed SUCCEEDED two seconds after it started, and the jobs page drew a full
 * green bar over a pass at 22 %. The index is the only place that knows how
 * much of the corpus has a code. This panel reads it.
 *
 * WHAT IS STORED, said on the page because it is the first question a reader
 * asks: the code of every residue lives in the artifact store as compressed
 * shards, one per GPU batch; the database holds one index row per sequence and
 * vocabulary pointing into them; nothing is pooled per protein. A
 * per-protein representation, when one is needed, is the output of a learned
 * aggregator over these codes, not a stored max or mean.
 *
 * It computes nothing: every count, ratio and estimate comes from the
 * endpoint, so the page and the API cannot disagree. It polls every 30
 * seconds, which is the server's own cache window.
 */

const POLL_MS = 30_000;

/** The house placeholder for a missing number, the one formatCount already prints. */
const NONE = formatCount(null);

function percent(x: number | null): string {
  return x == null ? NONE : `${(x * 100).toFixed(1)} %`;
}

function hours(h: number | null): string {
  if (h == null) return NONE;
  if (h < 1) return `${Math.max(1, Math.round(h * 60))} min`;
  return `${formatDecimal(Math.round(h * 10) / 10)} h`;
}

function Stat({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="rounded-md border border-slate-200 bg-white px-3 py-2" title={hint}>
      <dt className="text-xs text-slate-500">{label}</dt>
      <dd className="mt-0.5 text-sm font-semibold tabular-nums text-slate-900">{value}</dd>
    </div>
  );
}

function CoverageBar({ coverage, label }: { coverage: number | null; label: string }) {
  const pct = coverage == null ? 0 : Math.min(100, Math.max(0, coverage * 100));
  return (
    <div
      role="progressbar"
      aria-label={label}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={coverage == null ? undefined : Math.round(pct)}
      className="h-2.5 w-full overflow-hidden rounded-full bg-slate-100"
    >
      <div
        className={`h-2.5 rounded-full transition-all duration-700 ${pct >= 100 ? "bg-emerald-500" : "bg-blue-500"}`}
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}

function VocabularyCard({ s }: { s: ResidueCodeSummary }) {
  const t = useTranslations("embeddings.residueTab");
  const locale = useLocale();
  const total = s.source?.sequences ?? null;
  const finished = s.coverage != null && s.coverage >= 1;
  return (
    <article className="rounded-lg border border-slate-200 bg-slate-50 p-4 sm:p-5">
      <header className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <h3 className="break-all font-mono text-sm font-semibold text-slate-900">{s.name}</h3>
        <span className="text-xs text-slate-500">
          {t("layers", { layers: s.layer_indices.join(", ") })}
        </span>
        {s.source && (
          <span className="text-xs text-slate-500">
            {t("from", { source: s.source.name ?? s.source.embedding_config_id.slice(0, 8) })}
          </span>
        )}
      </header>
      {s.description && <p className="mt-1 text-xs leading-relaxed text-slate-600">{s.description}</p>}

      <div className="mt-4">
        <div className="mb-1.5 flex flex-wrap items-baseline justify-between gap-2 text-sm">
          <span className="text-slate-700">
            {total == null
              ? t("sequencesNoTotal", { done: formatCount(s.index.sequences) })
              : t("sequencesOfTotal", { done: formatCount(s.index.sequences), total: formatCount(total) })}
          </span>
          <span className="font-semibold tabular-nums text-slate-900">{percent(s.coverage)}</span>
        </div>
        <CoverageBar coverage={s.coverage} label={t("coverageLabel", { name: s.name })} />
      </div>

      <dl className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <Stat label={t("residues")} value={formatCount(s.index.residues)} />
        <Stat label={t("entries")} value={formatCount(s.index.entries)} hint={t("entriesHint")} />
        <Stat label={t("shards")} value={formatCount(s.index.shards)} hint={t("shardsHint")} />
        <Stat label={t("lastWrite")} value={formatDateTime(s.index.last_written_at)} />
        <Stat
          label={t("pace")}
          value={t("paceValue", { n: formatCount(s.index.sequences_last_hour) })}
          hint={t("paceHint")}
        />
        <Stat label={t("remaining")} value={formatCount(s.remaining)} />
        <Stat
          label={t("eta")}
          value={finished ? t("done") : hours(s.eta_hours)}
          hint={t("etaHint")}
        />
        <Stat label={t("firstWrite")} value={formatDateTime(s.index.first_written_at)} />
      </dl>

      {s.job && (
        <p className="mt-3 text-xs text-slate-600">
          {t("job")}{" "}
          <Link
            href={`/${locale}/instrument/jobs/${s.job.id}`}
            className="font-mono text-blue-700 underline hover:text-blue-900"
          >
            {s.job.id.slice(0, 8)}
          </Link>{" "}
          · {s.job.status}
          {s.job.progress_total != null &&
            ` · ${t("batches", {
              done: formatCount(s.job.progress_current ?? 0),
              total: formatCount(s.job.progress_total),
            })}`}
        </p>
      )}
    </article>
  );
}

export function ResidueCodesPanel() {
  const t = useTranslations("embeddings.residueTab");
  const [rows, setRows] = useState<ResidueCodeSummary[] | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let alive = true;
    const load = () =>
      listResidueCodes()
        .then((r) => {
          if (!alive) return;
          setRows(r);
          setError("");
        })
        .catch((e) => {
          if (alive) setError(errorText(e));
        });
    load();
    const timer = setInterval(load, POLL_MS);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  return (
    <section aria-labelledby="residue-codes-heading" className="space-y-4">
      <div>
        <h2 id="residue-codes-heading" className="text-base font-semibold text-slate-900">
          {t("title")}
        </h2>
        <p className="mt-1 max-w-prose text-sm leading-relaxed text-slate-600">{t("intro")}</p>
      </div>

      {error && (
        <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </pre>
      )}

      {rows === null && !error && <p className="text-sm text-slate-500">{t("loading")}</p>}

      {rows !== null && rows.length === 0 && (
        <AwaitingData title={t("emptyTitle")} body={t("emptyBody")} />
      )}

      {rows !== null && rows.length > 0 && (
        <div className="space-y-4">
          {rows.map((s) => (
            <VocabularyCard key={s.embedding_config_id} s={s} />
          ))}
          <p className="text-xs text-slate-500">{t("footnote")}</p>
        </div>
      )}
    </section>
  );
}
