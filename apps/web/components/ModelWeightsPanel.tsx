"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";

import { AwaitingData } from "@/components/AwaitingData";
import { OperatorOnlyNotice, useMayLaunchJobs } from "@/components/OperatorOnly";
import { EncodeCorpusDialog, RegisterWeightsDialog, UploadWeightsDialog } from "@/components/WeightsDialogs";
import {
  errorText,
  getModelWeights,
  listModelWeights,
  WEIGHT_KINDS,
  type ModelWeights,
  type ModelWeightsDetail,
  type WeightKind,
} from "@/lib/api";
import { formatCount, formatDateTime, formatDecimal } from "@/lib/format";

/**
 * The registry of model weights (ADR-D54): every file of fitted weights the platform
 * can use, by kind, with its hash and what uses it.
 *
 * WHY THIS EXISTS. The lab's weights reached the platform as store keys typed into
 * payloads, and nothing listed them. The author, 2026-11-03: "los pesos almacenados y
 * listado que se puedan subir... parecido a las representaciones aprendidas o mejor".
 * So a weight here is identified by its sha256, says what it was fitted on, lists the
 * configurations made from it and the jobs that read it, and offers the operation that
 * uses it. Uploads and registrations are for operators.
 */

function bytes(n: number): string {
  if (n >= 1024 ** 3) return `${formatDecimal(Math.round((n / 1024 ** 3) * 10) / 10)} GB`;
  if (n >= 1024 ** 2) return `${formatDecimal(Math.round((n / 1024 ** 2) * 10) / 10)} MB`;
  if (n >= 1024) return `${formatDecimal(Math.round((n / 1024) * 10) / 10)} KB`;
  return `${formatCount(n)} B`;
}

function MetaTable({ meta }: { meta: Record<string, unknown> }) {
  const entries = Object.entries(meta);
  if (!entries.length) return null;
  return (
    <dl className="grid grid-cols-1 gap-x-4 gap-y-1 text-xs sm:grid-cols-[max-content_1fr]">
      {entries.map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="font-medium text-slate-500">{k}</dt>
          <dd className="break-all font-mono text-slate-800">{typeof v === "string" ? v : JSON.stringify(v)}</dd>
        </div>
      ))}
    </dl>
  );
}

function Detail({ id, onEncode }: { id: string; onEncode: (w: ModelWeights) => void }) {
  const t = useTranslations("weights");
  const locale = useLocale();
  const mayLaunch = useMayLaunchJobs();
  const [d, setD] = useState<ModelWeightsDetail | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    getModelWeights(id)
      .then(setD)
      .catch((e) => setError(errorText(e)));
  }, [id]);
  if (error) return <p className="text-sm text-red-700">{error}</p>;
  if (!d) return <p className="text-sm text-slate-500">{t("loading")}</p>;
  return (
    <div className="space-y-4 rounded-b-lg border-x border-b border-slate-200 bg-white p-4">
      {d.description && <p className="text-sm text-slate-700">{d.description}</p>}
      <dl className="grid grid-cols-1 gap-x-4 gap-y-1 text-xs sm:grid-cols-[max-content_1fr]">
        <dt className="font-medium text-slate-500">{t("detail.uri")}</dt>
        <dd className="break-all font-mono text-slate-800">{d.uri}</dd>
        <dt className="font-medium text-slate-500">sha256</dt>
        <dd className="break-all font-mono text-slate-800">{d.sha256}</dd>
        <dt className="font-medium text-slate-500">{t("detail.format")}</dt>
        <dd className="text-slate-800">{d.format}</dd>
        <dt className="font-medium text-slate-500">{t("detail.trainedOn")}</dt>
        <dd className="font-mono text-slate-800">{d.trained_on_annotation_set_id ?? t("detail.noCut")}</dd>
        <dt className="font-medium text-slate-500">{t("detail.registered")}</dt>
        <dd className="text-slate-800">
          {formatDateTime(d.created_at)}
          {d.created_by ? ` · ${d.created_by}` : ""}
        </dd>
      </dl>
      <MetaTable meta={d.meta} />
      <div className="grid gap-4 sm:grid-cols-2">
        <section>
          <h4 className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
            {t("detail.configurations")}
          </h4>
          {d.used_by.configurations.length === 0 ? (
            <p className="text-xs text-slate-500">{t("detail.noConfigurations")}</p>
          ) : (
            <ul className="space-y-1 text-xs">
              {d.used_by.configurations.map((c) => (
                <li key={c.id}>
                  <Link href={`/${locale}/instrument/embeddings`} className="font-mono text-blue-700 underline">
                    {c.name}
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </section>
        <section>
          <h4 className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">{t("detail.jobs")}</h4>
          {d.used_by.jobs.length === 0 ? (
            <p className="text-xs text-slate-500">{t("detail.noJobs")}</p>
          ) : (
            <ul className="space-y-1 text-xs">
              {d.used_by.jobs.map((j) => (
                <li key={j.id}>
                  <Link href={`/${locale}/instrument/jobs/${j.id}`} className="font-mono text-blue-700 underline">
                    {j.id.slice(0, 8)}
                  </Link>{" "}
                  {j.operation} · {j.status} · {formatDateTime(j.created_at)}
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
      {d.kind === "residue_vocabulary" &&
        (mayLaunch ? (
          <button
            type="button"
            onClick={() => onEncode(d)}
            className="rounded-md bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700"
          >
            {t("actions.encode")}
          </button>
        ) : (
          <OperatorOnlyNotice />
        ))}
    </div>
  );
}

export function ModelWeightsPanel() {
  const t = useTranslations("weights");
  const locale = useLocale();
  const mayLaunch = useMayLaunchJobs();
  const [rows, setRows] = useState<ModelWeights[] | null>(null);
  const [error, setError] = useState("");
  const [kind, setKind] = useState<WeightKind | "all">("all");
  const [open, setOpen] = useState<string | null>(null);
  const [dialog, setDialog] = useState<"upload" | "register" | null>(null);
  const [encoding, setEncoding] = useState<ModelWeights | null>(null);

  const load = useCallback(() => {
    listModelWeights()
      .then((r) => {
        setRows(r);
        setError("");
      })
      .catch((e) => setError(errorText(e)));
  }, []);
  useEffect(load, [load]);

  const counts = Object.fromEntries(WEIGHT_KINDS.map((k) => [k, (rows ?? []).filter((r) => r.kind === k).length]));
  const shown = (rows ?? []).filter((r) => kind === "all" || r.kind === kind);
  const done = (w: ModelWeights) => {
    setDialog(null);
    setOpen(w.id);
    load();
  };

  return (
    <section className="space-y-4">
      <div className="flex flex-wrap items-start gap-3">
        <p className="max-w-prose flex-1 text-sm leading-relaxed text-slate-600">
          {t("intro")}{" "}
          <Link href={`/${locale}/instrument/reranker`} className="text-blue-700 underline">
            {t("rerankerLink")}
          </Link>
        </p>
        {mayLaunch ? (
          <div className="flex gap-2">
            <button
              type="button"
              onClick={() => setDialog("register")}
              className="rounded-md border border-slate-300 px-3 py-1.5 text-sm font-medium hover:bg-slate-50"
            >
              {t("register.open")}
            </button>
            <button
              type="button"
              onClick={() => setDialog("upload")}
              className="rounded-md bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700"
            >
              {t("upload.open")}
            </button>
          </div>
        ) : (
          <OperatorOnlyNotice />
        )}
      </div>

      <div role="tablist" aria-label={t("kindFilter")} className="flex flex-wrap gap-1 border-b">
        {(["all", ...WEIGHT_KINDS] as const).map((k) => (
          <button
            key={k}
            role="tab"
            aria-selected={kind === k}
            onClick={() => setKind(k)}
            className={`border-b-2 px-3 py-1.5 text-sm ${
              kind === k ? "border-blue-600 text-blue-700" : "border-transparent text-slate-500 hover:text-slate-700"
            }`}
          >
            {k === "all" ? t("all") : t(`kinds.${k}`)}
            <span className="ml-1 text-xs tabular-nums text-slate-400">
              {k === "all" ? (rows ?? []).length : counts[k]}
            </span>
          </button>
        ))}
      </div>

      {error && <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">{error}</pre>}
      {rows === null && !error && <p className="text-sm text-slate-500">{t("loading")}</p>}
      {rows !== null && rows.length === 0 && <AwaitingData title={t("emptyTitle")} body={t("emptyBody")} />}

      {shown.length > 0 && (
        <ul className="space-y-2">
          {shown.map((w) => (
            <li key={w.id}>
              <button
                type="button"
                aria-expanded={open === w.id}
                onClick={() => setOpen(open === w.id ? null : w.id)}
                className={`flex w-full flex-wrap items-baseline gap-x-4 gap-y-1 border border-slate-200 bg-slate-50 px-4 py-3 text-left hover:bg-slate-100 ${
                  open === w.id ? "rounded-t-lg" : "rounded-lg"
                }`}
              >
                <span className="font-mono text-sm font-semibold text-slate-900">{w.name}</span>
                <span className="rounded-full bg-white px-2 py-0.5 text-xs text-slate-600 ring-1 ring-slate-200">
                  {t(`kinds.${w.kind}`)}
                </span>
                <span className="text-xs tabular-nums text-slate-600">{bytes(w.size_bytes)}</span>
                <span className="font-mono text-xs text-slate-500">{w.sha256.slice(0, 16)}</span>
                <span className="text-xs text-slate-500">{formatDateTime(w.created_at)}</span>
                <span className="ml-auto text-xs text-slate-600">
                  {t("configurationsCount", { n: w.configurations ?? 0 })}
                </span>
              </button>
              {open === w.id && <Detail id={w.id} onEncode={setEncoding} />}
            </li>
          ))}
        </ul>
      )}

      {dialog === "upload" && <UploadWeightsDialog onClose={() => setDialog(null)} onDone={done} />}
      {dialog === "register" && <RegisterWeightsDialog onClose={() => setDialog(null)} onDone={done} />}
      {encoding && <EncodeCorpusDialog weights={encoding} onClose={() => setEncoding(null)} />}
    </section>
  );
}
