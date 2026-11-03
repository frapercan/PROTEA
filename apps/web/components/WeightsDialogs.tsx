"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";

import {
  createJob,
  errorText,
  listEmbeddingConfigs,
  listJobs,
  listResidueCodes,
  registerModelWeightsByReference,
  uploadModelWeights,
  WEIGHT_KINDS,
  type EmbeddingConfig,
  type Job,
  type ModelWeights,
  type WeightKind,
} from "@/lib/api";

/**
 * The three dialogs of the model weights page (ADR-D54): upload a file in parts,
 * register a file already in the artifact store, and encode a corpus with a residue
 * vocabulary. Every one of them is for operators; the page only opens them for one.
 */

const NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$/;

/** The coordinators whose batches run on the card (protea.core.operations._card_guard). */
const CARD_OPERATIONS = ["compute_embeddings", "encode_residue_vocabulary", "encode_residue_sparse"];

/**
 * What holds the card now: coordinators of a card operation in RUNNING, and residue
 * vocabularies that wrote codes in the last hour (a pass whose coordinator closed when it
 * dispatched is SUCCEEDED while its batches still run, and only the index shows it).
 */
function useCardBusy() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [writing, setWriting] = useState<string[]>([]);
  useEffect(() => {
    Promise.all(CARD_OPERATIONS.map((operation) => listJobs({ status: "RUNNING", operation, limit: 5 })))
      .then((lists) => setJobs(lists.flat()))
      .catch(() => setJobs([]));
    listResidueCodes()
      .then((rows) => setWriting(rows.filter((r) => r.index.sequences_last_hour > 0).map((r) => r.name)))
      .catch(() => setWriting([]));
  }, []);
  return { jobs, writing, busy: jobs.length > 0 || writing.length > 0 };
}
const SHA256 = /^[0-9a-f]{64}$/;

const input =
  "w-full rounded-md border border-slate-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
const label = "mb-1 block text-sm font-medium text-slate-700";

function Shell({
  id,
  title,
  subtitle,
  onClose,
  children,
}: {
  id: string;
  title: string;
  subtitle: string;
  onClose: () => void;
  children: React.ReactNode;
}) {
  const t = useTranslations("weights");
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={onClose}>
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={id}
        className="flex max-h-[92vh] w-full max-w-2xl flex-col rounded-xl border bg-white shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4 border-b px-5 py-4">
          <div>
            <h2 id={id} className="text-base font-semibold text-slate-900">
              {title}
            </h2>
            <p className="mt-0.5 text-xs leading-relaxed text-slate-500">{subtitle}</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label={t("close")}
            className="text-xl leading-none text-slate-500 hover:text-slate-900"
          >
            ×
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-5">{children}</div>
      </div>
    </div>
  );
}

/** Kind, name, description, metadata and cut: what the caller says about a weight. */
function useInfoFields(initialName = "") {
  const [kind, setKind] = useState<WeightKind>("residue_vocabulary");
  const [name, setName] = useState(initialName);
  const [description, setDescription] = useState("");
  const [meta, setMeta] = useState("");
  const [trainedOn, setTrainedOn] = useState("");
  return { kind, setKind, name, setName, description, setDescription, meta, setMeta, trainedOn, setTrainedOn };
}

type InfoFields = ReturnType<typeof useInfoFields>;

function InfoForm({ f, idPrefix }: { f: InfoFields; idPrefix: string }) {
  const t = useTranslations("weights");
  return (
    <>
      <div className="grid gap-4 sm:grid-cols-2">
        <div>
          <label className={label} htmlFor={`${idPrefix}-kind`}>
            {t("form.kind")}
          </label>
          <select
            id={`${idPrefix}-kind`}
            className={input}
            value={f.kind}
            onChange={(e) => f.setKind(e.target.value as WeightKind)}
          >
            {WEIGHT_KINDS.map((k) => (
              <option key={k} value={k}>
                {t(`kinds.${k}`)}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className={label} htmlFor={`${idPrefix}-name`}>
            {t("form.name")}
          </label>
          <input
            id={`${idPrefix}-name`}
            className={input}
            value={f.name}
            onChange={(e) => f.setName(e.target.value)}
            placeholder="vocabulary-v1"
          />
          <p className="mt-1 text-xs text-slate-500">{t("form.nameHint")}</p>
        </div>
      </div>
      <div>
        <label className={label} htmlFor={`${idPrefix}-description`}>
          {t("form.description")}
        </label>
        <input
          id={`${idPrefix}-description`}
          className={input}
          value={f.description}
          onChange={(e) => f.setDescription(e.target.value)}
        />
      </div>
      <div className="grid gap-4 sm:grid-cols-2">
        <div>
          <label className={label} htmlFor={`${idPrefix}-meta`}>
            {t("form.meta")}
          </label>
          <textarea
            id={`${idPrefix}-meta`}
            className={`${input} font-mono text-xs`}
            rows={4}
            value={f.meta}
            onChange={(e) => f.setMeta(e.target.value)}
            placeholder='{"layers": [2, 10, 22, 40, 48], "dict_dim": 8192, "actives": 128}'
          />
          <p className="mt-1 text-xs text-slate-500">{t("form.metaHint")}</p>
        </div>
        <div>
          <label className={label} htmlFor={`${idPrefix}-trained`}>
            {t("form.trainedOn")}
          </label>
          <input
            id={`${idPrefix}-trained`}
            className={`${input} font-mono text-xs`}
            value={f.trainedOn}
            onChange={(e) => f.setTrainedOn(e.target.value)}
            placeholder="00000000-0000-0000-0000-000000000000"
          />
          <p className="mt-1 text-xs text-slate-500">{t("form.trainedOnHint")}</p>
        </div>
      </div>
    </>
  );
}

/** The info body, or the key of the message that says why it is not valid. */
function infoBody(f: InfoFields): { body?: Parameters<typeof uploadModelWeights>[1]; error?: string } {
  if (!NAME.test(f.name)) return { error: "form.nameInvalid" };
  let meta: Record<string, unknown> = {};
  if (f.meta.trim()) {
    try {
      const parsed: unknown = JSON.parse(f.meta);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { error: "form.metaInvalid" };
      meta = parsed as Record<string, unknown>;
    } catch {
      return { error: "form.metaInvalid" };
    }
  }
  return {
    body: {
      kind: f.kind,
      name: f.name,
      description: f.description.trim() || null,
      meta,
      trained_on_annotation_set_id: f.trainedOn.trim() || null,
    },
  };
}

function stemOf(filename: string): string {
  const base = filename.replace(/\.(tar\.gz|tgz|npz|npy|pt|pth|safetensors|onnx|json|tar)$/i, "");
  return base.replace(/[^A-Za-z0-9._-]/g, "-").replace(/^[^A-Za-z0-9]+/, "").slice(0, 120);
}

export function UploadWeightsDialog({
  onClose,
  onDone,
}: {
  onClose: () => void;
  onDone: (w: ModelWeights) => void;
}) {
  const t = useTranslations("weights");
  const f = useInfoFields();
  const [file, setFile] = useState<File | null>(null);
  const [progress, setProgress] = useState<{ sent: number; parts: number } | null>(null);
  const [error, setError] = useState("");
  const busy = progress !== null && !error;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    if (!file) return setError(t("upload.noFile"));
    const { body, error: why } = infoBody(f);
    if (!body) return setError(t(why as string));
    setProgress({ sent: 0, parts: 1 });
    try {
      const w = await uploadModelWeights(file, body, (sent, parts) => setProgress({ sent, parts }));
      onDone(w);
    } catch (err) {
      setError(errorText(err));
    }
  }

  return (
    <Shell id="weights-upload-title" title={t("upload.title")} subtitle={t("upload.subtitle")} onClose={onClose}>
      <form onSubmit={submit} className="space-y-4">
        <div>
          <label className={label} htmlFor="wu-file">
            {t("upload.file")}
          </label>
          <input
            id="wu-file"
            type="file"
            className="block w-full text-sm"
            onChange={(e) => {
              const picked = e.target.files?.[0] ?? null;
              setFile(picked);
              if (picked && !f.name) f.setName(stemOf(picked.name));
            }}
          />
        </div>
        <InfoForm f={f} idPrefix="wu" />
        {progress && (
          <div>
            <div
              role="progressbar"
              aria-label={t("upload.progressLabel")}
              aria-valuemin={0}
              aria-valuemax={progress.parts}
              aria-valuenow={progress.sent}
              className="h-2 w-full overflow-hidden rounded-full bg-slate-100"
            >
              <div
                className="h-2 rounded-full bg-blue-500 transition-all"
                style={{ width: `${(100 * progress.sent) / Math.max(progress.parts, 1)}%` }}
              />
            </div>
            <p className="mt-1 text-xs text-slate-600">
              {t("upload.sending", { sent: progress.sent, parts: progress.parts })}
            </p>
          </div>
        )}
        {error && <p className="rounded-md border border-red-200 bg-red-50 p-2 text-sm text-red-700">{error}</p>}
        <div className="flex justify-end gap-2">
          <button type="button" onClick={onClose} className="rounded-md border px-3 py-1.5 text-sm">
            {t("cancel")}
          </button>
          <button
            type="submit"
            disabled={busy}
            className="rounded-md bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          >
            {t("upload.submit")}
          </button>
        </div>
      </form>
    </Shell>
  );
}

export function RegisterWeightsDialog({
  onClose,
  onDone,
}: {
  onClose: () => void;
  onDone: (w: ModelWeights) => void;
}) {
  const t = useTranslations("weights");
  const f = useInfoFields();
  const [uri, setUri] = useState("");
  const [sha, setSha] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    const { body, error: why } = infoBody(f);
    if (!body) return setError(t(why as string));
    const digest = sha.trim().toLowerCase();
    if (!SHA256.test(digest)) return setError(t("register.shaInvalid"));
    setBusy(true);
    try {
      onDone(await registerModelWeightsByReference({ ...body, uri: uri.trim(), sha256: digest }));
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Shell
      id="weights-register-title"
      title={t("register.title")}
      subtitle={t("register.subtitle")}
      onClose={onClose}
    >
      <form onSubmit={submit} className="space-y-4">
        <div>
          <label className={label} htmlFor="wr-uri">
            {t("register.uri")}
          </label>
          <input
            id="wr-uri"
            className={`${input} font-mono text-xs`}
            value={uri}
            onChange={(e) => {
              setUri(e.target.value);
              if (!f.name) f.setName(stemOf(e.target.value.split("/").pop() ?? ""));
            }}
            placeholder="residue-vocabulary/vocabulary-v1-06fed75ca76d17be.npz"
          />
        </div>
        <div>
          <label className={label} htmlFor="wr-sha">
            {t("register.sha256")}
          </label>
          <input
            id="wr-sha"
            className={`${input} font-mono text-xs`}
            value={sha}
            onChange={(e) => setSha(e.target.value)}
          />
          <p className="mt-1 text-xs text-slate-500">{t("register.sha256Hint")}</p>
        </div>
        <InfoForm f={f} idPrefix="wr" />
        {error && <p className="rounded-md border border-red-200 bg-red-50 p-2 text-sm text-red-700">{error}</p>}
        <div className="flex justify-end gap-2">
          <button type="button" onClick={onClose} className="rounded-md border px-3 py-1.5 text-sm">
            {t("cancel")}
          </button>
          <button
            type="submit"
            disabled={busy}
            className="rounded-md bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          >
            {t("register.submit")}
          </button>
        </div>
      </form>
    </Shell>
  );
}

/** Queue ``encode_residue_vocabulary`` over a source corpus with this vocabulary. */
export function EncodeCorpusDialog({ weights, onClose }: { weights: ModelWeights; onClose: () => void }) {
  const t = useTranslations("weights");
  const locale = useLocale();
  const [configs, setConfigs] = useState<EmbeddingConfig[]>([]);
  const [source, setSource] = useState("");
  const [device, setDevice] = useState("cuda");
  const [perJob, setPerJob] = useState(128);
  const [perCall, setPerCall] = useState(32768);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [queued, setQueued] = useState<string | null>(null);
  const card = useCardBusy();
  const [anyway, setAnyway] = useState(false);

  useEffect(() => {
    listEmbeddingConfigs()
      .then((all) => {
        const usable = all.filter((c) => c.model_backend !== "residue-vocabulary");
        setConfigs(usable);
        const ankh = usable.find((c) => c.model_backend === "ankh");
        setSource((ankh ?? usable[0])?.id ?? "");
      })
      .catch((e) => setError(errorText(e)));
  }, []);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    if (!source) return setError(t("encode.noSource"));
    setBusy(true);
    try {
      const job = await createJob({
        operation: "encode_residue_vocabulary",
        queue_name: "protea.embeddings",
        payload: {
          source_embedding_config_id: source,
          vocabulary_artifact_uri: weights.uri,
          device,
          sequences_per_job: perJob,
          residues_per_call: perCall,
        },
      });
      setQueued(job.id);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Shell
      id="weights-encode-title"
      title={t("encode.title", { name: weights.name })}
      subtitle={t("encode.subtitle")}
      onClose={onClose}
    >
      <form onSubmit={submit} className="space-y-4">
        <div>
          <label className={label} htmlFor="we-source">
            {t("encode.source")}
          </label>
          <select id="we-source" className={input} value={source} onChange={(e) => setSource(e.target.value)}>
            {configs.map((c) => (
              <option key={c.id} value={c.id}>
                {(c.display_name ?? c.model_name) + ` (${c.model_backend}, ${c.id.slice(0, 8)})`}
              </option>
            ))}
          </select>
          <p className="mt-1 text-xs text-slate-500">{t("encode.sourceHint")}</p>
        </div>
        <div className="grid gap-4 sm:grid-cols-3">
          <div>
            <label className={label} htmlFor="we-device">
              {t("encode.device")}
            </label>
            <select id="we-device" className={input} value={device} onChange={(e) => setDevice(e.target.value)}>
              <option value="cuda">cuda</option>
              <option value="cpu">cpu</option>
            </select>
          </div>
          <div>
            <label className={label} htmlFor="we-perjob">
              {t("encode.sequencesPerJob")}
            </label>
            <input
              id="we-perjob"
              type="number"
              min={1}
              className={input}
              value={perJob}
              onChange={(e) => setPerJob(Number(e.target.value))}
            />
          </div>
          <div>
            <label className={label} htmlFor="we-percall">
              {t("encode.residuesPerCall")}
            </label>
            <input
              id="we-percall"
              type="number"
              min={1024}
              className={input}
              value={perCall}
              onChange={(e) => setPerCall(Number(e.target.value))}
            />
          </div>
        </div>
        <p className="rounded-md border border-amber-200 bg-amber-50 p-2 text-xs leading-relaxed text-amber-800">
          {t("encode.warning")}
        </p>
        {card.busy && !queued && (
          <div className="space-y-2 rounded-md border border-amber-300 bg-amber-50 p-3 text-xs leading-relaxed text-amber-900">
            <p className="font-semibold">{t("encode.busyTitle")}</p>
            {card.jobs.length > 0 && (
              <ul className="space-y-0.5">
                {card.jobs.map((j) => (
                  <li key={j.id}>
                    <Link href={`/${locale}/instrument/jobs/${j.id}`} className="font-mono underline">
                      {j.id.slice(0, 8)}
                    </Link>{" "}
                    {j.operation}
                  </li>
                ))}
              </ul>
            )}
            {card.writing.map((name) => (
              <p key={name}>{t("encode.busyWriting", { name })}</p>
            ))}
            <label className="flex items-start gap-2">
              <input type="checkbox" checked={anyway} onChange={(e) => setAnyway(e.target.checked)} />
              <span>{t("encode.queueAnyway")}</span>
            </label>
          </div>
        )}
        {error && <p className="rounded-md border border-red-200 bg-red-50 p-2 text-sm text-red-700">{error}</p>}
        {queued && (
          <p className="rounded-md border border-green-200 bg-green-50 p-2 text-sm text-green-700">
            {t("encode.queued")}{" "}
            <Link href={`/${locale}/instrument/jobs/${queued}`} className="font-mono underline">
              {queued.slice(0, 8)}
            </Link>
          </p>
        )}
        <div className="flex justify-end gap-2">
          <button type="button" onClick={onClose} className="rounded-md border px-3 py-1.5 text-sm">
            {queued ? t("close") : t("cancel")}
          </button>
          {!queued && (
            <button
              type="submit"
              disabled={busy || !source || (card.busy && !anyway)}
              className="rounded-md bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
            >
              {t("encode.submit")}
            </button>
          )}
        </div>
      </form>
    </Shell>
  );
}
