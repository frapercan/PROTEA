"use client";

import { useCallback, useEffect, useId, useMemo, useState } from "react";
import { useTranslations } from "next-intl";
import {
  createExperimentRun,
  deleteExperimentRun,
  listExperimentRuns,
  updateExperimentRun,
  type ExperimentRun,
  type ExperimentRunStatus,
  type CreateExperimentRunPayload,
  type UpdateExperimentRunPayload,
} from "@/lib/api";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import { SkeletonTableRow } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";
import { useFocusTrap } from "@/lib/useFocusTrap";

/**
 * Admin surface for ExperimentRun rows (the F-EXP campaign narrative
 * spine). Backend lives at protea/api/routers/experiment_runs.py and
 * has shipped POST/GET/PATCH/DELETE for months; this is the first UI
 * consumer.
 *
 * Layout: list (newest first) with status filter chips on top, a "New
 * run" button that opens an inline mint form (planned status), and a
 * per-row Edit modal that exposes the narrative + status transition +
 * tags. Status transitions stamp started_at / finished_at on the
 * server side; the UI just sends the new status.
 *
 * Auth: this page is intentionally not gated by client-side role
 * inspection — the backend remains the source of truth (writes go
 * through ``require_api_key_or_bearer`` on the deployment). A friendly
 * 401/403 surface is rendered if the API refuses a write.
 */

const STATUS_OPTIONS: ExperimentRunStatus[] = [
  "planned",
  "running",
  "done",
  "abandoned",
];

const STATUS_STYLES: Record<ExperimentRunStatus, { chip: string; dot: string }> = {
  planned: { chip: "bg-slate-100 text-slate-700 ring-slate-200", dot: "bg-slate-400" },
  running: { chip: "bg-blue-100 text-blue-800 ring-blue-200", dot: "bg-blue-500" },
  done: { chip: "bg-emerald-100 text-emerald-800 ring-emerald-200", dot: "bg-emerald-500" },
  abandoned: { chip: "bg-rose-100 text-rose-800 ring-rose-200", dot: "bg-rose-500" },
};

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "short" });
  } catch {
    return iso;
  }
}

function parseTags(raw: string): string[] {
  return raw
    .split(/[\s,]+/)
    .map((t) => t.trim())
    .filter(Boolean);
}

function parseJsonBlob(raw: string): { ok: true; value: Record<string, unknown> } | { ok: false; error: string } {
  const trimmed = raw.trim();
  if (!trimmed) return { ok: true, value: {} };
  try {
    const parsed = JSON.parse(trimmed);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      return { ok: false, error: "Expected a JSON object." };
    }
    return { ok: true, value: parsed as Record<string, unknown> };
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: msg };
  }
}

export default function ExperimentRunsPage() {
  const t = useTranslations("admin.experimentRuns");
  const toast = useToast();

  const [runs, setRuns] = useState<ExperimentRun[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<ExperimentRunStatus | "all">("all");
  const [editing, setEditing] = useState<ExperimentRun | null>(null);
  const [showMint, setShowMint] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<ExperimentRun | null>(null);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async () => {
    setLoadError(null);
    try {
      const rows = await listExperimentRuns({
        status: statusFilter === "all" ? undefined : statusFilter,
        limit: 200,
      });
      setRuns(rows);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setLoadError(msg);
      setRuns([]);
    }
  }, [statusFilter]);

  useEffect(() => { void load(); }, [load]);

  // Manual refresh — other admins on the same deployment may have
  // minted or transitioned runs since the page was loaded, and the
  // backend has no SSE / polling channel for ExperimentRun yet. The
  // loading state is local (separate from initial-fetch skeleton) so
  // the table content does not blank during the refetch.
  async function handleRefresh() {
    if (refreshing) return;
    setRefreshing(true);
    try {
      await load();
    } finally {
      setRefreshing(false);
    }
  }

  function handleCreated(run: ExperimentRun) {
    setRuns((prev) => (prev ? [run, ...prev] : [run]));
    setShowMint(false);
    toast(t("toasts.created", { name: run.name }), "success");
  }

  function handleUpdated(run: ExperimentRun) {
    setRuns((prev) => (prev ? prev.map((r) => (r.id === run.id ? run : r)) : [run]));
    setEditing(null);
    toast(t("toasts.updated", { name: run.name }), "success");
  }

  async function confirmDelete(run: ExperimentRun) {
    setDeletingId(run.id);
    try {
      await deleteExperimentRun(run.id);
      setRuns((prev) => (prev ? prev.filter((r) => r.id !== run.id) : prev));
      toast(t("toasts.deleted", { name: run.name }), "info");
      setDeleteTarget(null);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast(msg, "error");
    } finally {
      setDeletingId(null);
    }
  }

  const counts = useMemo(() => {
    const c: Record<ExperimentRunStatus | "all", number> = {
      all: 0,
      planned: 0,
      running: 0,
      done: 0,
      abandoned: 0,
    };
    for (const r of runs ?? []) {
      c.all += 1;
      c[r.status] += 1;
    }
    return c;
  }, [runs]);

  return (
    <>
      <Breadcrumbs />
      <div className="max-w-6xl mx-auto px-4 sm:px-6 py-6 space-y-6">
        <header className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-2xl font-bold text-slate-900">{t("title")}</h1>
              <span
                className="rounded bg-amber-100 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-amber-700 ring-1 ring-amber-200"
                title={t("adminBadgeTooltip")}
              >
                {t("adminBadge")}
              </span>
            </div>
            <p className="text-sm text-slate-500 mt-1 max-w-3xl">{t("subtitle")}</p>
          </div>
          <button
            type="button"
            onClick={() => setShowMint(true)}
            className="rounded-md bg-blue-700 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-800"
          >
            {t("newRun")}
          </button>
        </header>

        {/* Status filter chips + manual refresh.

            Refresh sits next to (not on top of) the chips so the
            tablist remains a single horizontal block at the natural
            breakpoint; on narrow viewports the wrapper flexes to a
            stack so the button does not crowd the chip row. */}
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div
            role="tablist"
            aria-label={t("filterAriaLabel")}
            className="inline-flex flex-wrap rounded-lg border border-slate-200 bg-white p-1 shadow-sm self-start"
          >
            <FilterChip
              label={`${t("filters.all")} (${counts.all})`}
              active={statusFilter === "all"}
              onClick={() => setStatusFilter("all")}
            />
            {STATUS_OPTIONS.map((s) => (
              <FilterChip
                key={s}
                label={`${t(`status.${s}`)} (${counts[s]})`}
                active={statusFilter === s}
                onClick={() => setStatusFilter(s)}
              />
            ))}
          </div>
          <button
            type="button"
            onClick={() => void handleRefresh()}
            disabled={refreshing || runs === null}
            className="self-start rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {refreshing ? t("refreshing") : t("refresh")}
          </button>
        </div>

        {loadError && (
          <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            {loadError}
          </pre>
        )}

        {runs === null && (
          <div className="rounded-lg border bg-white shadow-sm overflow-hidden">
            {Array.from({ length: 4 }).map((_, i) => (
              <SkeletonTableRow key={i} cols={4} />
            ))}
          </div>
        )}

        {runs !== null && runs.length === 0 && !loadError && (
          <div className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-slate-600 shadow-sm">
            {statusFilter === "all" ? t("emptyAll") : t("emptyFiltered")}
          </div>
        )}

        {runs && runs.length > 0 && (
          <>
            {/* Mobile card list — matches the proteins page convention
                (each row becomes a card stacked vertically with the
                key fields, status chip and per-row actions). The dense
                4-column table is illegible at 375px once tags and
                descriptions wrap. */}
            <ul className="md:hidden space-y-2">
              {runs.map((r) => {
                const style = STATUS_STYLES[r.status];
                return (
                  <li
                    key={r.id}
                    className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm space-y-2"
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="min-w-0">
                        <div className="font-medium text-slate-900 break-words">{r.name}</div>
                        {r.description && (
                          <p className="mt-0.5 text-xs text-slate-500 leading-snug line-clamp-3">
                            {r.description}
                          </p>
                        )}
                      </div>
                      <span className={`shrink-0 inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px] font-semibold ring-1 ring-inset ${style.chip}`}>
                        <span className={`inline-block h-1.5 w-1.5 rounded-full ${style.dot}`} />
                        {t(`status.${r.status}`)}
                      </span>
                    </div>
                    {r.tags.length > 0 && (
                      <div className="flex flex-wrap gap-1">
                        {r.tags.map((tag) => (
                          <span
                            key={tag}
                            className="rounded bg-slate-100 px-1.5 py-0.5 text-[11px] text-slate-600"
                          >
                            {tag}
                          </span>
                        ))}
                      </div>
                    )}
                    <p className="text-[11px] text-slate-500">
                      {t("cols.created")}: {formatDate(r.created_at)}
                    </p>
                    <div className="flex gap-2 pt-1">
                      <button
                        type="button"
                        onClick={() => setEditing(r)}
                        className="flex-1 rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50"
                      >
                        {t("actions.edit")}
                      </button>
                      <button
                        type="button"
                        onClick={() => setDeleteTarget(r)}
                        className="flex-1 rounded-md border border-red-200 bg-red-50 px-2.5 py-1.5 text-xs font-medium text-red-700 hover:bg-red-100"
                      >
                        {t("actions.delete")}
                      </button>
                    </div>
                  </li>
                );
              })}
            </ul>

            {/* Desktop table — unchanged from the original layout. */}
            <div className="hidden md:block overflow-x-auto rounded-lg border bg-white shadow-sm">
            <table className="w-full text-sm">
              <thead className="bg-slate-50 text-left">
                <tr>
                  <th className="px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-500">{t("cols.name")}</th>
                  <th className="px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-500">{t("cols.status")}</th>
                  <th className="px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-500">{t("cols.tags")}</th>
                  <th className="px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-500">{t("cols.created")}</th>
                  <th className="px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-slate-500 text-right">{t("cols.actions")}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {runs.map((r) => {
                  const style = STATUS_STYLES[r.status];
                  return (
                    <tr key={r.id} className="hover:bg-slate-50/60">
                      <td className="px-4 py-3 align-top">
                        <div className="font-medium text-slate-900">{r.name}</div>
                        {r.description && (
                          <p className="mt-0.5 text-xs text-slate-500 leading-snug line-clamp-2">
                            {r.description}
                          </p>
                        )}
                      </td>
                      <td className="px-4 py-3 align-top">
                        <span className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px] font-semibold ring-1 ring-inset ${style.chip}`}>
                          <span className={`inline-block h-1.5 w-1.5 rounded-full ${style.dot}`} />
                          {t(`status.${r.status}`)}
                        </span>
                      </td>
                      <td className="px-4 py-3 align-top">
                        {r.tags.length === 0 ? (
                          <span className="text-xs text-slate-400">—</span>
                        ) : (
                          <div className="flex flex-wrap gap-1">
                            {r.tags.map((tag) => (
                              <span
                                key={tag}
                                className="rounded bg-slate-100 px-1.5 py-0.5 text-[11px] text-slate-600"
                              >
                                {tag}
                              </span>
                            ))}
                          </div>
                        )}
                      </td>
                      <td className="px-4 py-3 align-top text-xs text-slate-500">
                        {formatDate(r.created_at)}
                      </td>
                      <td className="px-4 py-3 align-top text-right">
                        <div className="inline-flex gap-2">
                          <button
                            type="button"
                            onClick={() => setEditing(r)}
                            className="rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
                          >
                            {t("actions.edit")}
                          </button>
                          <button
                            type="button"
                            onClick={() => setDeleteTarget(r)}
                            className="rounded-md border border-red-200 bg-red-50 px-2.5 py-1 text-xs font-medium text-red-700 hover:bg-red-100"
                          >
                            {t("actions.delete")}
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          </>
        )}
      </div>

      {showMint && (
        <MintExperimentRunModal
          onClose={() => setShowMint(false)}
          onCreated={handleCreated}
        />
      )}
      {editing && (
        <EditExperimentRunModal
          run={editing}
          onClose={() => setEditing(null)}
          onUpdated={handleUpdated}
        />
      )}
      {deleteTarget && (
        <DeleteExperimentRunConfirmDialog
          target={deleteTarget}
          loading={deletingId === deleteTarget.id}
          onCancel={() => setDeleteTarget(null)}
          onConfirm={() => void confirmDelete(deleteTarget)}
        />
      )}
    </>
  );
}

function FilterChip({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      onClick={onClick}
      className={`px-2.5 py-1 text-xs font-medium rounded-md transition-colors ${
        active ? "bg-blue-600 text-white" : "text-slate-600 hover:text-slate-900 hover:bg-slate-50"
      }`}
    >
      {label}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Mint modal
// ---------------------------------------------------------------------------

function MintExperimentRunModal({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: (run: ExperimentRun) => void;
}) {
  const t = useTranslations("admin.experimentRuns");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [hypothesis, setHypothesis] = useState("");
  const [tagsRaw, setTagsRaw] = useState("");
  const [configRaw, setConfigRaw] = useState("");
  const [provenanceRaw, setProvenanceRaw] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (!name.trim()) {
      setError(t("errors.nameRequired"));
      return;
    }
    const config = parseJsonBlob(configRaw);
    if (!config.ok) {
      setError(t("errors.configInvalid", { reason: config.error }));
      return;
    }
    const provenance = parseJsonBlob(provenanceRaw);
    if (!provenance.ok) {
      setError(t("errors.provenanceInvalid", { reason: provenance.error }));
      return;
    }
    const payload: CreateExperimentRunPayload = {
      name: name.trim(),
      description: description.trim() || null,
      hypothesis: hypothesis.trim() || null,
      config: config.value,
      provenance: provenance.value,
      tags: parseTags(tagsRaw),
    };
    setSubmitting(true);
    try {
      const created = await createExperimentRun(payload);
      onCreated(created);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <ModalShell title={t("mint.title")} subtitle={t("mint.subtitle")} onClose={onClose}>
      <form onSubmit={handleSubmit} className="space-y-4">
        <FieldText
          id="exp-name"
          label={t("fields.name")}
          required
          value={name}
          onChange={setName}
          placeholder="ablation-K-2026-05-09"
          mono
        />
        <FieldTextarea
          id="exp-desc"
          label={t("fields.description")}
          value={description}
          onChange={setDescription}
          placeholder={t("fields.descriptionPlaceholder")}
          rows={3}
        />
        <FieldTextarea
          id="exp-hyp"
          label={t("fields.hypothesis")}
          value={hypothesis}
          onChange={setHypothesis}
          placeholder={t("fields.hypothesisPlaceholder")}
          rows={2}
        />
        <FieldText
          id="exp-tags"
          label={t("fields.tags")}
          help={t("fields.tagsHelp")}
          value={tagsRaw}
          onChange={setTagsRaw}
          placeholder="ablation, K-sweep, bench-v1"
        />
        <FieldTextarea
          id="exp-config"
          label={t("fields.config")}
          help={t("fields.configHelp")}
          value={configRaw}
          onChange={setConfigRaw}
          placeholder='{"K_values": [3, 5, 10], "embedding_backend": "esm2"}'
          rows={3}
          mono
        />
        <FieldTextarea
          id="exp-prov"
          label={t("fields.provenance")}
          help={t("fields.provenanceHelp")}
          value={provenanceRaw}
          onChange={setProvenanceRaw}
          placeholder='{"campaign": "bench-v1"}'
          rows={2}
          mono
        />
        {error && (
          <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            {error}
          </pre>
        )}
        <ModalActions
          onClose={onClose}
          submitting={submitting}
          submitLabel={t("mint.submit")}
          submittingLabel={t("mint.submitting")}
        />
      </form>
    </ModalShell>
  );
}

// ---------------------------------------------------------------------------
// Edit modal
// ---------------------------------------------------------------------------

function EditExperimentRunModal({
  run,
  onClose,
  onUpdated,
}: {
  run: ExperimentRun;
  onClose: () => void;
  onUpdated: (run: ExperimentRun) => void;
}) {
  const t = useTranslations("admin.experimentRuns");
  const [description, setDescription] = useState(run.description ?? "");
  const [hypothesis, setHypothesis] = useState(run.hypothesis ?? "");
  const [findings, setFindings] = useState(run.findings ?? "");
  const [status, setStatus] = useState<ExperimentRunStatus>(run.status);
  const [tagsRaw, setTagsRaw] = useState(run.tags.join(", "));
  const [configRaw, setConfigRaw] = useState(JSON.stringify(run.config ?? {}, null, 2));
  const [provenanceRaw, setProvenanceRaw] = useState(JSON.stringify(run.provenance ?? {}, null, 2));
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    const config = parseJsonBlob(configRaw);
    if (!config.ok) {
      setError(t("errors.configInvalid", { reason: config.error }));
      return;
    }
    const provenance = parseJsonBlob(provenanceRaw);
    if (!provenance.ok) {
      setError(t("errors.provenanceInvalid", { reason: provenance.error }));
      return;
    }
    const payload: UpdateExperimentRunPayload = {
      description: description,
      hypothesis: hypothesis,
      findings: findings,
      status,
      config: config.value,
      provenance: provenance.value,
      tags: parseTags(tagsRaw),
    };
    setSubmitting(true);
    try {
      const updated = await updateExperimentRun(run.id, payload);
      onUpdated(updated);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <ModalShell
      title={t("edit.title", { name: run.name })}
      subtitle={t("edit.subtitle")}
      onClose={onClose}
    >
      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label className="block text-xs font-semibold uppercase tracking-wider text-slate-500 mb-1">
            {t("fields.status")}
          </label>
          <div className="flex flex-wrap gap-1.5">
            {STATUS_OPTIONS.map((s) => {
              const selected = status === s;
              const style = STATUS_STYLES[s];
              return (
                <button
                  type="button"
                  key={s}
                  onClick={() => setStatus(s)}
                  aria-pressed={selected}
                  className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-semibold ring-1 ring-inset transition-colors ${
                    selected
                      ? style.chip + " ring-offset-2 ring-offset-white"
                      : "bg-white text-slate-600 ring-slate-200 hover:bg-slate-50"
                  }`}
                >
                  <span className={`inline-block h-1.5 w-1.5 rounded-full ${style.dot}`} />
                  {t(`status.${s}`)}
                </button>
              );
            })}
          </div>
          <p className="text-[11px] text-slate-500 mt-1 leading-snug">{t("fields.statusHelp")}</p>
        </div>

        <FieldTextarea
          id="edit-desc"
          label={t("fields.description")}
          value={description}
          onChange={setDescription}
          rows={3}
        />
        <FieldTextarea
          id="edit-hyp"
          label={t("fields.hypothesis")}
          value={hypothesis}
          onChange={setHypothesis}
          rows={2}
        />
        <FieldTextarea
          id="edit-find"
          label={t("fields.findings")}
          help={t("fields.findingsHelp")}
          value={findings}
          onChange={setFindings}
          rows={3}
        />
        <FieldText
          id="edit-tags"
          label={t("fields.tags")}
          help={t("fields.tagsHelp")}
          value={tagsRaw}
          onChange={setTagsRaw}
        />
        <FieldTextarea
          id="edit-config"
          label={t("fields.config")}
          help={t("fields.configHelp")}
          value={configRaw}
          onChange={setConfigRaw}
          rows={4}
          mono
        />
        <FieldTextarea
          id="edit-prov"
          label={t("fields.provenance")}
          help={t("fields.provenanceHelp")}
          value={provenanceRaw}
          onChange={setProvenanceRaw}
          rows={3}
          mono
        />

        <dl className="grid grid-cols-1 sm:grid-cols-3 gap-2 rounded-md border border-slate-200 bg-slate-50/60 p-3 text-xs">
          <div>
            <dt className="text-slate-500">{t("readonly.id")}</dt>
            <dd className="font-mono text-slate-700">{run.id}</dd>
          </div>
          <div>
            <dt className="text-slate-500">{t("readonly.startedAt")}</dt>
            <dd className="font-mono text-slate-700">{formatDate(run.started_at)}</dd>
          </div>
          <div>
            <dt className="text-slate-500">{t("readonly.finishedAt")}</dt>
            <dd className="font-mono text-slate-700">{formatDate(run.finished_at)}</dd>
          </div>
        </dl>

        {error && (
          <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            {error}
          </pre>
        )}
        <ModalActions
          onClose={onClose}
          submitting={submitting}
          submitLabel={t("edit.submit")}
          submittingLabel={t("edit.submitting")}
        />
      </form>
    </ModalShell>
  );
}

// ---------------------------------------------------------------------------
// Shared modal primitives
// ---------------------------------------------------------------------------

function ModalShell({
  title,
  subtitle,
  onClose,
  children,
}: {
  title: string;
  subtitle?: string;
  onClose: () => void;
  children: React.ReactNode;
}) {
  const t = useTranslations("admin.experimentRuns");
  const titleId = useId();
  // Focus-trap mirrors the api-keys RevokeConfirmDialog pattern: focus
  // moves into the dialog on open, Tab/Shift+Tab cycles within, Escape
  // closes, and focus returns to the trigger on unmount.
  const dialogRef = useFocusTrap<HTMLDivElement>(true, onClose);
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        onClick={(e) => e.stopPropagation()}
        className="w-full max-w-2xl rounded-xl border bg-white shadow-xl flex flex-col max-h-[92vh]"
      >
        <div className="flex items-start justify-between border-b px-5 py-4">
          <div>
            <h2 id={titleId} className="text-base font-semibold text-slate-900">{title}</h2>
            {subtitle && <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>}
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label={t("modal.close")}
            className="text-slate-500 hover:text-slate-900 text-xl leading-none"
          >
            ×
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-5">{children}</div>
      </div>
    </div>
  );
}

function DeleteExperimentRunConfirmDialog({
  target,
  loading,
  onCancel,
  onConfirm,
}: {
  target: ExperimentRun;
  loading: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const t = useTranslations("admin.experimentRuns");
  const titleId = useId();

  const handleClose = useCallback(() => {
    if (!loading) onCancel();
  }, [loading, onCancel]);

  const dialogRef = useFocusTrap<HTMLDivElement>(true, handleClose);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={handleClose}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="w-full max-w-sm rounded-xl border border-stone-200 bg-white p-6 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id={titleId} className="text-base font-semibold text-stone-900">
          {t("deleteDialog.title")}
        </h2>
        <p className="mt-2 text-sm text-stone-600">{t("deleteDialog.body")}</p>
        <div className="mt-3 rounded-md border border-stone-200 bg-stone-50 px-3 py-2 text-xs">
          <div className="font-mono text-stone-800 break-all">{target.name}</div>
          <div className="mt-0.5 font-mono text-[11px] text-stone-500">{target.id}</div>
        </div>
        <div className="mt-5 flex justify-end gap-2">
          <button
            type="button"
            onClick={handleClose}
            disabled={loading}
            className="rounded-md border border-stone-300 px-4 py-2 text-sm text-stone-700 hover:bg-stone-50 disabled:opacity-50"
          >
            {t("actions.cancel")}
          </button>
          {/* Red destructive signal: deletion is irreversible (the row
              is removed server-side, not just hidden), matching the
              api-keys RevokeConfirmDialog convention. */}
          <button
            type="button"
            onClick={onConfirm}
            disabled={loading}
            className="rounded-md bg-rose-700 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-800 disabled:opacity-50"
          >
            {loading ? t("deleteDialog.confirming") : t("deleteDialog.confirm")}
          </button>
        </div>
      </div>
    </div>
  );
}

function ModalActions({
  onClose,
  submitting,
  submitLabel,
  submittingLabel,
}: {
  onClose: () => void;
  submitting: boolean;
  submitLabel: string;
  submittingLabel: string;
}) {
  const t = useTranslations("admin.experimentRuns");
  return (
    <div className="flex justify-end gap-2 border-t border-slate-100 pt-4">
      <button
        type="button"
        onClick={onClose}
        className="rounded-md border border-slate-300 px-4 py-2 text-sm hover:bg-slate-50"
      >
        {t("actions.cancel")}
      </button>
      <button
        type="submit"
        disabled={submitting}
        className="rounded-md bg-blue-700 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-800 disabled:opacity-50"
      >
        {submitting ? submittingLabel : submitLabel}
      </button>
    </div>
  );
}

function FieldText({
  id,
  label,
  value,
  onChange,
  placeholder,
  required,
  help,
  mono,
}: {
  id: string;
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  required?: boolean;
  help?: string;
  mono?: boolean;
}) {
  return (
    <div>
      <label htmlFor={id} className="block text-xs font-semibold uppercase tracking-wider text-slate-500 mb-1">
        {label}{required && <span className="text-red-500 ml-0.5">*</span>}
      </label>
      <input
        id={id}
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        required={required}
        className={`w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 ${mono ? "font-mono text-[13px]" : ""}`}
      />
      {help && <p className="text-[11px] text-slate-500 mt-1 leading-snug">{help}</p>}
    </div>
  );
}

function FieldTextarea({
  id,
  label,
  value,
  onChange,
  placeholder,
  rows = 3,
  help,
  mono,
}: {
  id: string;
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  rows?: number;
  help?: string;
  mono?: boolean;
}) {
  return (
    <div>
      <label htmlFor={id} className="block text-xs font-semibold uppercase tracking-wider text-slate-500 mb-1">
        {label}
      </label>
      <textarea
        id={id}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        rows={rows}
        className={`w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 ${mono ? "font-mono text-[13px]" : ""}`}
      />
      {help && <p className="text-[11px] text-slate-500 mt-1 leading-snug">{help}</p>}
    </div>
  );
}
