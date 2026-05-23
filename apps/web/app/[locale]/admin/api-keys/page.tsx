"use client";

import Link from "next/link";
import { useCallback, useEffect, useId, useMemo, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import {
  listApiKeys,
  revokeApiKey,
  type ApiKey,
  type ApiKeyRole,
} from "@/lib/api";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import { SkeletonTableRow } from "@/components/Skeleton";
import { NewApiKeyDialog } from "@/components/NewApiKeyDialog";
import { useToast } from "@/components/Toast";
import { useFocusTrap } from "@/lib/useFocusTrap";
import { useHasRole, useRole } from "@/lib/useRole";

/**
 * Admin-only API-keys management page (FEAT-UI-APIKEY-MGMT).
 *
 * Unblocked by FEAT-AUTH (PR #456) which ships ``useRole`` /
 * ``useHasRole``: this page gates the entire surface behind
 * ``useHasRole("admin")``. Non-admin viewers see a 403-style panel
 * with a single link back home — the mint and revoke controls are
 * not even mounted in the React tree.
 *
 * Backed by ``GET /v1/auth/api-keys``, ``POST /v1/auth/api-keys``
 * and ``DELETE /v1/auth/api-keys/{id}``. The role + expiry inputs
 * collected by the mint dialog are intentionally forward-compatible
 * with the FEAT-AUTH backend schema extension (see ``lib/api.ts``
 * ``createApiKey``): the UI surfaces them today so operators learn
 * the shape, and the wire payload starts sending them the moment
 * the backend ``CreateApiKeyRequest`` model accepts the fields.
 */

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "short" });
  } catch {
    return iso;
  }
}

function roleStyles(role: ApiKeyRole): { bg: string; text: string; ring: string; dot: string } {
  switch (role) {
    case "admin":
      return { bg: "bg-red-50", text: "text-red-800", ring: "ring-red-200", dot: "bg-red-500" };
    case "operator":
      return { bg: "bg-amber-50", text: "text-amber-800", ring: "ring-amber-200", dot: "bg-amber-500" };
    default:
      return { bg: "bg-slate-100", text: "text-slate-700", ring: "ring-slate-200", dot: "bg-slate-400" };
  }
}

function RoleBadge({ role }: { role: ApiKeyRole }) {
  const s = roleStyles(role);
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wider ring-1 ring-inset ${s.bg} ${s.text} ${s.ring}`}
    >
      <span aria-hidden className={`h-1.5 w-1.5 rounded-full ${s.dot}`} />
      {role}
    </span>
  );
}

export default function ApiKeysAdminPage() {
  const t = useTranslations("apiKeys");
  const locale = useLocale();
  const toast = useToast();
  const role = useRole();
  const isAdmin = useHasRole("admin");

  // Mounted flag so the SSR pass renders the "loading" skeleton and
  // the client effect overwrites it with the role-aware tree. Without
  // this we'd briefly show the 403 panel during hydration for actual
  // admins, then flip to the management UI: jarring.
  const [hydrated, setHydrated] = useState(false);
  useEffect(() => setHydrated(true), []);

  const [keys, setKeys] = useState<ApiKey[] | null>(null);
  const [includeRevoked, setIncludeRevoked] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showDialog, setShowDialog] = useState(false);
  const [revoking, setRevoking] = useState<string | null>(null);
  const [confirmTarget, setConfirmTarget] = useState<ApiKey | null>(null);

  const load = useCallback(async () => {
    if (!isAdmin) return;
    setError(null);
    try {
      const rows = await listApiKeys({ include_revoked: includeRevoked, limit: 200 });
      setKeys(rows);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
    }
  }, [includeRevoked, isAdmin]);

  useEffect(() => {
    if (!hydrated || !isAdmin) return;
    void load();
  }, [hydrated, isAdmin, load]);

  const sorted = useMemo(() => {
    if (!keys) return [];
    // Active first, then revoked at the bottom. Within each bucket
    // the API already returns newest-first (created_at desc).
    return [...keys].sort((a, b) => {
      const ar = a.revoked_at ? 1 : 0;
      const br = b.revoked_at ? 1 : 0;
      return ar - br;
    });
  }, [keys]);

  async function handleRevoke(key: ApiKey) {
    setRevoking(key.id);
    try {
      await revokeApiKey(key.id);
      toast(t("revokedToast", { name: key.name }), "success");
      setConfirmTarget(null);
      await load();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      toast(t("revokeErrorToast", { error: msg }), "error");
    } finally {
      setRevoking(null);
    }
  }

  // Pre-hydration: show a minimal skeleton so admins don't blink past
  // the 403 panel and viewers don't see the management chrome before
  // the gate decides.
  if (!hydrated) {
    return (
      <>
        <Breadcrumbs />
        <div className="max-w-5xl mx-auto px-4 sm:px-6 py-6">
          <div className="h-7 w-48 animate-pulse rounded bg-slate-200" />
          <div className="mt-2 h-4 w-72 animate-pulse rounded bg-slate-200/70" />
        </div>
      </>
    );
  }

  if (!isAdmin) {
    return <ForbiddenPanel locale={locale} role={role} t={t} />;
  }

  const loading = keys === null;
  const activeCount = keys?.filter((k) => !k.revoked_at).length ?? 0;
  const revokedCount = (keys?.length ?? 0) - activeCount;

  return (
    <>
      <Breadcrumbs />
      <div className="max-w-5xl mx-auto px-4 sm:px-6 py-6 space-y-6">
        {/* Header */}
        <header className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-3">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">{t("title")}</h1>
            <p className="mt-1 max-w-3xl text-sm text-slate-500">{t("subtitle")}</p>
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => void load()}
              className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
            >
              {t("refresh")}
            </button>
            <button
              onClick={() => setShowDialog(true)}
              className="rounded-md bg-blue-700 px-3 py-1.5 text-sm font-semibold text-white hover:bg-blue-800"
            >
              {t("mint")}
            </button>
          </div>
        </header>

        {/* Filters */}
        <div className="flex flex-wrap items-center gap-3 text-sm">
          <label className="inline-flex items-center gap-2 cursor-pointer select-none text-slate-700">
            <input
              type="checkbox"
              checked={includeRevoked}
              onChange={(e) => setIncludeRevoked(e.target.checked)}
              className="rounded border-slate-300"
            />
            {t("includeRevoked")}
          </label>
          <span className="ml-auto text-xs text-slate-500 tabular-nums">
            {t("counts", { active: activeCount, revoked: revokedCount })}
          </span>
        </div>

        {error && (
          <div
            role="alert"
            className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
          >
            {t("loadError")}: {error}
          </div>
        )}

        {/* Table — desktop */}
        <div className="hidden md:block overflow-x-auto rounded-lg border bg-white shadow-sm">
          <table className="w-full text-sm">
            <thead className="protea-thead-sticky bg-slate-50 text-left">
              <tr>
                <th scope="col" className="px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                  {t("cols.name")}
                </th>
                <th scope="col" className="px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                  {t("cols.role")}
                </th>
                <th scope="col" className="px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                  {t("cols.prefix")}
                </th>
                <th scope="col" className="px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                  {t("cols.created")}
                </th>
                <th scope="col" className="px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                  {t("cols.lastUsed")}
                </th>
                <th scope="col" className="px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                  {t("cols.status")}
                </th>
                <th scope="col" className="px-3 py-2 text-right text-xs font-semibold uppercase tracking-wide text-slate-500">
                  {t("cols.actions")}
                </th>
              </tr>
            </thead>
            <tbody>
              {loading && Array.from({ length: 4 }).map((_, i) => <SkeletonTableRow key={i} cols={7} />)}
              {!loading && sorted.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-10 text-center text-sm text-slate-600">
                    <p>{t("empty")}</p>
                    <button
                      onClick={() => setShowDialog(true)}
                      className="mt-2 text-sm text-blue-700 underline"
                    >
                      {t("mintFirst")}
                    </button>
                  </td>
                </tr>
              )}
              {!loading && sorted.map((k) => {
                const revoked = Boolean(k.revoked_at);
                const r: ApiKeyRole = (k.role ?? "viewer") as ApiKeyRole;
                return (
                  <tr
                    key={k.id}
                    className={`border-t transition-colors ${revoked ? "bg-slate-50/60 text-slate-500" : "hover:bg-blue-50/60"}`}
                  >
                    <td className="px-3 py-2.5">
                      <span className={`font-mono text-[13px] ${revoked ? "text-slate-500" : "text-slate-900"}`}>
                        {k.name}
                      </span>
                    </td>
                    <td className="px-3 py-2.5">
                      <RoleBadge role={r} />
                    </td>
                    <td className="px-3 py-2.5 font-mono text-[11px] text-slate-600">
                      {k.prefix}…
                    </td>
                    <td className="px-3 py-2.5 text-xs">{formatDate(k.created_at)}</td>
                    <td className="px-3 py-2.5 text-xs">
                      {k.last_used_at ? formatDate(k.last_used_at) : <span className="text-slate-400">{t("neverUsed")}</span>}
                    </td>
                    <td className="px-3 py-2.5">
                      {revoked ? (
                        <span className="inline-flex items-center gap-1.5 rounded-full bg-stone-100 px-2 py-0.5 text-[11px] font-medium text-stone-600">
                          {t("statusRevoked")}
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 rounded-full bg-emerald-50 px-2 py-0.5 text-[11px] font-medium text-emerald-700 ring-1 ring-inset ring-emerald-200">
                          <span aria-hidden className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
                          {t("statusActive")}
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2.5 text-right">
                      {!revoked && (
                        <button
                          type="button"
                          onClick={() => setConfirmTarget(k)}
                          disabled={revoking === k.id}
                          className="inline-flex items-center gap-1.5 rounded-md border border-red-200 bg-red-50 px-2.5 py-1 text-[12px] font-semibold text-red-700 hover:bg-red-100 hover:border-red-300 disabled:opacity-50 transition-colors"
                        >
                          {revoking === k.id ? t("revoking") : t("revoke")}
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Mobile card list */}
        <div className="md:hidden space-y-2">
          {loading && Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="rounded-lg border bg-white p-3 shadow-sm animate-pulse space-y-2">
              <div className="h-4 w-2/3 rounded bg-slate-200" />
              <div className="h-3 w-1/2 rounded bg-slate-100" />
            </div>
          ))}
          {!loading && sorted.length === 0 && (
            <div className="rounded-lg border bg-white px-4 py-8 text-center text-sm text-slate-600 shadow-sm">
              {t("empty")}
            </div>
          )}
          {!loading && sorted.map((k) => {
            const revoked = Boolean(k.revoked_at);
            const r: ApiKeyRole = (k.role ?? "viewer") as ApiKeyRole;
            return (
              <div
                key={k.id}
                className={`rounded-lg border p-3 shadow-sm ${revoked ? "bg-slate-50/60" : "bg-white"}`}
              >
                <div className="flex items-start justify-between gap-2">
                  <span className="font-mono text-[13px] font-medium text-slate-900 break-all">{k.name}</span>
                  <RoleBadge role={r} />
                </div>
                <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-[11px] text-slate-600">
                  <span className="font-mono">{k.prefix}…</span>
                  <span>{formatDate(k.created_at)}</span>
                </div>
                <div className="mt-1 text-[11px] text-slate-500">
                  {t("cols.lastUsed")}: {k.last_used_at ? formatDate(k.last_used_at) : t("neverUsed")}
                </div>
                <div className="mt-2 flex items-center justify-between">
                  {revoked ? (
                    <span className="text-[11px] font-medium text-stone-500">{t("statusRevoked")}</span>
                  ) : (
                    <span className="inline-flex items-center gap-1.5 text-[11px] font-medium text-emerald-700">
                      <span aria-hidden className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
                      {t("statusActive")}
                    </span>
                  )}
                  {!revoked && (
                    <button
                      type="button"
                      onClick={() => setConfirmTarget(k)}
                      disabled={revoking === k.id}
                      className="inline-flex items-center gap-1.5 rounded-md border border-red-200 bg-red-50 px-2.5 py-1 text-[12px] font-semibold text-red-700 hover:bg-red-100 disabled:opacity-50"
                    >
                      {revoking === k.id ? t("revoking") : t("revoke")}
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        <p className="text-xs text-slate-500">{t("footnote")}</p>
      </div>

      <NewApiKeyDialog
        open={showDialog}
        onClose={() => {
          setShowDialog(false);
          void load();
        }}
        onMinted={({ name }) => {
          toast(t("mintedToast", { name }), "success");
        }}
      />

      {confirmTarget && (
        <RevokeConfirmDialog
          target={confirmTarget}
          loading={revoking === confirmTarget.id}
          onCancel={() => setConfirmTarget(null)}
          onConfirm={() => handleRevoke(confirmTarget)}
        />
      )}
    </>
  );
}

function ForbiddenPanel({
  locale,
  role,
  t,
}: {
  locale: string;
  role: ApiKeyRole;
  t: ReturnType<typeof useTranslations>;
}) {
  return (
    <>
      <Breadcrumbs />
      <div className="max-w-xl mx-auto px-4 sm:px-6 py-12">
        <div
          role="alert"
          className="rounded-xl border border-stone-200 bg-white p-6 shadow-sm text-center"
        >
          <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-red-50 text-red-600">
            <svg className="h-6 w-6" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24" aria-hidden>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
            </svg>
          </div>
          <h1 className="text-lg font-semibold text-slate-900">{t("forbiddenTitle")}</h1>
          <p className="mt-1 text-sm text-slate-600">{t("forbiddenBody")}</p>
          <p className="mt-2 text-xs text-slate-500">{t("forbiddenRoleLabel")}: <span className="font-mono">{role}</span></p>
          <Link
            href={`/${locale}`}
            className="mt-5 inline-flex items-center gap-1.5 rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800"
          >
            {t("forbiddenBackHome")}
          </Link>
        </div>
      </div>
    </>
  );
}

function RevokeConfirmDialog({
  target,
  loading,
  onCancel,
  onConfirm,
}: {
  target: ApiKey;
  loading: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const t = useTranslations("apiKeys.confirm");
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
        className="w-full max-w-sm rounded-xl border bg-white p-6 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id={titleId} className="text-base font-semibold text-slate-900">
          {t("title")}
        </h2>
        <p className="mt-2 text-sm text-slate-500">{t("body")}</p>
        <div className="mt-3 rounded-md border border-stone-200 bg-stone-50 px-3 py-2 text-xs">
          <div className="font-mono text-slate-800 break-all">{target.name}</div>
          <div className="mt-0.5 font-mono text-[11px] text-slate-500">{target.prefix}…</div>
        </div>
        <div className="mt-5 flex justify-end gap-2">
          <button
            onClick={handleClose}
            disabled={loading}
            className="rounded-md border px-4 py-2 text-sm hover:bg-slate-50 disabled:opacity-50"
          >
            {t("cancel")}
          </button>
          <button
            onClick={onConfirm}
            disabled={loading}
            className="rounded-md bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50"
          >
            {loading ? t("confirming") : t("confirm")}
          </button>
        </div>
      </div>
    </div>
  );
}
