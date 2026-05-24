"use client";

import Link from "next/link";
import { useLocale } from "next-intl";
import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AuthError,
  approveAdminUser,
  deactivateAdminUser,
  listAdminUsers,
  setAdminUserRole,
  type AdminUserRow,
  type UserRole,
} from "@/lib/authApi";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import { useHasRole, useRole } from "@/lib/useRole";

/**
 * FARM-AUTH.10 — admin user-management dashboard.
 *
 * Gated by ``useHasRole("admin")``; non-admins see a 403-style panel.
 * The data source is ``GET /api-proxy/v1/admin/users``, which lands in
 * the FARM-AUTH.4/.5 sweep. Until those routers merge the endpoint
 * returns 404 and we render a friendly "the admin user-management
 * endpoints have not shipped yet" panel — much less hostile than a
 * raw red traceback.
 *
 * Approve / deactivate / role-change rely on
 *   POST /admin/users/{id}/approve
 *   POST /admin/users/{id}/deactivate
 *   POST /admin/users/{id}/role  (body: {role})
 * which are wired through ``lib/authApi.ts`` and gracefully no-op
 * when the backend returns 404 / 501.
 */

type LoadState =
  | { kind: "loading" }
  | { kind: "ok"; rows: AdminUserRow[] }
  | { kind: "missing"; status: number }
  | { kind: "error"; message: string };

type Tab = "all" | "pending" | "active" | "deactivated";

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "short" });
  } catch {
    return iso;
  }
}

function roleStyles(role: UserRole): { bg: string; text: string; ring: string; dot: string } {
  switch (role) {
    case "admin":
      return { bg: "bg-amber-100", text: "text-amber-800", ring: "ring-amber-200", dot: "bg-amber-500" };
    case "operator":
      return { bg: "bg-blue-100", text: "text-blue-800", ring: "ring-blue-200", dot: "bg-blue-500" };
    case "researcher":
      return { bg: "bg-emerald-50", text: "text-emerald-800", ring: "ring-emerald-200", dot: "bg-emerald-500" };
    default:
      return { bg: "bg-stone-100", text: "text-stone-700", ring: "ring-stone-200", dot: "bg-stone-400" };
  }
}

function statusStyles(status: AdminUserRow["status"]): {
  bg: string;
  text: string;
  ring: string;
  dot: string;
} {
  switch (status) {
    case "active":
      return { bg: "bg-emerald-50", text: "text-emerald-700", ring: "ring-emerald-200", dot: "bg-emerald-500" };
    case "pending":
      return { bg: "bg-amber-50", text: "text-amber-800", ring: "ring-amber-200", dot: "bg-amber-500" };
    case "deactivated":
      return { bg: "bg-rose-50", text: "text-rose-800", ring: "ring-rose-200", dot: "bg-rose-500" };
  }
}

function RoleBadge({ role }: { role: UserRole }) {
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

function StatusBadge({ status }: { status: AdminUserRow["status"] }) {
  const s = statusStyles(status);
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px] font-medium ring-1 ring-inset ${s.bg} ${s.text} ${s.ring}`}
    >
      <span aria-hidden className={`h-1.5 w-1.5 rounded-full ${s.dot}`} />
      {status}
    </span>
  );
}

export default function AdminUsersPage() {
  const locale = useLocale();
  const role = useRole();
  const isAdmin = useHasRole("admin");

  const [hydrated, setHydrated] = useState(false);
  useEffect(() => setHydrated(true), []);

  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [tab, setTab] = useState<Tab>("all");
  const [pendingAction, setPendingAction] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setActionError(null);
    setState({ kind: "loading" });
    try {
      const rows = await listAdminUsers();
      setState({ kind: "ok", rows });
    } catch (e) {
      if (e instanceof AuthError && (e.status === 404 || e.status === 501)) {
        setState({ kind: "missing", status: e.status });
      } else if (e instanceof AuthError) {
        setState({ kind: "error", message: `HTTP ${e.status}: ${e.detail}` });
      } else {
        setState({ kind: "error", message: e instanceof Error ? e.message : String(e) });
      }
    }
  }, []);

  useEffect(() => {
    if (!hydrated || !isAdmin) return;
    void load();
  }, [hydrated, isAdmin, load]);

  const filtered = useMemo(() => {
    if (state.kind !== "ok") return [] as AdminUserRow[];
    if (tab === "all") return state.rows;
    return state.rows.filter((r) => r.status === tab);
  }, [state, tab]);

  const counts = useMemo(() => {
    if (state.kind !== "ok") return { all: 0, pending: 0, active: 0, deactivated: 0 };
    return state.rows.reduce(
      (acc, r) => {
        acc.all += 1;
        acc[r.status] += 1;
        return acc;
      },
      { all: 0, pending: 0, active: 0, deactivated: 0 } as Record<Tab, number>,
    );
  }, [state]);

  async function runAction(
    label: string,
    id: string,
    fn: () => Promise<unknown>,
  ) {
    setPendingAction(`${label}:${id}`);
    setActionError(null);
    try {
      await fn();
      await load();
    } catch (e) {
      if (e instanceof AuthError && (e.status === 404 || e.status === 501)) {
        setActionError(
          `The "${label}" endpoint is not implemented on this backend yet (HTTP ${e.status}). When the corresponding FARM-AUTH router merges this control will start working.`,
        );
      } else if (e instanceof AuthError) {
        setActionError(`${label} failed: HTTP ${e.status} ${e.detail}`);
      } else {
        setActionError(`${label} failed: ${e instanceof Error ? e.message : String(e)}`);
      }
    } finally {
      setPendingAction(null);
    }
  }

  if (!hydrated) {
    return (
      <>
        <Breadcrumbs />
        <div className="mx-auto max-w-5xl px-4 sm:px-6 py-6 space-y-3">
          <div className="h-7 w-48 animate-pulse rounded bg-stone-200" />
          <div className="h-4 w-72 animate-pulse rounded bg-stone-100" />
          <div className="h-64 animate-pulse rounded-xl bg-stone-100" />
        </div>
      </>
    );
  }

  if (!isAdmin) {
    return <ForbiddenPanel locale={locale} role={role} />;
  }

  return (
    <>
      <Breadcrumbs />
      <div className="mx-auto max-w-6xl px-4 sm:px-6 py-6 space-y-6">
        <header className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-3">
          <div>
            <h1 className="text-2xl font-bold tracking-tight text-stone-950">User accounts</h1>
            <p className="mt-1 max-w-3xl text-sm text-stone-600">
              Approve new sign-ups, change roles and deactivate accounts.
              Each PROTEA deployment has its own user table (ADR D37).
            </p>
          </div>
          <button
            type="button"
            onClick={() => void load()}
            className="self-start rounded-md border border-stone-300 bg-white px-3 py-1.5 text-sm font-semibold text-stone-700 transition-colors hover:bg-stone-50"
          >
            Refresh
          </button>
        </header>

        {state.kind === "missing" && (
          <BackendMissingPanel status={state.status} />
        )}

        {state.kind === "error" && (
          <div
            role="alert"
            className="rounded-md border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800"
          >
            Could not load users: {state.message}
          </div>
        )}

        {state.kind !== "missing" && (
          <>
            <nav
              role="tablist"
              aria-label="Filter users by status"
              className="flex flex-wrap gap-1 rounded-lg bg-stone-100 p-0.5"
            >
              {(
                [
                  ["all", "All"],
                  ["pending", "Pending"],
                  ["active", "Active"],
                  ["deactivated", "Deactivated"],
                ] as [Tab, string][]
              ).map(([key, label]) => {
                const active = tab === key;
                return (
                  <button
                    key={key}
                    role="tab"
                    aria-selected={active}
                    onClick={() => setTab(key)}
                    className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-semibold transition-colors ${
                      active
                        ? "bg-white text-stone-900 shadow-sm"
                        : "text-stone-500 hover:text-stone-700"
                    }`}
                  >
                    {label}
                    <span className="rounded-full bg-stone-200/80 px-1.5 text-[10px] font-medium text-stone-700 tabular-nums">
                      {counts[key]}
                    </span>
                  </button>
                );
              })}
            </nav>

            {actionError && (
              <div
                role="alert"
                className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900"
              >
                {actionError}
              </div>
            )}

            <div className="overflow-x-auto rounded-xl border border-stone-200 bg-white shadow-sm">
              <table className="w-full text-sm">
                <thead className="bg-stone-50 text-left">
                  <tr>
                    <Th>User</Th>
                    <Th>Role</Th>
                    <Th>Status</Th>
                    <Th>Signed up</Th>
                    <Th>Last login</Th>
                    <Th className="text-right">Actions</Th>
                  </tr>
                </thead>
                <tbody>
                  {state.kind === "loading" &&
                    Array.from({ length: 4 }).map((_, i) => (
                      <tr key={i} className="border-t border-stone-200">
                        {Array.from({ length: 6 }).map((_, j) => (
                          <td key={j} className="px-3 py-3">
                            <div className="h-4 w-full max-w-[180px] animate-pulse rounded bg-stone-100" />
                          </td>
                        ))}
                      </tr>
                    ))}
                  {state.kind === "ok" && filtered.length === 0 && (
                    <tr>
                      <td colSpan={6} className="px-4 py-10 text-center text-sm text-stone-600">
                        No users match this filter.
                      </td>
                    </tr>
                  )}
                  {state.kind === "ok" &&
                    filtered.map((u) => {
                      const isPending = u.status === "pending";
                      const isActive = u.status === "active";
                      const isDeact = u.status === "deactivated";
                      const approveKey = `approve:${u.id}`;
                      const deactivateKey = `deactivate:${u.id}`;
                      const roleKey = `role:${u.id}`;
                      return (
                        <tr
                          key={u.id}
                          className={`border-t border-stone-200 transition-colors ${
                            isDeact ? "bg-stone-50/60 text-stone-500" : "hover:bg-stone-50"
                          }`}
                        >
                          <td className="px-3 py-3 align-top">
                            <div className="flex items-start gap-3">
                              <div
                                aria-hidden
                                className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-stone-900 text-[11px] font-bold uppercase tracking-wider text-white"
                              >
                                {(u.display_name || u.username || "?").slice(0, 2)}
                              </div>
                              <div className="min-w-0">
                                <div className="truncate font-semibold text-stone-950">
                                  {u.display_name || u.username}
                                </div>
                                <div className="truncate font-mono text-[11px] text-stone-500">
                                  {u.email}
                                </div>
                                {u.intended_use && (
                                  <div className="mt-1 max-w-md truncate text-[12px] text-stone-600">
                                    {u.intended_use}
                                  </div>
                                )}
                              </div>
                            </div>
                          </td>
                          <td className="px-3 py-3 align-top">
                            <RoleBadge role={u.role} />
                          </td>
                          <td className="px-3 py-3 align-top">
                            <StatusBadge status={u.status} />
                          </td>
                          <td className="px-3 py-3 align-top text-xs text-stone-700 tabular-nums">
                            {formatDate(u.created_at)}
                          </td>
                          <td className="px-3 py-3 align-top text-xs text-stone-700 tabular-nums">
                            {u.last_login_at ? formatDate(u.last_login_at) : <span className="text-stone-400">never</span>}
                          </td>
                          <td className="px-3 py-3 align-top">
                            <div className="flex flex-wrap items-center justify-end gap-1.5">
                              {isPending && (
                                <ActionButton
                                  variant="primary"
                                  loading={pendingAction === approveKey}
                                  onClick={() =>
                                    runAction("Approve", u.id, () => approveAdminUser(u.id))
                                  }
                                >
                                  Approve
                                </ActionButton>
                              )}
                              <RoleSelect
                                current={u.role}
                                disabled={isDeact || pendingAction === roleKey}
                                onChange={(next) =>
                                  runAction("Set role", u.id, () => setAdminUserRole(u.id, next))
                                }
                              />
                              {isActive && (
                                <ActionButton
                                  variant="danger"
                                  loading={pendingAction === deactivateKey}
                                  onClick={() =>
                                    runAction("Deactivate", u.id, () =>
                                      deactivateAdminUser(u.id),
                                    )
                                  }
                                >
                                  Deactivate
                                </ActionButton>
                              )}
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                </tbody>
              </table>
            </div>

            <p className="text-xs text-stone-500">
              Role escalations and deactivations are append-only events; the
              audit log (FARM-AUTH.9) preserves the actor, target and timestamp.
            </p>
          </>
        )}
      </div>
    </>
  );
}

function Th({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <th
      scope="col"
      className={`px-3 py-2 text-xs font-semibold uppercase tracking-wider text-stone-500 ${className ?? ""}`}
    >
      {children}
    </th>
  );
}

function ActionButton({
  children,
  loading,
  onClick,
  variant,
}: {
  children: React.ReactNode;
  loading: boolean;
  onClick: () => void;
  variant: "primary" | "danger";
}) {
  const palette =
    variant === "primary"
      ? "bg-blue-700 text-white hover:bg-blue-800 disabled:bg-blue-300"
      : "border border-rose-300 bg-white text-rose-700 hover:bg-rose-50 disabled:bg-stone-100 disabled:text-stone-400";
  return (
    <button
      type="button"
      disabled={loading}
      onClick={onClick}
      className={`inline-flex items-center rounded-md px-2.5 py-1 text-[12px] font-semibold transition-colors disabled:cursor-not-allowed ${palette}`}
    >
      {loading ? "Working" : children}
    </button>
  );
}

function RoleSelect({
  current,
  disabled,
  onChange,
}: {
  current: UserRole;
  disabled: boolean;
  onChange: (next: UserRole) => void;
}) {
  return (
    <label className="inline-flex items-center gap-1.5 text-[11px] uppercase tracking-wider text-stone-500">
      <span className="sr-only">Role</span>
      <select
        value={current}
        disabled={disabled}
        onChange={(e) => {
          const next = e.target.value as UserRole;
          if (next !== current) onChange(next);
        }}
        className="rounded-md border border-stone-300 bg-white px-2 py-1 text-[12px] font-semibold text-stone-800 shadow-sm focus:border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-200 disabled:cursor-not-allowed disabled:bg-stone-100 disabled:text-stone-400"
      >
        <option value="viewer">viewer</option>
        <option value="researcher">researcher</option>
        <option value="operator">operator</option>
        <option value="admin">admin</option>
      </select>
    </label>
  );
}

function BackendMissingPanel({ status }: { status: number }) {
  return (
    <div className="rounded-xl border border-stone-200 bg-white p-6 shadow-sm">
      <div className="flex items-start gap-3">
        <div
          aria-hidden
          className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-amber-50 text-amber-700 ring-1 ring-inset ring-amber-200"
        >
          <svg className="h-5 w-5" fill="none" stroke="currentColor" strokeWidth="1.8" viewBox="0 0 24 24" aria-hidden>
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m0 3.75h.008v.008H12v-.008zM10.5 5.25h3a1.5 1.5 0 011.5 1.5v11.5a1.5 1.5 0 01-1.5 1.5h-3a1.5 1.5 0 01-1.5-1.5V6.75a1.5 1.5 0 011.5-1.5z" />
          </svg>
        </div>
        <div className="min-w-0">
          <h2 className="text-base font-semibold text-stone-950">
            Admin user-management endpoints have not shipped yet
          </h2>
          <p className="mt-1 text-sm text-stone-600">
            <code className="font-mono">GET /v1/admin/users</code> returned HTTP {status} on
            this server. The FARM-AUTH.4 / FARM-AUTH.5 sweep ships the
            FastAPI routers that back this page (list, approve,
            deactivate, change role). Once they merge this page starts
            working with no extra deploy.
          </p>
          <p className="mt-2 text-xs text-stone-500">
            The frontend wiring is already complete — it just needs the
            backend half. Sign-up, sign-in, profile and the sidebar gate
            are functional today.
          </p>
        </div>
      </div>
    </div>
  );
}

function ForbiddenPanel({ locale, role }: { locale: string; role: UserRole }) {
  return (
    <>
      <Breadcrumbs />
      <div className="mx-auto max-w-xl px-4 sm:px-6 py-12">
        <div
          role="alert"
          className="rounded-xl border border-stone-200 bg-white p-6 text-center shadow-sm"
        >
          <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-stone-100 text-stone-600">
            <svg className="h-6 w-6" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24" aria-hidden>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
            </svg>
          </div>
          <h1 className="text-lg font-semibold text-stone-900">Admin only</h1>
          <p className="mt-1 text-sm text-stone-600">
            User-management controls are reserved for accounts with the admin role.
          </p>
          <p className="mt-2 text-xs text-stone-500">
            Current role: <span className="font-mono">{role}</span>
          </p>
          <Link
            href={`/${locale}`}
            className="mt-5 inline-flex items-center gap-1.5 rounded-md bg-stone-900 px-4 py-2 text-sm font-semibold text-white hover:bg-stone-800"
          >
            Back home
          </Link>
        </div>
      </div>
    </>
  );
}
