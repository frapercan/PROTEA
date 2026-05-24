"use client";

import Link from "next/link";
import { useLocale } from "next-intl";
import { useEffect, useState } from "react";
import { AuthError, me, type UserMe } from "@/lib/authApi";
import { Breadcrumbs } from "@/components/Breadcrumbs";

/**
 * FARM-AUTH.10 — current-user profile page.
 *
 * Reads ``GET /api-proxy/v1/auth/me`` which surfaces the User row
 * for the session-cookie holder. The display-name edit affordance is
 * intentionally read-only at this slice: the FARM-AUTH.3 backend
 * exposes no ``PATCH /auth/me`` endpoint and the plan explicitly
 * forbids stubbing a fake mutation. When the update endpoint lands
 * (tracked as a follow-up auth slice) the existing layout already
 * has the "Save" affordance wired through the disabled prop — the
 * one-line diff is enabling the form + POST'ing the JSON body.
 */
type LoadState =
  | { kind: "loading" }
  | { kind: "anonymous" }
  | { kind: "error"; message: string }
  | { kind: "ready"; user: UserMe };

function roleBadge(role: UserMe["role"]): { bg: string; text: string; ring: string; dot: string; label: string } {
  switch (role) {
    case "admin":
      return { bg: "bg-amber-100", text: "text-amber-800", ring: "ring-amber-200", dot: "bg-amber-500", label: "Admin" };
    case "operator":
      return { bg: "bg-blue-100", text: "text-blue-800", ring: "ring-blue-200", dot: "bg-blue-500", label: "Operator" };
    case "researcher":
      return { bg: "bg-emerald-50", text: "text-emerald-800", ring: "ring-emerald-200", dot: "bg-emerald-500", label: "Researcher" };
    default:
      return { bg: "bg-stone-100", text: "text-stone-700", ring: "ring-stone-200", dot: "bg-stone-400", label: "Viewer" };
  }
}

function statusBadge(status: UserMe["status"]): { bg: string; text: string; ring: string; dot: string; label: string } {
  switch (status) {
    case "active":
      return { bg: "bg-emerald-50", text: "text-emerald-700", ring: "ring-emerald-200", dot: "bg-emerald-500", label: "Active" };
    case "pending":
      return { bg: "bg-amber-50", text: "text-amber-800", ring: "ring-amber-200", dot: "bg-amber-500", label: "Pending approval" };
    case "deactivated":
      return { bg: "bg-rose-50", text: "text-rose-800", ring: "ring-rose-200", dot: "bg-rose-500", label: "Deactivated" };
  }
}

export default function ProfilePage() {
  const locale = useLocale();
  const [state, setState] = useState<LoadState>({ kind: "loading" });

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const user = await me();
        if (!cancelled) setState({ kind: "ready", user });
      } catch (e) {
        if (cancelled) return;
        if (e instanceof AuthError && e.status === 401) {
          setState({ kind: "anonymous" });
        } else {
          setState({ kind: "error", message: e instanceof Error ? e.message : String(e) });
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  if (state.kind === "loading") {
    return (
      <>
        <Breadcrumbs />
        <div className="mx-auto max-w-3xl px-4 sm:px-6 py-8 space-y-4">
          <div className="h-8 w-48 animate-pulse rounded bg-stone-200" />
          <div className="h-32 animate-pulse rounded-xl bg-stone-100" />
        </div>
      </>
    );
  }

  if (state.kind === "anonymous") {
    return (
      <>
        <Breadcrumbs />
        <div className="mx-auto max-w-md px-4 sm:px-6 py-12">
          <div className="rounded-xl border border-stone-200 bg-white p-6 text-center shadow-sm">
            <h1 className="text-lg font-semibold text-stone-950">Sign in to view your profile</h1>
            <p className="mt-2 text-sm text-stone-600">
              Your account details, role and status appear here once you are signed in.
            </p>
            <div className="mt-5 flex justify-center gap-3">
              <Link
                href={`/${locale}/login?next=${encodeURIComponent(`/${locale}/profile`)}`}
                className="rounded-md bg-blue-800 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-900"
              >
                Sign in
              </Link>
              <Link
                href={`/${locale}/signup`}
                className="rounded-md border border-stone-300 bg-white px-4 py-2 text-sm font-semibold text-stone-700 hover:bg-stone-50"
              >
                Create account
              </Link>
            </div>
          </div>
        </div>
      </>
    );
  }

  if (state.kind === "error") {
    return (
      <>
        <Breadcrumbs />
        <div className="mx-auto max-w-2xl px-4 sm:px-6 py-12">
          <div
            role="alert"
            className="rounded-md border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800"
          >
            Could not load your profile: {state.message}
          </div>
        </div>
      </>
    );
  }

  const u = state.user;
  const role = roleBadge(u.role);
  const status = statusBadge(u.status);

  return (
    <>
      <Breadcrumbs />
      <div className="mx-auto max-w-3xl px-4 sm:px-6 py-8 space-y-8">
        <header className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight text-stone-950">Your profile</h1>
            <p className="mt-1 text-sm text-stone-600">
              Account details for {u.display_name || u.username}.
            </p>
          </div>
          <Link
            href={`/${locale}/logout`}
            className="self-start rounded-md border border-stone-300 bg-white px-3 py-1.5 text-sm font-semibold text-stone-700 transition-colors hover:bg-stone-50"
          >
            Sign out
          </Link>
        </header>

        <section className="rounded-xl border border-stone-200 bg-white shadow-sm overflow-hidden">
          <div className="flex items-center gap-4 border-b border-stone-100 px-6 py-5">
            <div
              aria-hidden
              className="flex h-14 w-14 shrink-0 items-center justify-center rounded-full bg-stone-900 text-lg font-bold uppercase tracking-wider text-white"
            >
              {(u.display_name || u.username || "?").slice(0, 2)}
            </div>
            <div className="min-w-0 flex-1">
              <p className="truncate text-base font-semibold text-stone-950">
                {u.display_name || u.username}
              </p>
              <p className="truncate text-sm text-stone-600">{u.email}</p>
            </div>
            <div className="flex shrink-0 flex-col items-end gap-1.5">
              <span
                className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-[11px] font-semibold uppercase tracking-wider ring-1 ring-inset ${role.bg} ${role.text} ${role.ring}`}
              >
                <span aria-hidden className={`h-1.5 w-1.5 rounded-full ${role.dot}`} />
                {role.label}
              </span>
              <span
                className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-[11px] font-medium ring-1 ring-inset ${status.bg} ${status.text} ${status.ring}`}
              >
                <span aria-hidden className={`h-1.5 w-1.5 rounded-full ${status.dot}`} />
                {status.label}
              </span>
            </div>
          </div>

          <dl className="grid grid-cols-1 gap-px bg-stone-200 sm:grid-cols-2">
            <Field label="Username" value={u.username} mono />
            <Field label="Email" value={u.email} />
            <Field label="User ID" value={u.id} mono compact />
            <Field label="Role" value={role.label} />
          </dl>
        </section>

        <section className="rounded-xl border border-stone-200 bg-white p-6 shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-sm font-semibold uppercase tracking-wider text-stone-700">
                Display name
              </h2>
              <p className="mt-1 text-sm text-stone-600">
                Shown next to your jobs, audit-log entries and shared resources.
              </p>
            </div>
          </div>
          <div className="mt-4 flex flex-col gap-2 sm:flex-row sm:items-center">
            <input
              type="text"
              value={u.display_name ?? ""}
              readOnly
              aria-readonly
              className="w-full rounded-md border border-stone-200 bg-stone-50 px-3 py-2 text-sm text-stone-700 cursor-not-allowed"
              placeholder="(none set)"
            />
            <button
              type="button"
              disabled
              title="The PATCH /auth/me endpoint is not yet wired. Display name edits land in a follow-up auth slice."
              className="rounded-md border border-stone-300 bg-stone-100 px-4 py-2 text-sm font-semibold text-stone-500 cursor-not-allowed"
            >
              Save
            </button>
          </div>
          <p className="mt-2 text-[11px] text-stone-500">
            Editing is disabled in this build (no profile-update endpoint on the
            current server). It will activate automatically once the backend
            exposes the corresponding mutation.
          </p>
        </section>
      </div>
    </>
  );
}

function Field({
  label,
  value,
  mono,
  compact,
}: {
  label: string;
  value: string;
  mono?: boolean;
  compact?: boolean;
}) {
  return (
    <div className="bg-white px-6 py-4">
      <dt className="text-[11px] font-semibold uppercase tracking-wider text-stone-500">
        {label}
      </dt>
      <dd
        className={`mt-1 ${mono ? "font-mono" : ""} ${compact ? "text-[12px]" : "text-sm"} break-words text-stone-900`}
      >
        {value}
      </dd>
    </div>
  );
}
