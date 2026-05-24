"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useLocale } from "next-intl";
import { type FormEvent, useId, useState } from "react";
import { AuthError, login } from "@/lib/authApi";
import { Breadcrumbs } from "@/components/Breadcrumbs";

/**
 * FARM-AUTH.10 — email + password sign-in form.
 *
 * Posts to ``POST /api-proxy/v1/auth/login`` (FARM-AUTH.3). On 200
 * the API sets ``protea_session`` (HttpOnly + SameSite=strict) and we
 * push the user back to ``?next=...`` if present, otherwise home.
 *
 * Backend detail codes (see ``protea/api/routers/auth_user.py``):
 *
 *   401 invalid_credentials       — bad email or wrong password
 *   403 account_pending_approval  — signup not yet approved
 *   403 account_deactivated       — admin-disabled
 *   503 (no message)              — JWT_SECRET missing on the server
 */
export default function LoginPage() {
  const locale = useLocale();
  const search = useSearchParams();
  const next = search.get("next") ?? `/${locale}`;
  const formId = useId();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<{ tone: "warn" | "error"; text: string } | null>(null);

  async function onSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await login({ email: email.trim().toLowerCase(), password });
      // Hard navigation so server components that read the session
      // cookie (Sidebar admin gate, /me prefetch on profile) re-render
      // with the fresh cookie instead of inheriting the anonymous SSR.
      window.location.assign(next);
    } catch (err) {
      if (err instanceof AuthError) {
        if (err.status === 401) {
          setError({ tone: "error", text: "Invalid email or password." });
        } else if (err.status === 403 && err.detail === "account_pending_approval") {
          setError({
            tone: "warn",
            text:
              "Your account is still pending admin approval. You will be able to sign in once it has been activated.",
          });
        } else if (err.status === 403 && err.detail === "account_deactivated") {
          setError({
            tone: "warn",
            text: "This account has been deactivated. Contact an administrator to re-enable it.",
          });
        } else if (err.status === 503) {
          setError({
            tone: "error",
            text: "Sign-in is not available on this server (JWT secret is not configured).",
          });
        } else {
          setError({ tone: "error", text: err.detail || `Sign in failed (HTTP ${err.status}).` });
        }
      } else {
        setError({ tone: "error", text: err instanceof Error ? err.message : "Unexpected error." });
      }
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <>
      <Breadcrumbs />
      <div className="mx-auto max-w-md px-4 sm:px-6 py-8">
        <header className="text-center">
          <h1 className="text-2xl font-bold tracking-tight text-stone-950">Sign in to PROTEA</h1>
          <p className="mt-2 text-sm text-stone-600">
            Use the email and password you registered with.
          </p>
        </header>

        <form
          id={formId}
          onSubmit={onSubmit}
          className="mt-8 space-y-4 rounded-xl border border-stone-200 bg-white p-6 shadow-sm"
          noValidate
        >
          <div>
            <label
              htmlFor={`${formId}-email`}
              className="text-xs font-semibold uppercase tracking-wider text-stone-700"
            >
              Email
            </label>
            <input
              id={`${formId}-email`}
              type="email"
              autoComplete="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full rounded-md border border-stone-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-200"
            />
          </div>

          <div>
            <label
              htmlFor={`${formId}-password`}
              className="text-xs font-semibold uppercase tracking-wider text-stone-700"
            >
              Password
            </label>
            <input
              id={`${formId}-password`}
              type="password"
              autoComplete="current-password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full rounded-md border border-stone-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-200"
            />
          </div>

          {error && (
            <div
              role="alert"
              className={`rounded-md border px-3 py-2 text-sm ${
                error.tone === "warn"
                  ? "border-amber-200 bg-amber-50 text-amber-900"
                  : "border-rose-200 bg-rose-50 text-rose-800"
              }`}
            >
              {error.text}
            </div>
          )}

          <button
            type="submit"
            disabled={submitting}
            className="w-full rounded-md bg-blue-800 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition-colors hover:bg-blue-900 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {submitting ? "Signing in" : "Sign in"}
          </button>
        </form>

        <p className="mt-6 text-center text-sm text-stone-600">
          Need an account?{" "}
          <Link href={`/${locale}/signup`} className="font-semibold text-blue-800 hover:text-blue-900">
            Request one
          </Link>
        </p>
      </div>
    </>
  );
}
