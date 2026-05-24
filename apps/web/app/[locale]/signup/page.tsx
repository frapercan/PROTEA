"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useLocale } from "next-intl";
import { type FormEvent, useId, useState } from "react";
import { AuthError, signup } from "@/lib/authApi";
import { Breadcrumbs } from "@/components/Breadcrumbs";

/**
 * FARM-AUTH.10 — signup form.
 *
 * Posts to ``POST /api-proxy/v1/auth/signup`` (FARM-AUTH.3). On 201
 * the new account is in ``status=pending`` until an admin approves
 * it; we redirect to ``/auth/pending-approval`` instead of attempting
 * a login that would fail with 403 anyway.
 *
 * Field grammar mirrors ``SignupRequest`` in
 * ``protea/api/routers/auth_user.py`` exactly. We deliberately do
 * NOT pre-validate the password client-side beyond ``minLength=8`` so
 * the backend remains the single source of truth (any strength bump
 * we add later only needs to land in the FastAPI validator).
 */

export default function SignupPage() {
  const locale = useLocale();
  const router = useRouter();
  const formId = useId();

  const [email, setEmail] = useState("");
  const [username, setUsername] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [password, setPassword] = useState("");
  const [intendedUse, setIntendedUse] = useState("");

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await signup({
        email: email.trim().toLowerCase(),
        username: username.trim(),
        display_name: displayName.trim() ? displayName.trim() : null,
        password,
        intended_use: intendedUse.trim() ? intendedUse.trim() : null,
      });
      router.push(`/${locale}/auth/pending-approval`);
    } catch (err) {
      if (err instanceof AuthError) {
        if (err.status === 409 && err.detail === "email_already_registered") {
          setError("An account with this email already exists. Try signing in instead.");
        } else if (err.status === 409 && err.detail === "username_already_taken") {
          setError("That username is already taken. Please pick another one.");
        } else if (err.status === 409) {
          setError("This account already exists.");
        } else {
          setError(err.detail || `Signup failed (HTTP ${err.status}).`);
        }
      } else {
        setError(err instanceof Error ? err.message : "Unexpected error.");
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
          <h1 className="text-2xl font-bold tracking-tight text-stone-950">Create a PROTEA account</h1>
          <p className="mt-2 text-sm text-stone-600">
            Tell us a little about why you need access. An admin reviews every
            new account before activation.
          </p>
        </header>

        <form
          id={formId}
          onSubmit={onSubmit}
          className="mt-8 space-y-4 rounded-xl border border-stone-200 bg-white p-6 shadow-sm"
          noValidate
        >
          <Field label="Email" htmlFor={`${formId}-email`} required>
            <input
              id={`${formId}-email`}
              type="email"
              autoComplete="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full rounded-md border border-stone-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-200"
            />
          </Field>

          <Field
            label="Username"
            htmlFor={`${formId}-username`}
            required
            hint="Shown on shared resources (jobs, audit log). Letters, digits, dashes."
          >
            <input
              id={`${formId}-username`}
              type="text"
              autoComplete="username"
              required
              minLength={1}
              maxLength={100}
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full rounded-md border border-stone-300 bg-white px-3 py-2 font-mono text-sm shadow-sm focus:border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-200"
            />
          </Field>

          <Field label="Password" htmlFor={`${formId}-password`} required hint="Minimum 8 characters.">
            <input
              id={`${formId}-password`}
              type="password"
              autoComplete="new-password"
              required
              minLength={8}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full rounded-md border border-stone-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-200"
            />
          </Field>

          <Field label="Display name" htmlFor={`${formId}-display`} hint="Optional human-readable name.">
            <input
              id={`${formId}-display`}
              type="text"
              autoComplete="name"
              maxLength={255}
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              className="w-full rounded-md border border-stone-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-200"
            />
          </Field>

          <Field
            label="Intended use"
            htmlFor={`${formId}-intent`}
            hint="What you plan to do with PROTEA. Helps admins approve faster."
          >
            <textarea
              id={`${formId}-intent`}
              rows={3}
              value={intendedUse}
              onChange={(e) => setIntendedUse(e.target.value)}
              placeholder="e.g. Benchmarking GO-term predictors on CAFA-style splits."
              className="w-full rounded-md border border-stone-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-200"
            />
          </Field>

          {error && (
            <div
              role="alert"
              className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800"
            >
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={submitting}
            className="w-full rounded-md bg-blue-800 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition-colors hover:bg-blue-900 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {submitting ? "Submitting account request" : "Request an account"}
          </button>
        </form>

        <p className="mt-6 text-center text-sm text-stone-600">
          Already have an account?{" "}
          <Link href={`/${locale}/login`} className="font-semibold text-blue-800 hover:text-blue-900">
            Sign in
          </Link>
        </p>
      </div>
    </>
  );
}

function Field({
  label,
  htmlFor,
  required,
  hint,
  children,
}: {
  label: string;
  htmlFor: string;
  required?: boolean;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <label
        htmlFor={htmlFor}
        className="flex items-baseline justify-between text-xs font-semibold uppercase tracking-wider text-stone-700"
      >
        <span>
          {label}
          {required && <span aria-hidden className="ml-0.5 text-rose-600">*</span>}
        </span>
        {!required && <span className="text-[10px] font-normal normal-case text-stone-400">optional</span>}
      </label>
      {children}
      {hint && <p className="mt-1 text-[12px] leading-snug text-stone-500">{hint}</p>}
    </div>
  );
}
