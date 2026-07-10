"use client";

/**
 * Route-level error boundary for every page under `[locale]`.
 *
 * Next renders this when a page (or a component below it) throws while
 * rendering. It runs inside the locale layout, so the sidebar and chrome
 * stay in place and `next-intl` translations are available.
 *
 * The goal is to teach the operator what happened, not to hide it. When
 * the thrown value is an `ApiError` (client-thrown, class intact) the
 * boundary reads its `kind`, `status`, and `path` and shows targeted
 * guidance: sign in, ask for a role, or check the backend. Errors thrown
 * inside server components reach here as a plain message plus a `digest`
 * (Next strips the class over the RSC boundary); the digest is shown as a
 * reference the operator can quote from the server logs.
 */

import { useEffect } from "react";
import Link from "next/link";
import { useLocale, useTranslations } from "next-intl";
import { RotateCw, Home, LogIn, AlertTriangle } from "lucide-react";
import { ApiError } from "@/lib/api";

type ApiErrorShape = { kind: ApiError["kind"]; status: number; path: string };

// Client-thrown errors keep their class; server-thrown ones arrive as a
// plain Error with only message + digest. Read the ApiError fields off
// whichever we got, defensively (the class identity can differ across
// bundle chunks, so duck-type rather than rely on instanceof alone).
function readApiError(error: Error): ApiErrorShape | null {
  const e = error as Partial<ApiErrorShape> & { name?: string };
  if (
    (error instanceof ApiError || e.name === "ApiError") &&
    typeof e.status === "number" &&
    typeof e.path === "string" &&
    typeof e.kind === "string"
  ) {
    return { kind: e.kind, status: e.status, path: e.path };
  }
  return null;
}

export default function RouteError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  const t = useTranslations("errors");
  const locale = useLocale();

  // Surface the failure in the browser console for the operator who has
  // devtools open. This is a report channel, not the handling: the UI
  // below is what actually recovers the user.
  useEffect(() => {
    console.error("[route-error]", error);
  }, [error]);

  const api = readApiError(error);
  const kind = api?.kind;

  let title = t("title");
  let body = t("body");
  if (kind === "unauthorized") {
    title = t("unauthorizedTitle");
    body = t("unauthorizedBody");
  } else if (kind === "forbidden") {
    title = t("forbiddenTitle");
    body = t("forbiddenBody");
  } else if (kind === "network") {
    title = t("networkTitle");
    body = t("networkBody");
  }

  return (
    <div className="mx-auto flex max-w-2xl flex-col items-start gap-6 py-10">
      <div className="flex items-center gap-3">
        <span className="flex h-11 w-11 items-center justify-center rounded-2xl bg-amber-100 text-amber-700">
          <AlertTriangle className="h-6 w-6" aria-hidden />
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900">{title}</h1>
      </div>

      <p className="text-base leading-relaxed text-slate-600">{body}</p>

      <dl className="w-full space-y-2 rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm">
        {api && (
          <>
            <div className="flex gap-3">
              <dt className="w-28 shrink-0 font-medium text-slate-500">{t("endpointLabel")}</dt>
              <dd className="min-w-0 break-all font-mono text-slate-800">{api.path}</dd>
            </div>
            <div className="flex gap-3">
              <dt className="w-28 shrink-0 font-medium text-slate-500">{t("statusLabel")}</dt>
              <dd className="font-mono text-slate-800">{api.status}</dd>
            </div>
          </>
        )}
        {error.message && (
          <div className="flex gap-3">
            <dt className="w-28 shrink-0 font-medium text-slate-500">{t("messageLabel")}</dt>
            <dd className="min-w-0 break-words text-slate-800">{error.message}</dd>
          </div>
        )}
        {error.digest && (
          <div className="flex gap-3">
            <dt className="w-28 shrink-0 font-medium text-slate-500">{t("referenceLabel")}</dt>
            <dd className="font-mono text-slate-800">{error.digest}</dd>
          </div>
        )}
      </dl>

      <div className="flex flex-wrap items-center gap-3">
        <button
          type="button"
          onClick={reset}
          className="inline-flex items-center gap-2 rounded-xl bg-blue-600 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-blue-700"
        >
          <RotateCw className="h-4 w-4" aria-hidden />
          {t("retry")}
        </button>
        {kind === "unauthorized" ? (
          <Link
            href={`/${locale}/login`}
            className="inline-flex items-center gap-2 rounded-xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 transition hover:bg-slate-50"
          >
            <LogIn className="h-4 w-4" aria-hidden />
            {t("signIn")}
          </Link>
        ) : null}
        <Link
          href={`/${locale}`}
          className="inline-flex items-center gap-2 rounded-xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 transition hover:bg-slate-50"
        >
          <Home className="h-4 w-4" aria-hidden />
          {t("home")}
        </Link>
      </div>
    </div>
  );
}
