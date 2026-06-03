"use client";

// UX-ADMIN-AUDIT P0-FRM-1 (2026-05-26) — client-side session gate that
// wraps every page under /[locale]/farm/*. Anonymous visitors are sent
// to /login with a ``?next=...`` round-trip so they bounce back to the
// farm view they originally requested once they sign in. While the
// session check resolves we render a minimal skeleton so signed-in
// operators do not flash through a "sign in" panel on first paint.
//
// Why a client component instead of middleware: the existing
// middleware.ts already gates /admin and /maintenance at the edge,
// but /farm/* serves the same surface to operator and viewer roles
// alike. Anonymous (no cookie) is the only thing we must block here.
// Doing it at the layout level keeps the chrome/redirect logic in one
// place and avoids broadening the middleware matcher for a soft gate.

import Link from "next/link";
import { useRouter, usePathname, useSearchParams } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";
import { useEffect, useState } from "react";
import { useIsAuthenticated } from "@/lib/useRole";

export function FarmAuthGate({ children }: { children: React.ReactNode }) {
  const locale = useLocale();
  const t = useTranslations("farm.authGate");
  const router = useRouter();
  const pathname = usePathname();
  const search = useSearchParams();
  const authed = useIsAuthenticated();

  // Mounted flag: useIsAuthenticated returns false during SSR and then
  // re-reads the cookie inside useEffect. Without this flag we would
  // briefly render the "Sign in" panel for genuine signed-in users.
  const [hydrated, setHydrated] = useState(false);
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setHydrated(true);
  }, []);

  useEffect(() => {
    if (!hydrated) return;
    if (authed) return;
    // Build next= with the current path + query so the login flow can
    // return the user to exactly where they were. URLSearchParams are
    // already URL-encoded by the SearchParams API; we only encode the
    // composite value once for the redirect target.
    const qs = search?.toString();
    const target = qs ? `${pathname}?${qs}` : pathname;
    router.replace(`/${locale}/login?next=${encodeURIComponent(target)}`);
  }, [hydrated, authed, locale, pathname, search, router]);

  if (!hydrated) {
    return (
      <div
        aria-busy
        className="mx-auto max-w-5xl px-4 sm:px-6 py-8 space-y-3"
        data-testid="farm-auth-gate-loading"
      >
        <div className="h-7 w-48 animate-pulse rounded bg-slate-200" />
        <div className="h-4 w-72 animate-pulse rounded bg-slate-200/70" />
        <div className="h-64 animate-pulse rounded-xl bg-slate-100" />
      </div>
    );
  }

  if (!authed) {
    // Inline fallback in case the redirect is delayed (slow router /
    // disabled JS / programmatic navigation suppressed). Keeps the
    // farm contents out of the DOM no matter what.
    const qs = search?.toString();
    const target = qs ? `${pathname}?${qs}` : pathname;
    return (
      <main
        className="mx-auto max-w-md px-4 sm:px-6 py-12"
        data-testid="farm-auth-gate-anonymous"
      >
        <div className="rounded-xl border border-stone-200 bg-white p-6 text-center shadow-sm">
          <h1 className="text-lg font-semibold text-stone-950">{t("title")}</h1>
          <p className="mt-2 text-sm text-stone-600">{t("body")}</p>
          <Link
            href={`/${locale}/login?next=${encodeURIComponent(target)}`}
            className="mt-5 inline-flex items-center gap-1.5 rounded-md bg-blue-800 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-900"
          >
            {t("signIn")}
          </Link>
        </div>
      </main>
    );
  }

  return <>{children}</>;
}
