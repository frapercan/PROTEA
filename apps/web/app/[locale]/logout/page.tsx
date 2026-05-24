"use client";

import { useEffect, useState } from "react";
import { useLocale } from "next-intl";
import { logout } from "@/lib/authApi";
import { Breadcrumbs } from "@/components/Breadcrumbs";

/**
 * FARM-AUTH.10 — sign-out landing.
 *
 * Calls ``POST /api-proxy/v1/auth/logout`` once on mount (the backend
 * clears the ``protea_session`` cookie + invalidates the in-memory
 * session entry; FARM-AUTH.8 will swap that for a real revocation
 * table). On success we navigate hard to ``/login`` so chrome that
 * reads the cookie at SSR sees the cleared state.
 *
 * If the logout call fails we still navigate, because the more common
 * failure mode is "no cookie was present in the first place" and the
 * UX shouldn't strand the user on a perpetual loading screen.
 */
export default function LogoutPage() {
  const locale = useLocale();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        await logout();
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e));
        }
      } finally {
        if (!cancelled) {
          // Brief pause so the visual confirmation registers; otherwise
          // the page flickers past faster than a user can read it.
          setTimeout(() => {
            window.location.assign(`/${locale}/login`);
          }, 600);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [locale]);

  return (
    <>
      <Breadcrumbs />
      <div className="mx-auto max-w-md px-4 sm:px-6 py-16">
        <div className="rounded-xl border border-stone-200 bg-white p-8 text-center shadow-sm">
          <div className="mx-auto mb-3 h-8 w-8 animate-spin rounded-full border-2 border-stone-200 border-t-blue-700" aria-hidden />
          <p className="text-sm font-semibold text-stone-900">Signing you out</p>
          <p className="mt-1 text-xs text-stone-500">
            Clearing the session cookie. You will be returned to the sign-in page.
          </p>
          {error && (
            <p className="mt-3 text-xs text-rose-700">
              The logout request did not complete cleanly ({error}). The cookie
              will still be cleared when you reach the sign-in page.
            </p>
          )}
        </div>
      </div>
    </>
  );
}
