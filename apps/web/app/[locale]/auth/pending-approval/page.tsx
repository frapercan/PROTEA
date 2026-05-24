"use client";

import Link from "next/link";
import { useLocale } from "next-intl";
import { Breadcrumbs } from "@/components/Breadcrumbs";

/**
 * FARM-AUTH.10 — post-signup "thanks, wait for approval" landing.
 *
 * The backend creates the user in ``status=pending`` (ADR D37) so a
 * login attempt right after signup would 403 with
 * ``account_pending_approval``. Telling the user that up front is far
 * less confusing than a generic error toast on the login screen.
 */
export default function PendingApprovalPage() {
  const locale = useLocale();
  return (
    <>
      <Breadcrumbs />
      <div className="mx-auto max-w-md px-4 sm:px-6 py-12">
        <div className="rounded-xl border border-stone-200 bg-white p-8 text-center shadow-sm">
          <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-full bg-amber-50 ring-1 ring-inset ring-amber-200">
            <svg
              className="h-7 w-7 text-amber-700"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.8"
              viewBox="0 0 24 24"
              aria-hidden
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M12 8v4l2.5 2.5M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
          </div>
          <h1 className="text-xl font-bold tracking-tight text-stone-950">
            Account pending admin approval
          </h1>
          <p className="mt-2 text-sm text-stone-600">
            Your account request has been recorded. An administrator will
            review your intended use and activate the account. You will be
            able to sign in once that happens.
          </p>
          <p className="mt-4 text-xs text-stone-500">
            Approvals are usually processed within a working day. If you
            have not heard back after that, please ping the project
            maintainer through the documentation contact.
          </p>
          <div className="mt-6 flex justify-center gap-3">
            <Link
              href={`/${locale}`}
              className="rounded-md border border-stone-300 bg-white px-4 py-2 text-sm font-semibold text-stone-700 transition-colors hover:bg-stone-50"
            >
              Back home
            </Link>
            <Link
              href={`/${locale}/login`}
              className="rounded-md bg-stone-900 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-stone-800"
            >
              Go to sign in
            </Link>
          </div>
        </div>
      </div>
    </>
  );
}
