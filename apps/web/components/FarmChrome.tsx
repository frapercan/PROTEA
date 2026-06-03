"use client";

// FARM-UI.3 mount-gate for the farm-scope chrome. The locale layout is
// shared across every route in the app, so we conditionally render the
// farm-only floating widget and status pill only when the user is on
// a /[locale]/farm/* page. The pill itself is intentionally lightweight
// even when mounted everywhere, but isolating it here keeps the chrome
// uncluttered outside the farm scope and prevents the sidecar from
// being polled by users who never visit /farm.

import { usePathname } from "next/navigation";
import { FarmJobsWidget } from "@/components/FarmJobsWidget";
import { FarmStatusPill } from "@/components/FarmStatusPill";

function isFarmRoute(pathname: string | null): boolean {
  if (!pathname) return false;
  // Next.js locale prefix sits at /[locale]/farm. We accept both the
  // section root and any descendant so that /farm/<task_id>,
  // /farm/plan, /farm/<anything> all trigger the chrome.
  return /^\/[a-z]{2}\/farm(\/|$)/.test(pathname);
}

type FarmChromeProps = {
  slot: "pill" | "widget";
};

export function FarmChrome({ slot }: FarmChromeProps) {
  const pathname = usePathname();
  if (!isFarmRoute(pathname)) return null;
  return slot === "pill" ? <FarmStatusPill /> : <FarmJobsWidget />;
}
