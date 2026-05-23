"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";

type NavKey =
  | "home"
  | "functionalAnnotation"
  | "proteins"
  | "jobs"
  | "embeddings"
  | "annotations"
  | "evaluation"
  | "scoring"
  | "reranker"
  | "datasets"
  | "querySets"
  | "maintenance"
  | "benchmark"
  | "stack"
  | "support"
  | "farmSection"
  | "farmPlan"
  | "farmCost"
<<<<<<< HEAD
  | "adminGroup"
  | "apiKeys";
=======
  | "admin"
  | "experimentRuns";
>>>>>>> 3db057c (feat(web): admin page for ExperimentRun CRUD (F-EXP narrative spine))

// Map URL segments to `nav` translation keys. Segments without an entry here
// are rendered verbatim (typically dynamic ids like /jobs/<uuid>).
// FARM-UI.9 P1.2 added farm/plan/cost so the agent-farm section displays
// translated breadcrumb labels instead of the raw URL segment.
const SEGMENT_TO_NAV_KEY: Record<string, NavKey> = {
  "functional-annotation": "functionalAnnotation",
  proteins: "proteins",
  jobs: "jobs",
  embeddings: "embeddings",
  annotations: "annotations",
  evaluation: "evaluation",
  scoring: "scoring",
  reranker: "reranker",
  datasets: "datasets",
  "query-sets": "querySets",
  maintenance: "maintenance",
  benchmark: "benchmark",
  stack: "stack",
  support: "support",
  farm: "farmSection",
  plan: "farmPlan",
  cost: "farmCost",
<<<<<<< HEAD
  admin: "adminGroup",
  "api-keys": "apiKeys",
=======
  admin: "admin",
  "experiment-runs": "experimentRuns",
>>>>>>> 3db057c (feat(web): admin page for ExperimentRun CRUD (F-EXP narrative spine))
};

export function Breadcrumbs() {
  const pathname = usePathname();
  const locale = useLocale();
  const t = useTranslations("nav");
  // Remove locale prefix
  const stripped = pathname.replace(/^\/[a-z]{2}(?=\/|$)/, "") || "/";
  const segments = stripped.split("/").filter(Boolean);

  // Render empty on home (/) and on top-level routes.
  if (segments.length < 2) return null;

  const crumbs: { label: string; href: string }[] = [
    { label: t("home"), href: `/${locale}` },
  ];

  let path = `/${locale}`;
  for (let i = 0; i < segments.length; i++) {
    path += `/${segments[i]}`;
    const isLast = i === segments.length - 1;
    const navKey = SEGMENT_TO_NAV_KEY[segments[i]];
    const label = navKey
      ? t(navKey)
      : isLast && segments[i].length > 12
        ? `${segments[i].slice(0, 8)}...`
        : segments[i];
    crumbs.push({ label, href: path });
  }

  return (
    <div className="w-full max-w-screen-2xl px-4 sm:px-6 lg:px-8 pt-4 sm:pt-5 lg:pt-6">
      <nav className="flex items-center flex-wrap gap-1.5 text-sm text-slate-500" aria-label="Breadcrumb">
        {crumbs.map((crumb, i) => {
          const isLast = i === crumbs.length - 1;
          return (
            <span key={crumb.href} className="flex items-center gap-1.5">
              {i > 0 && (
                <svg
                  className="w-3.5 h-3.5 text-slate-300"
                  fill="none"
                  viewBox="0 0 14 14"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <path d="M5 3l4 4-4 4" />
                </svg>
              )}
              {isLast ? (
                <span className="text-slate-900 font-semibold">{crumb.label}</span>
              ) : (
                <Link
                  href={crumb.href}
                  className="rounded-md px-1.5 py-0.5 hover:bg-slate-100 hover:text-slate-700 transition-colors"
                >
                  {crumb.label}
                </Link>
              )}
            </span>
          );
        })}
      </nav>
    </div>
  );
}
