"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const ROUTE_LABELS: Record<string, string> = {
  "functional-annotation": "Functional Annotation",
  proteins: "Proteins",
  jobs: "Jobs",
  embeddings: "Embeddings",
  annotations: "Annotations",
  evaluation: "Evaluation",
  scoring: "Scoring",
  reranker: "Re-ranker",
  "query-sets": "Query Sets",
  maintenance: "Maintenance",
  benchmark: "Benchmark",
  stack: "Stack",
  support: "Support",
};

export function Breadcrumbs() {
  const pathname = usePathname();
  // Remove locale prefix
  const stripped = pathname.replace(/^\/[a-z]{2}(?=\/|$)/, "") || "/";
  const segments = stripped.split("/").filter(Boolean);

  if (segments.length < 2) return null;

  const crumbs: { label: string; href: string }[] = [
    { label: "Home", href: "/" },
  ];

  let path = "";
  for (let i = 0; i < segments.length; i++) {
    path += `/${segments[i]}`;
    const isLast = i === segments.length - 1;
    const label = ROUTE_LABELS[segments[i]] ?? (
      isLast && segments[i].length > 12
        ? `${segments[i].slice(0, 8)}...`
        : segments[i]
    );
    crumbs.push({ label, href: path });
  }

  return (
    <nav className="flex items-center flex-wrap gap-1.5 text-sm text-slate-500 mb-5" aria-label="Breadcrumb">
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
  );
}
