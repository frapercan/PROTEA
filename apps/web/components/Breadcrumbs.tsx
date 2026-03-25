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
    <nav className="flex items-center gap-1 text-xs text-gray-400 mb-3" aria-label="Breadcrumb">
      {crumbs.map((crumb, i) => (
        <span key={crumb.href} className="flex items-center gap-1">
          {i > 0 && <span>/</span>}
          {i < crumbs.length - 1 ? (
            <Link href={crumb.href} className="hover:text-gray-600 transition-colors">
              {crumb.label}
            </Link>
          ) : (
            <span className="text-gray-600 font-medium">{crumb.label}</span>
          )}
        </span>
      ))}
    </nav>
  );
}
