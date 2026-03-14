"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { DocLinks } from "./DocLinks";

const NAV_GROUPS = [
  [
    { href: "/proteins", label: "Proteins" },
    { href: "/annotations", label: "Annotations" },
    { href: "/query-sets", label: "Query Sets" },
  ],
  [
    { href: "/embeddings", label: "Embeddings" },
    { href: "/functional-annotation", label: "Functional Annotation" },
    { href: "/evaluation", label: "Evaluation" },
  ],
  [
    { href: "/jobs", label: "Jobs" },
    { href: "/maintenance", label: "Maintenance" },
  ],
];

export function NavLinks() {
  const pathname = usePathname();
  return (
    <nav className="flex items-center gap-1 text-sm">
      {NAV_GROUPS.map((group, gi) => (
        <span key={gi} className="flex items-center gap-1">
          {gi > 0 && <span className="mx-2 text-gray-200">|</span>}
          {group.map(({ href, label }) => {
            const active = pathname === href || pathname.startsWith(href + "/");
            return (
              <Link
                key={href}
                href={href}
                className={`px-2 py-1 rounded transition-colors ${
                  active
                    ? "font-semibold text-blue-600"
                    : "text-gray-500 hover:text-gray-900"
                }`}
              >
                {label}
              </Link>
            );
          })}
        </span>
      ))}
      <span className="mx-2 text-gray-200">|</span>
      <DocLinks />
    </nav>
  );
}
