"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState, useEffect } from "react";
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
    { href: "/scoring", label: "Scoring" },
    { href: "/evaluation", label: "Evaluation" },
  ],
  [
    { href: "/jobs", label: "Jobs" },
    { href: "/maintenance", label: "Maintenance" },
  ],
];

const ALL_LINKS = NAV_GROUPS.flat();

export function NavLinks() {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);

  // Close menu on route change
  useEffect(() => { setOpen(false); }, [pathname]);

  return (
    <>
      {/* Desktop nav */}
      <nav className="hidden lg:flex items-center gap-1 text-sm">
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

      {/* Mobile hamburger */}
      <button
        className="lg:hidden flex flex-col justify-center items-center w-8 h-8 gap-1.5 rounded text-gray-600 hover:bg-gray-100 transition-colors"
        onClick={() => setOpen((v) => !v)}
        aria-label="Toggle menu"
      >
        <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "rotate-45 translate-y-2" : ""}`} />
        <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "opacity-0" : ""}`} />
        <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "-rotate-45 -translate-y-2" : ""}`} />
      </button>

      {/* Mobile dropdown */}
      {open && (
        <div className="lg:hidden absolute left-0 right-0 top-full z-50 border-b bg-white shadow-lg">
          <nav className="px-4 py-3 flex flex-col gap-1">
            {ALL_LINKS.map(({ href, label }) => {
              const active = pathname === href || pathname.startsWith(href + "/");
              return (
                <Link
                  key={href}
                  href={href}
                  className={`px-3 py-2.5 rounded-md text-sm transition-colors ${
                    active
                      ? "font-semibold text-blue-600 bg-blue-50"
                      : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                  }`}
                >
                  {label}
                </Link>
              );
            })}
            <div className="mt-1 pt-2 border-t flex gap-4 px-3 text-sm text-gray-500">
              <DocLinks />
            </div>
          </nav>
        </div>
      )}
    </>
  );
}
