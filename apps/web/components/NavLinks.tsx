"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState, useEffect, useRef } from "react";
import { DocLinks } from "./DocLinks";
import { useTranslations } from "next-intl";

type NavItem = { href: string; label: string };
type NavGroup = { title: string; items: NavItem[] };

function DropdownGroup({ group, pathname }: { group: NavGroup; pathname: string }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const stripped = pathname.replace(/^\/[a-z]{2}(?=\/|$)/, "") || "/";
  const groupActive = group.items.some(
    ({ href }) => stripped === href || stripped.startsWith(href + "/")
  );

  // Close on click outside
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className={`flex items-center gap-1 px-2 py-1 rounded transition-colors text-sm ${
          groupActive ? "font-semibold text-blue-600" : "text-gray-500 hover:text-gray-900"
        }`}
      >
        {group.title}
        <svg className={`w-3 h-3 transition-transform ${open ? "rotate-180" : ""}`} fill="none" viewBox="0 0 12 12" stroke="currentColor" strokeWidth="2">
          <path d="M3 4.5l3 3 3-3" />
        </svg>
      </button>
      {open && (
        <div className="absolute top-full left-0 mt-1 py-1 bg-white rounded-lg border shadow-lg z-50 min-w-[180px]">
          {group.items.map(({ href, label }) => {
            const active = stripped === href || stripped.startsWith(href + "/");
            return (
              <Link
                key={href}
                href={href}
                onClick={() => setOpen(false)}
                className={`block px-4 py-2 text-sm transition-colors ${
                  active
                    ? "font-semibold text-blue-600 bg-blue-50"
                    : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                }`}
              >
                {label}
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function NavLinks({ mobileExtras }: { mobileExtras?: React.ReactNode }) {
  const t = useTranslations("nav");
  const pathname = usePathname();
  const [open, setOpen] = useState(false);

  const NAV_GROUPS: NavGroup[] = [
    {
      title: t("data" as any),
      items: [
        { href: "/proteins", label: t("proteins") },
        { href: "/annotations", label: t("annotations") },
        { href: "/query-sets", label: t("querySets") },
      ],
    },
    {
      title: t("pipelineGroup" as any),
      items: [
        { href: "/embeddings", label: t("embeddings") },
        { href: "/functional-annotation", label: t("functionalAnnotation") },
        { href: "/reranker", label: t("reranker") },
      ],
    },
    {
      title: t("results" as any),
      items: [
        { href: "/evaluation", label: t("evaluation") },
        { href: "/scoring", label: t("scoring") },
      ],
    },
    {
      title: t("system" as any),
      items: [
        { href: "/jobs", label: t("jobs") },
        { href: "/maintenance", label: t("maintenance") },
      ],
    },
  ];

  const ALL_LINKS = NAV_GROUPS.flatMap((g) => g.items);

  // Close menu on route change
  useEffect(() => { setOpen(false); }, [pathname]);

  return (
    <>
      {/* Desktop nav */}
      <nav className="hidden lg:flex items-center gap-1 text-sm">
        {NAV_GROUPS.map((group, gi) => (
          <span key={gi} className="flex items-center">
            {gi > 0 && <span className="mx-1.5 text-gray-200">|</span>}
            <DropdownGroup group={group} pathname={pathname} />
          </span>
        ))}
        <span className="mx-1.5 text-gray-200">|</span>
        <DocLinks />
      </nav>

      {/* Mobile hamburger */}
      <button
        className="lg:hidden flex flex-col justify-center items-center w-10 h-10 gap-1.5 rounded text-gray-600 hover:bg-gray-100 transition-colors"
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
          <nav className="px-4 py-3 flex flex-col gap-0.5">
            {NAV_GROUPS.map((group, gi) => (
              <div key={gi}>
                {gi > 0 && <div className="border-t my-1" />}
                <div className="px-3 py-1.5 text-xs font-semibold text-gray-400 uppercase tracking-wider">
                  {group.title}
                </div>
                {group.items.map(({ href, label }) => {
                  const stripped = pathname.replace(/^\/[a-z]{2}(?=\/|$)/, "") || "/";
                  const active = stripped === href || stripped.startsWith(href + "/");
                  return (
                    <Link
                      key={href}
                      href={href}
                      className={`block px-3 py-2 rounded-md text-sm transition-colors ${
                        active
                          ? "font-semibold text-blue-600 bg-blue-50"
                          : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                      }`}
                    >
                      {label}
                    </Link>
                  );
                })}
              </div>
            ))}
            <div className="mt-1 pt-2 border-t flex gap-4 px-3 text-sm text-gray-500">
              <DocLinks />
            </div>
            {mobileExtras && (
              <div className="mt-1 pt-2 border-t px-3 pb-1 flex items-center justify-between gap-3">
                {mobileExtras}
              </div>
            )}
          </nav>
        </div>
      )}
    </>
  );
}
