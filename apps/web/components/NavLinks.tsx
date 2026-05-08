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
        aria-haspopup="menu"
        aria-expanded={open}
        aria-current={groupActive ? "page" : undefined}
        className={`relative flex items-center gap-1.5 px-3 h-10 rounded-lg text-[14px] font-medium transition-all ${
          groupActive
            ? "text-blue-700 bg-blue-50/70"
            : open
              ? "text-slate-900 bg-slate-100/80"
              : "text-slate-600 hover:text-slate-900 hover:bg-slate-100/70"
        }`}
      >
        {group.title}
        <svg
          className={`w-3.5 h-3.5 transition-transform duration-200 ${open ? "rotate-180" : ""}`}
          fill="none"
          viewBox="0 0 12 12"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="M3 4.5l3 3 3-3" />
        </svg>
        {groupActive && (
          <span className="absolute left-3 right-3 -bottom-[1px] h-[2px] rounded-full bg-blue-600" />
        )}
      </button>
      {open && (
        <div
          role="menu"
          aria-label={group.title}
          className="absolute top-full left-0 mt-2 py-2 bg-white rounded-xl border border-slate-200 shadow-xl z-50 min-w-[220px] animate-[fadeIn_120ms_ease-out]"
        >
          <div className="px-3 pb-1.5 mb-1 border-b border-slate-100">
            <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-400">
              {group.title}
            </p>
          </div>
          {group.items.map(({ href, label }) => {
            const active = stripped === href || stripped.startsWith(href + "/");
            return (
              <Link
                key={href}
                href={href}
                role="menuitem"
                aria-current={active ? "page" : undefined}
                onClick={() => setOpen(false)}
                className={`flex items-center gap-2 mx-1.5 px-3 py-2 rounded-lg text-[13px] transition-colors ${
                  active
                    ? "font-semibold text-blue-700 bg-blue-50"
                    : "text-slate-600 hover:bg-slate-50 hover:text-slate-900"
                }`}
              >
                <span
                  aria-hidden
                  className={`h-1.5 w-1.5 rounded-full transition-colors ${
                    active ? "bg-blue-600" : "bg-slate-300"
                  }`}
                />
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
        { href: "/benchmark", label: t("benchmark" as any) },
        { href: "/evaluation", label: t("evaluation") },
        { href: "/scoring", label: t("scoring") },
      ],
    },
    {
      title: t("system" as any),
      items: [
        { href: "/jobs", label: t("jobs") },
        { href: "/maintenance", label: t("maintenance") },
        { href: "/stack", label: t("stack" as any) },
      ],
    },
  ];

  // Close menu on route change
  useEffect(() => { setOpen(false); }, [pathname]);

  // Body scroll lock while the mobile menu is open. Restore the user's
  // previous overflow on close so we don't fight other modals.
  useEffect(() => {
    if (typeof document === "undefined") return;
    if (!open) return;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previous;
    };
  }, [open]);

  return (
    <>
      {/* Desktop nav */}
      <nav className="hidden lg:flex items-center gap-1">
        {NAV_GROUPS.map((group, gi) => (
          <DropdownGroup key={gi} group={group} pathname={pathname} />
        ))}
        <span className="mx-2 h-5 w-px bg-slate-200" />
        <div className="flex items-center gap-1 text-[13px] text-slate-500">
          <DocLinks />
        </div>
      </nav>

      {/* Mobile hamburger */}
      <button
        className="lg:hidden ml-auto flex flex-col justify-center items-center w-11 h-11 gap-1.5 rounded-lg text-slate-600 hover:bg-slate-100 transition-colors"
        onClick={() => setOpen((v) => !v)}
        aria-label="Toggle menu"
        aria-expanded={open}
      >
        <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "rotate-45 translate-y-2" : ""}`} />
        <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "opacity-0" : ""}`} />
        <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "-rotate-45 -translate-y-2" : ""}`} />
      </button>

      {/* Mobile dropdown */}
      {open && (
        <div className="lg:hidden absolute left-0 right-0 top-full z-50 border-b border-slate-200 bg-white shadow-xl max-h-[calc(100vh-4rem)] overflow-y-auto">
          <nav className="px-4 py-4 flex flex-col gap-1">
            {NAV_GROUPS.map((group, gi) => (
              <div key={gi} className={gi > 0 ? "mt-3 pt-3 border-t border-slate-100" : ""}>
                <div className="px-3 py-1.5 text-[10px] font-semibold text-slate-400 uppercase tracking-[0.14em]">
                  {group.title}
                </div>
                {group.items.map(({ href, label }) => {
                  const stripped = pathname.replace(/^\/[a-z]{2}(?=\/|$)/, "") || "/";
                  const active = stripped === href || stripped.startsWith(href + "/");
                  return (
                    <Link
                      key={href}
                      href={href}
                      className={`flex items-center gap-2.5 px-3 py-2.5 rounded-lg text-[14px] transition-colors ${
                        active
                          ? "font-semibold text-blue-700 bg-blue-50"
                          : "text-slate-700 hover:bg-slate-50"
                      }`}
                    >
                      <span
                        className={`h-1.5 w-1.5 rounded-full ${
                          active ? "bg-blue-600" : "bg-slate-300"
                        }`}
                      />
                      {label}
                    </Link>
                  );
                })}
              </div>
            ))}
            <div className="mt-3 pt-3 border-t border-slate-100 flex gap-5 px-3 text-[13px] text-slate-500">
              <DocLinks />
            </div>
            {mobileExtras && (
              <div className="mt-3 pt-3 border-t border-slate-100 px-3 pb-1 flex items-center justify-between gap-3">
                {mobileExtras}
              </div>
            )}
          </nav>
        </div>
      )}
    </>
  );
}
