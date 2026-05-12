"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState, useEffect, useRef, useCallback } from "react";
import { useLocale, useTranslations } from "next-intl";
import { baseUrl } from "@/lib/api";

/**
 * Primary navigation for PROTEA.
 *
 * Information architecture is intent-first, not table-first:
 *
 *   ── Annotate     primary CTA, links to /functional-annotation;
 *                   on the homepage the same brand bar exposes a
 *                   #annotate-form anchor for the quick path.
 *
 *   ── Pipeline     the actual pipeline stages (Embeddings → KNN
 *                   Search → Scoring → Re-ranker) in process order,
 *                   each with a domain-fluent caption.
 *
 *   ── Reference data
 *                   the inputs/outputs of the pipeline that live in
 *                   the DB (Proteins, GO Annotations, Query Sets).
 *
 *   ── Results      Benchmark matrix + CAFA Evaluation runs.
 *
 *   ── Operations   Jobs, Maintenance, Stack — the observability
 *                   surface for the running system.
 *
 *   ── Docs         Sphinx manual, OpenAPI/Swagger, Thesis PDF,
 *                   Support — promoted out of the thin footer link
 *                   row that used to host them.
 *
 * The right cluster (SystemStatusPill, AuthChip, CommandPalette, …)
 * is rendered by the layout itself; this component only owns the
 * left/centre navigation.
 *
 * Mobile (< 1024px) renders a single drawer with the same groupings.
 */

type NavItem = {
  href: string;        // path or external URL
  label: string;
  hint?: string;       // 1-line caption shown in the dropdown
  external?: boolean;
  badge?: string;
};

type NavGroup = {
  id: string;
  title: string;
  hint?: string;
  items: NavItem[];
};

// ──────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────

function stripLocale(pathname: string): string {
  return pathname.replace(/^\/[a-z]{2}(?=\/|$)/, "") || "/";
}

function isInternalActive(stripped: string, href: string): boolean {
  if (href.startsWith("http") || href.startsWith("/sphinx") || href.startsWith("/thesis")) {
    return false;
  }
  return stripped === href || stripped.startsWith(href + "/");
}

// ──────────────────────────────────────────────────────────────────
// Dropdown
// ──────────────────────────────────────────────────────────────────

function GroupTrigger({
  group,
  open,
  groupActive,
  onToggle,
  buttonId,
  menuId,
}: {
  group: NavGroup;
  open: boolean;
  groupActive: boolean;
  onToggle: () => void;
  buttonId: string;
  menuId: string;
}) {
  return (
    <button
      id={buttonId}
      onClick={onToggle}
      aria-haspopup="menu"
      aria-expanded={open}
      aria-controls={menuId}
      aria-current={groupActive ? "page" : undefined}
      className={`relative flex items-center gap-1.5 px-3 h-10 rounded-lg text-[14px] font-medium transition-colors ${
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
        aria-hidden="true"
      >
        <path d="M3 4.5l3 3 3-3" />
      </svg>
      {groupActive && (
        <span className="absolute left-3 right-3 -bottom-[1px] h-[2px] rounded-full bg-blue-600" />
      )}
    </button>
  );
}

function DropdownGroup({
  group,
  stripped,
  locale,
  openId,
  onOpenChange,
}: {
  group: NavGroup;
  stripped: string;
  locale: string;
  openId: string | null;
  onOpenChange: (id: string | null) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const open = openId === group.id;
  const groupActive = group.items.some((item) => isInternalActive(stripped, item.href));
  const buttonId = `nav-trigger-${group.id}`;
  const menuId = `nav-menu-${group.id}`;

  // Close on click outside.
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onOpenChange(null);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open, onOpenChange]);

  // Close on Escape.
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onOpenChange(null);
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open, onOpenChange]);

  return (
    <div ref={ref} className="relative">
      <GroupTrigger
        group={group}
        open={open}
        groupActive={groupActive}
        onToggle={() => onOpenChange(open ? null : group.id)}
        buttonId={buttonId}
        menuId={menuId}
      />
      {open && (
        <div
          id={menuId}
          role="menu"
          aria-labelledby={buttonId}
          className="absolute top-full left-0 mt-2 py-2 bg-white rounded-2xl border border-slate-200 shadow-xl z-50 min-w-[260px] max-w-[320px] animate-[fadeIn_120ms_ease-out]"
        >
          <div className="px-4 pb-2 mb-1 border-b border-slate-100">
            <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-600">
              {group.title}
            </p>
            {group.hint && (
              <p className="text-[11px] text-slate-600 mt-1 leading-snug">{group.hint}</p>
            )}
          </div>
          <ul className="py-0.5">
            {group.items.map((item) => {
              const active = isInternalActive(stripped, item.href);
              const href = item.external ? item.href : `/${locale}${item.href}`;
              return (
                <li key={item.href}>
                  <Link
                    href={href}
                    role="menuitem"
                    aria-current={active ? "page" : undefined}
                    onClick={() => onOpenChange(null)}
                    target={item.external ? "_blank" : undefined}
                    rel={item.external ? "noopener noreferrer" : undefined}
                    className={`flex items-start gap-2.5 mx-1.5 px-3 py-2 rounded-lg text-[13px] transition-colors ${
                      active
                        ? "bg-blue-50 text-blue-800"
                        : "text-slate-700 hover:bg-slate-50 hover:text-slate-900"
                    }`}
                  >
                    <span
                      aria-hidden
                      className={`mt-1.5 h-1.5 w-1.5 rounded-full shrink-0 transition-colors ${
                        active ? "bg-blue-600" : "bg-slate-300 group-hover:bg-slate-400"
                      }`}
                    />
                    <span className="min-w-0 flex-1">
                      <span className="flex items-center gap-1.5">
                        <span className={active ? "font-semibold" : "font-medium"}>{item.label}</span>
                        {item.external && (
                          <span aria-hidden className="text-[10px] text-slate-400">↗</span>
                        )}
                        {item.badge && (
                          <span className="text-[9px] uppercase tracking-wider bg-slate-100 text-slate-600 rounded px-1 py-0.5">
                            {item.badge}
                          </span>
                        )}
                      </span>
                      {item.hint && (
                        <span className="block text-[11px] text-slate-500 mt-0.5 leading-snug">
                          {item.hint}
                        </span>
                      )}
                    </span>
                  </Link>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────
// Public component
// ──────────────────────────────────────────────────────────────────

export function NavLinks({ mobileExtras }: { mobileExtras?: React.ReactNode }) {
  const t = useTranslations("nav");
  const pathname = usePathname();
  const locale = useLocale();
  const [open, setOpen] = useState(false);
  const [openGroupId, setOpenGroupId] = useState<string | null>(null);

  // Build groups. `t` is stable across renders for the same locale, so a
  // memo would only pay off if we passed it down to many leaves — we don't.
  const onHome = stripLocale(pathname) === "/";
  const annotateHref = onHome
    ? `/${locale}#annotate-form`
    : `/${locale}/functional-annotation`;

  const swaggerHref = `${baseUrl()}/docs`;

  const NAV_GROUPS: NavGroup[] = [
    {
      id: "pipeline",
      title: t("pipelineGroup"),
      hint: t("pipelineHint"),
      items: [
        { href: "/embeddings",            label: t("embeddings"),           hint: "PLM embedding configs · ESM-2 · ESM3c · ProstT5 · Ankh" },
        { href: "/functional-annotation", label: t("functionalAnnotation"), hint: "Embedding-similarity GO annotation, BPO / MFO / CCO" },
        { href: "/scoring",               label: t("scoring"),              hint: "Combine distance, alignment, taxonomy, evidence" },
        { href: "/reranker",              label: t("reranker"),             hint: "LightGBM reranker over scored predictions" },
      ],
    },
    {
      id: "data",
      title: t("data"),
      hint: t("dataHint"),
      items: [
        { href: "/proteins",   label: t("proteins"),   hint: "UniProt entries · Swiss-Prot + TrEMBL, isoforms" },
        { href: "/annotations", label: t("annotations"), hint: "GO ontology snapshots and ground-truth GAF / QuickGO sets" },
        { href: "/query-sets", label: t("querySets"),  hint: "FASTA uploads grouped for batch runs" },
      ],
    },
    {
      id: "results",
      title: t("results"),
      hint: t("resultsHint"),
      items: [
        { href: "/benchmark",  label: t("benchmark"),  hint: "Fmax matrix across embedding × stage × NK / LK / PK" },
        { href: "/evaluation", label: t("evaluation"), hint: "CAFA-style delta evaluation (Fmax, Smin, coverage)" },
      ],
    },
    {
      id: "operations",
      title: t("system"),
      hint: t("operationsHint"),
      items: [
        { href: "/jobs",        label: t("jobs"),        hint: "Live job queue and event audit trail" },
        { href: "/maintenance", label: t("maintenance"), hint: "Vacuum orphan sequences and unindexed embeddings" },
        { href: "/stack",       label: t("stack"),       hint: "Eight repositories, open PRs, deploy targets" },
      ],
    },
    {
      id: "docs",
      title: t("docs"),
      items: [
        { href: "/sphinx/",           label: t("sphinx"),  hint: t("sphinxHint"),  external: true },
        { href: swaggerHref,          label: t("swagger"), hint: t("swaggerHint"), external: true },
        { href: "/thesis.pdf",        label: t("thesis"),  hint: t("thesisHint"),  external: true },
        { href: "/support",           label: t("support"), hint: t("supportHint") },
      ],
    },
  ];

  // Close mobile drawer on route change.
  useEffect(() => {
    setOpen(false);
    setOpenGroupId(null);
  }, [pathname]);

  // Body scroll lock while the mobile drawer is open. Restore the previous
  // overflow on close so we don't fight any other modal that may have set it.
  useEffect(() => {
    if (typeof document === "undefined") return;
    if (!open) return;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previous;
    };
  }, [open]);

  // Group-state coordinator: only one dropdown open at a time.
  const handleGroupChange = useCallback((id: string | null) => {
    setOpenGroupId(id);
  }, []);

  const stripped = stripLocale(pathname);
  const annotateActive = stripped.startsWith("/functional-annotation");

  return (
    <>
      {/* ── Desktop ──────────────────────────────────────────────── */}
      <nav
        aria-label={t("ariaPrimary")}
        className="hidden lg:flex items-center gap-1"
      >
        {/* Primary CTA: Annotate. Domain-fluent label, blue pill, lives left
            of the dropdown groups so it always reads first. */}
        <Link
          href={annotateHref}
          aria-current={annotateActive ? "page" : undefined}
          title={t("annotateTooltip")}
          className={`relative inline-flex items-center gap-1.5 h-9 px-3.5 rounded-full text-[13px] font-semibold transition-all ${
            annotateActive
              ? "bg-blue-700 text-white shadow-sm shadow-blue-600/30"
              : "bg-blue-600 text-white hover:bg-blue-700 hover:shadow-md hover:shadow-blue-600/20"
          }`}
        >
          <svg
            className="w-3.5 h-3.5"
            fill="none"
            stroke="currentColor"
            strokeWidth="2.2"
            viewBox="0 0 12 12"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <path d="M2 6h8M6.5 2l3.5 4-3.5 4" />
          </svg>
          <span>{t("annotate")}</span>
        </Link>

        <span className="mx-1 h-5 w-px bg-slate-200" aria-hidden />

        {NAV_GROUPS.map((group) => (
          <DropdownGroup
            key={group.id}
            group={group}
            stripped={stripped}
            locale={locale}
            openId={openGroupId}
            onOpenChange={handleGroupChange}
          />
        ))}
      </nav>

      {/* ── Mobile trigger ───────────────────────────────────────── */}
      <button
        className="lg:hidden ml-auto flex flex-col justify-center items-center w-11 h-11 gap-1.5 rounded-lg text-slate-600 hover:bg-slate-100 transition-colors"
        onClick={() => setOpen((v) => !v)}
        aria-label={open ? t("closeMenu") : t("openMenu")}
        aria-expanded={open}
        aria-controls="protea-mobile-nav"
      >
        <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "rotate-45 translate-y-2" : ""}`} />
        <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "opacity-0" : ""}`} />
        <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "-rotate-45 -translate-y-2" : ""}`} />
      </button>

      {/* ── Mobile drawer ────────────────────────────────────────── */}
      {open && (
        <div
          id="protea-mobile-nav"
          className="lg:hidden absolute left-0 right-0 top-full z-50 border-b border-slate-200 bg-white shadow-xl max-h-[calc(100vh-4rem)] overflow-y-auto"
        >
          <nav className="px-4 py-4 flex flex-col gap-1" aria-label={t("ariaPrimary")}>
            {/* Primary CTA always at the very top */}
            <Link
              href={annotateHref}
              className={`mb-2 flex items-center justify-between gap-3 px-4 py-3 rounded-xl font-semibold text-[14px] transition-colors ${
                annotateActive
                  ? "bg-blue-700 text-white"
                  : "bg-blue-600 text-white hover:bg-blue-700"
              }`}
            >
              <span className="flex items-center gap-2">
                <svg
                  className="w-4 h-4"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2.2"
                  viewBox="0 0 12 12"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  aria-hidden="true"
                >
                  <path d="M2 6h8M6.5 2l3.5 4-3.5 4" />
                </svg>
                {t("annotate")}
              </span>
              <span className="text-[11px] opacity-80 font-normal text-right max-w-[60%]">
                {t("annotateTooltip")}
              </span>
            </Link>

            {NAV_GROUPS.map((group, gi) => (
              <section
                key={group.id}
                className={gi > 0 ? "mt-3 pt-3 border-t border-slate-100" : ""}
                aria-labelledby={`mobile-nav-${group.id}`}
              >
                <h2
                  id={`mobile-nav-${group.id}`}
                  className="px-3 py-1.5 text-[10px] font-semibold text-slate-600 uppercase tracking-[0.14em]"
                >
                  {group.title}
                </h2>
                {group.hint && (
                  <p className="px-3 pb-1.5 text-[11px] text-slate-500 leading-snug">{group.hint}</p>
                )}
                <ul>
                  {group.items.map((item) => {
                    const active = isInternalActive(stripped, item.href);
                    const href = item.external ? item.href : `/${locale}${item.href}`;
                    return (
                      <li key={item.href}>
                        <Link
                          href={href}
                          target={item.external ? "_blank" : undefined}
                          rel={item.external ? "noopener noreferrer" : undefined}
                          aria-current={active ? "page" : undefined}
                          className={`flex items-start gap-2.5 px-3 py-2.5 rounded-lg text-[14px] transition-colors ${
                            active
                              ? "font-semibold text-blue-700 bg-blue-50"
                              : "text-slate-700 hover:bg-slate-50"
                          }`}
                        >
                          <span
                            className={`mt-2 h-1.5 w-1.5 rounded-full shrink-0 ${
                              active ? "bg-blue-600" : "bg-slate-300"
                            }`}
                            aria-hidden
                          />
                          <span className="min-w-0 flex-1">
                            <span className="flex items-center gap-1.5">
                              {item.label}
                              {item.external && (
                                <span aria-hidden className="text-[10px] text-slate-400">↗</span>
                              )}
                            </span>
                            {item.hint && (
                              <span className="block text-[11px] text-slate-500 mt-0.5 leading-snug">
                                {item.hint}
                              </span>
                            )}
                          </span>
                        </Link>
                      </li>
                    );
                  })}
                </ul>
              </section>
            ))}
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
