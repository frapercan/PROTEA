"use client";

import Link from "next/link";
import Image from "next/image";
import { usePathname } from "next/navigation";
import { useState, useEffect, useRef, useCallback } from "react";
import { useLocale, useTranslations } from "next-intl";
import { baseUrl } from "@/lib/api";

/**
 * Primary navigation for PROTEA, rendered as a LEFT SIDEBAR rail.
 *
 * Information architecture (intent-first, mirrors the prior top-nav):
 *
 *   Pipeline        the actual pipeline stages (Embeddings, KNN Search,
 *                   Scoring, Re-ranker) in process order.
 *   Reference data  Proteins, GO Annotations, Query Sets.
 *   Results         Benchmark matrix + CAFA Evaluation.
 *   Operations      Jobs, Maintenance, Stack.
 *   Docs            Sphinx manual, OpenAPI/Swagger, Thesis PDF, Support.
 *
 * Desktop (>= lg): a sticky, full-height, scrollable rail. The brand and
 * the Annotate CTA sit at the top, the grouped sections fill the body, and
 * a utility footer (extras) sits at the bottom.
 *
 * Mobile (< lg): the rail collapses to a hamburger that opens a slide-in
 * drawer (overlay + Esc/backdrop close + focus trap). The hamburger and the
 * compact brand live in the slim mobile top bar (rendered by the layout).
 */

type NavItem = {
  href: string; // path or external URL
  label: string;
  hint?: string;
  external?: boolean;
  badge?: string;
};

type NavGroup = {
  id: string;
  title: string;
  hint?: string;
  items: NavItem[];
};

function stripLocale(pathname: string): string {
  return pathname.replace(/^\/[a-z]{2}(?=\/|$)/, "") || "/";
}

function isInternalActive(stripped: string, href: string): boolean {
  if (href.startsWith("http") || href.startsWith("/sphinx") || href.startsWith("/thesis")) {
    return false;
  }
  return stripped === href || stripped.startsWith(href + "/");
}

const ArrowIcon = () => (
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
);

/**
 * The shared body of the rail: brand block, Annotate CTA, grouped sections,
 * and the utility footer. Used verbatim by both the desktop rail and the
 * mobile drawer so the two stay in sync.
 */
function RailContent({
  groups,
  stripped,
  locale,
  annotateHref,
  annotateActive,
  onNavigate,
  extras,
  t,
}: {
  groups: NavGroup[];
  stripped: string;
  locale: string;
  annotateHref: string;
  annotateActive: boolean;
  onNavigate?: () => void;
  extras?: React.ReactNode;
  t: ReturnType<typeof useTranslations>;
}) {
  return (
    <div className="flex h-full flex-col">
      {/* Annotate CTA */}
      <div className="px-3 pt-3">
        <Link
          href={annotateHref}
          onClick={onNavigate}
          aria-current={annotateActive ? "page" : undefined}
          title={t("annotateTooltip")}
          className={`flex items-center gap-2 rounded-xl px-3 py-2.5 text-[13px] font-semibold transition-all ${
            annotateActive
              ? "bg-blue-700 text-white shadow-sm shadow-blue-600/30"
              : "bg-blue-600 text-white hover:bg-blue-700 hover:shadow-md hover:shadow-blue-600/20"
          }`}
        >
          <ArrowIcon />
          <span>{t("annotate")}</span>
        </Link>
      </div>

      {/* Grouped sections */}
      <nav
        aria-label={t("ariaPrimary")}
        className="flex-1 overflow-y-auto px-3 py-3"
      >
        {groups.map((group, gi) => (
          <section
            key={group.id}
            className={gi > 0 ? "mt-4 pt-4 border-t border-slate-200/70" : ""}
            aria-labelledby={`sidebar-group-${group.id}`}
          >
            <h2
              id={`sidebar-group-${group.id}`}
              className="px-3 pb-1.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500"
            >
              {group.title}
            </h2>
            <ul className="space-y-0.5">
              {group.items.map((item) => {
                const active = isInternalActive(stripped, item.href);
                const href = item.external ? item.href : `/${locale}${item.href}`;
                return (
                  <li key={item.href}>
                    <Link
                      href={href}
                      onClick={onNavigate}
                      target={item.external ? "_blank" : undefined}
                      rel={item.external ? "noopener noreferrer" : undefined}
                      aria-current={active ? "page" : undefined}
                      title={item.hint}
                      className={`group/link relative flex items-center gap-2.5 rounded-lg px-3 py-2 text-[13.5px] transition-colors ${
                        active
                          ? "bg-blue-50 font-semibold text-blue-800"
                          : "text-slate-700 hover:bg-slate-100/80 hover:text-slate-900"
                      }`}
                    >
                      {active && (
                        <span
                          aria-hidden
                          className="absolute left-0 top-1.5 bottom-1.5 w-[3px] rounded-full bg-blue-600"
                        />
                      )}
                      <span
                        aria-hidden
                        className={`h-1.5 w-1.5 shrink-0 rounded-full transition-colors ${
                          active
                            ? "bg-blue-600"
                            : "bg-slate-300 group-hover/link:bg-slate-400"
                        }`}
                      />
                      <span className="min-w-0 flex-1 truncate">{item.label}</span>
                      {item.external && (
                        <span aria-hidden className="text-[10px] text-slate-400">
                          ↗
                        </span>
                      )}
                      {item.badge && (
                        <span className="rounded bg-slate-100 px-1 py-0.5 text-[9px] uppercase tracking-wider text-slate-600">
                          {item.badge}
                        </span>
                      )}
                    </Link>
                  </li>
                );
              })}
            </ul>
          </section>
        ))}
      </nav>

      {/* Utility footer */}
      {extras && (
        <div className="border-t border-slate-200/70 px-4 py-3">
          <div className="flex items-center justify-between gap-3">{extras}</div>
        </div>
      )}
    </div>
  );
}

export function Sidebar({
  extras,
  mobileTopRight,
}: {
  extras?: React.ReactNode;
  mobileTopRight?: React.ReactNode;
}) {
  const t = useTranslations("nav");
  const pathname = usePathname();
  const locale = useLocale();
  const [open, setOpen] = useState(false);
  const drawerRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const previouslyFocused = useRef<HTMLElement | null>(null);

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
        { href: "/embeddings", label: t("embeddings"), hint: "PLM embedding configs · ESM-2 · ESM3c · ProstT5 · Ankh" },
        { href: "/functional-annotation", label: t("functionalAnnotation"), hint: "Embedding-similarity GO annotation, BPO / MFO / CCO" },
        { href: "/scoring", label: t("scoring"), hint: "Combine distance, alignment, taxonomy, evidence", badge: "LAB" },
        { href: "/reranker", label: t("reranker"), hint: "LightGBM reranker over scored predictions", badge: "LAB" },
      ],
    },
    {
      id: "data",
      title: t("data"),
      hint: t("dataHint"),
      items: [
        { href: "/proteins", label: t("proteins"), hint: "UniProt entries · Swiss-Prot + TrEMBL, isoforms" },
        { href: "/annotations", label: t("annotations"), hint: "GO ontology snapshots and ground-truth GAF / QuickGO sets" },
        { href: "/query-sets", label: t("querySets"), hint: "FASTA uploads grouped for batch runs" },
      ],
    },
    {
      id: "results",
      title: t("results"),
      hint: t("resultsHint"),
      items: [
        { href: "/benchmark", label: t("benchmark"), hint: "Fmax matrix across embedding × stage × NK / LK / PK" },
        { href: "/evaluation", label: t("evaluation"), hint: "CAFA-style delta evaluation (Fmax, Smin, coverage)" },
      ],
    },
    {
      id: "operations",
      title: t("system"),
      hint: t("operationsHint"),
      items: [
        { href: "/jobs", label: t("jobs"), hint: "Live job queue and event audit trail" },
        { href: "/maintenance", label: t("maintenance"), hint: "Vacuum orphan sequences and unindexed embeddings" },
        { href: "/stack", label: t("stack"), hint: "Eight repositories, open PRs, deploy targets" },
      ],
    },
    {
      id: "docs",
      title: t("docs"),
      items: [
        { href: "/sphinx/", label: t("sphinx"), hint: t("sphinxHint"), external: true },
        { href: swaggerHref, label: t("swagger"), hint: t("swaggerHint"), external: true },
        { href: "/thesis.pdf", label: t("thesis"), hint: t("thesisHint"), external: true },
        { href: "/support", label: t("support"), hint: t("supportHint") },
      ],
    },
  ];

  const stripped = stripLocale(pathname);
  const annotateActive = stripped.startsWith("/functional-annotation");
  const closeDrawer = useCallback(() => setOpen(false), []);

  // Close the mobile drawer on browser back/forward navigation. In-app link
  // clicks already close it via the per-link onNavigate handler, so this only
  // needs to cover history navigation (popstate). Doing it in an event
  // callback (rather than a pathname effect) avoids a synchronous setState in
  // an effect body.
  useEffect(() => {
    const handler = () => setOpen(false);
    window.addEventListener("popstate", handler);
    return () => window.removeEventListener("popstate", handler);
  }, []);

  // Body scroll lock while the mobile drawer is open.
  useEffect(() => {
    if (typeof document === "undefined" || !open) return;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previous;
    };
  }, [open]);

  // Close on Escape.
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open]);

  // Focus trap: move focus into the drawer on open, restore on close, and
  // keep Tab cycling within the drawer while it is open.
  useEffect(() => {
    if (!open) {
      previouslyFocused.current?.focus?.();
      return;
    }
    previouslyFocused.current = document.activeElement as HTMLElement;
    const node = drawerRef.current;
    if (!node) return;
    const focusables = () =>
      Array.from(
        node.querySelectorAll<HTMLElement>(
          'a[href], button:not([disabled]), input, select, textarea, [tabindex]:not([tabindex="-1"])'
        )
      ).filter((el) => el.offsetParent !== null);
    focusables()[0]?.focus();
    const handler = (e: KeyboardEvent) => {
      if (e.key !== "Tab") return;
      const els = focusables();
      if (els.length === 0) return;
      const first = els[0];
      const last = els[els.length - 1];
      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    };
    node.addEventListener("keydown", handler);
    return () => node.removeEventListener("keydown", handler);
  }, [open]);

  return (
    <>
      {/* ── Desktop rail ─────────────────────────────────────────── */}
      <aside className="hidden lg:flex lg:w-64 xl:w-72 shrink-0">
        <div className="sticky top-0 flex h-screen w-full flex-col border-r border-slate-200/70 bg-white/85 backdrop-blur-md supports-[backdrop-filter]:bg-white/70">
          {/* Brand */}
          <Link
            href={`/${locale}`}
            className="group flex items-center gap-2.5 border-b border-slate-200/70 px-4 h-16 shrink-0"
            aria-label="PROTEA home"
          >
            <span
              aria-hidden
              className="relative flex h-10 w-10 items-center justify-center transition-transform group-hover:scale-105"
            >
              <Image src="/protea-mark.png" alt="" width={40} height={40} priority className="h-10 w-10" />
              <span className="absolute -right-0.5 -bottom-0.5 h-2 w-2 rounded-full bg-emerald-400 ring-2 ring-white" />
            </span>
            <span className="flex min-w-0 flex-col leading-tight">
              <span className="text-[17px] font-bold tracking-tight text-slate-900 transition-colors group-hover:text-blue-700">
                PROTEA
              </span>
              <span className="truncate text-[10px] font-medium uppercase tracking-[0.12em] text-slate-500">
                {t("subtitleShort")}
              </span>
            </span>
          </Link>

          <div className="min-h-0 flex-1">
            <RailContent
              groups={NAV_GROUPS}
              stripped={stripped}
              locale={locale}
              annotateHref={annotateHref}
              annotateActive={annotateActive}
              extras={extras}
              t={t}
            />
          </div>
        </div>
      </aside>

      {/* ── Mobile top bar (hamburger + brand + utility cluster) ───
          Fixed (not a flex item) so it never affects the rail/main row;
          <main> reserves space with a top padding on < lg. */}
      <header className="lg:hidden fixed inset-x-0 top-0 z-50 flex h-14 items-center gap-2 border-b border-slate-200/70 bg-white/85 px-2 backdrop-blur-md supports-[backdrop-filter]:bg-white/70">
        <button
          ref={triggerRef}
          className="flex h-11 w-11 shrink-0 flex-col items-center justify-center gap-1.5 rounded-lg text-slate-600 transition-colors hover:bg-slate-100"
          onClick={() => setOpen((v) => !v)}
          aria-label={open ? t("closeMenu") : t("openMenu")}
          aria-expanded={open}
          aria-controls="protea-mobile-nav"
        >
          <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "translate-y-2 rotate-45" : ""}`} />
          <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "opacity-0" : ""}`} />
          <span className={`block h-0.5 w-5 bg-current transition-all duration-200 ${open ? "-translate-y-2 -rotate-45" : ""}`} />
        </button>
        <Link
          href={`/${locale}`}
          className="group flex min-w-0 items-center gap-2"
          aria-label="PROTEA home"
        >
          <span aria-hidden className="relative flex h-8 w-8 shrink-0 items-center justify-center">
            <Image src="/protea-mark.png" alt="" width={32} height={32} priority className="h-8 w-8" />
            <span className="absolute -right-0.5 -bottom-0.5 h-1.5 w-1.5 rounded-full bg-emerald-400 ring-2 ring-white" />
          </span>
          <span className="truncate text-[15px] font-bold tracking-tight text-slate-900 group-hover:text-blue-700">
            PROTEA
          </span>
        </Link>
        {mobileTopRight && (
          <div className="ml-auto flex shrink-0 items-center gap-1.5">{mobileTopRight}</div>
        )}
      </header>

      {/* ── Mobile drawer ────────────────────────────────────────── */}
      {open && (
        <div
          className="lg:hidden fixed inset-0 z-[60] bg-slate-900/40 backdrop-blur-[1px]"
          onClick={closeDrawer}
          aria-hidden
        />
      )}
      <div
        ref={drawerRef}
        id="protea-mobile-nav"
        role="dialog"
        aria-modal="true"
        aria-hidden={!open}
        aria-label={t("ariaPrimary")}
        className={`lg:hidden fixed inset-y-0 left-0 z-[70] flex w-[85vw] max-w-xs flex-col bg-white shadow-2xl transition-transform duration-300 ease-out ${
          open ? "translate-x-0 visible" : "-translate-x-full pointer-events-none invisible"
        }`}
      >
        {/* Drawer header: brand + close */}
        <div className="flex h-16 shrink-0 items-center justify-between gap-2 border-b border-slate-200/70 px-4">
          <Link
            href={`/${locale}`}
            onClick={closeDrawer}
            className="group flex items-center gap-2.5"
            aria-label="PROTEA home"
          >
            <span aria-hidden className="relative flex h-9 w-9 items-center justify-center">
              <Image src="/protea-mark.png" alt="" width={36} height={36} className="h-9 w-9" />
              <span className="absolute -right-0.5 -bottom-0.5 h-2 w-2 rounded-full bg-emerald-400 ring-2 ring-white" />
            </span>
            <span className="text-[16px] font-bold tracking-tight text-slate-900">PROTEA</span>
          </Link>
          <button
            onClick={closeDrawer}
            aria-label={t("closeMenu")}
            className="flex h-9 w-9 items-center justify-center rounded-lg text-slate-500 transition-colors hover:bg-slate-100 hover:text-slate-900"
          >
            <svg className="h-5 w-5" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24" aria-hidden>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 6l12 12M18 6L6 18" />
            </svg>
          </button>
        </div>

        <div className="min-h-0 flex-1">
          <RailContent
            groups={NAV_GROUPS}
            stripped={stripped}
            locale={locale}
            annotateHref={annotateHref}
            annotateActive={annotateActive}
            onNavigate={closeDrawer}
            extras={extras}
            t={t}
          />
        </div>
      </div>
    </>
  );
}
