"use client";

import Link from "next/link";
import Image from "next/image";
import { usePathname } from "next/navigation";
import { useState, useEffect, useRef, useCallback } from "react";
import { useLocale, useTranslations } from "next-intl";
import {
  Sparkles,
  LayoutGrid,
  Workflow,
  Database,
  BarChart3,
  Server,
  BookOpen,
  Atom,
  Tags,
  Sliders,
  ArrowUpDown,
  ListTree,
  Dna,
  Tag,
  FolderOpen,
  Gauge,
  Inbox,
  Wrench,
  Boxes,
  Book,
  Braces,
  GraduationCap,
  ThumbsUp,
  PanelLeftClose,
  ChevronRight,
  Archive,
  ShieldCheck,
  KeyRound,
  Users,
  LogIn,
  LogOut,
  UserPlus,
  UserCircle2,
  type LucideIcon,
} from "lucide-react";
import { publicBaseUrl } from "@/lib/api";
import { useHasRole, useIsAuthenticated } from "@/lib/useRole";

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
  icon: LucideIcon;
};

type NavGroup = {
  id: string;
  title: string;
  hint?: string;
  icon: LucideIcon;
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
          className={`flex items-center gap-2.5 rounded-xl px-3.5 py-3 text-sm font-semibold tracking-[-0.005em] transition-all ${
            annotateActive
              ? "bg-blue-900 text-white shadow-sm shadow-blue-800/30"
              : "bg-blue-800 text-white hover:bg-blue-900 hover:shadow-md hover:shadow-blue-800/25"
          }`}
        >
          <Sparkles className="h-4 w-4 shrink-0" aria-hidden />
          <span>{t("annotate")}</span>
        </Link>
      </div>

      {/* Instrument hub link: the labeled entrance to the operational
          dashboard. The book (home + pillars) stays the top-level entrance,
          reached from the brand mark; this rail is the instrument's tab, and
          this link is its overview / hub. */}
      <div className="px-3 pt-2">
        <Link
          href={`/${locale}/instrument`}
          onClick={onNavigate}
          aria-current={
            stripped === "/instrument" ? "page" : undefined
          }
          className={`flex items-center gap-2.5 rounded-lg px-3.5 py-2 text-[0.9rem] font-medium tracking-[-0.005em] transition-colors ${
            stripped === "/instrument"
              ? "bg-stone-100 text-blue-900"
              : "text-stone-700 hover:bg-stone-100/70 hover:text-stone-950"
          }`}
        >
          <LayoutGrid className="h-[18px] w-[18px] shrink-0 text-stone-500" aria-hidden strokeWidth={1.85} />
          <span>{t("instrument")}</span>
        </Link>
      </div>

      {/* Grouped sections */}
      <nav
        aria-label={t("ariaPrimary")}
        className="flex-1 overflow-y-auto px-3 py-4"
      >
        {groups.map((group, gi) => {
          const GroupIcon = group.icon;
          return (
            <section
              key={group.id}
              className={gi > 0 ? "mt-5 pt-4 border-t border-stone-200/80" : ""}
              aria-labelledby={`sidebar-group-${group.id}`}
            >
              {/* A div, not a heading, and the section keeps its accessible
                  name through aria-labelledby either way. These six labels are
                  navigation furniture: as headings they sit in the document
                  outline ahead of the page title, and because the sidebar
                  renders twice (one layout for narrow viewports, one for wide)
                  a reader navigating by heading passes twelve of them before
                  reaching the h1. Measured, not supposed. */}
              <div
                id={`sidebar-group-${group.id}`}
                role="presentation"
                className="protea-eyebrow flex items-center gap-2 px-3 pb-2 text-xs uppercase text-stone-500"
              >
                <GroupIcon className="h-3.5 w-3.5 shrink-0 text-stone-400" aria-hidden />
                <span>{group.title}</span>
              </div>
              <ul className="space-y-px">
                {group.items.map((item) => {
                  const active = isInternalActive(stripped, item.href);
                  const href = item.external ? item.href : `/${locale}${item.href}`;
                  const Icon = item.icon;
                  return (
                    <li key={item.href}>
                      <Link
                        href={href}
                        onClick={onNavigate}
                        target={item.external ? "_blank" : undefined}
                        rel={item.external ? "noopener noreferrer" : undefined}
                        aria-current={active ? "page" : undefined}
                        title={item.hint}
                        className={`group/link relative flex items-center gap-3 rounded-lg px-3 py-2.5 text-[0.9rem] tracking-[-0.005em] transition-colors ${
                          active
                            ? "font-semibold text-blue-900"
                            : "text-stone-700 hover:bg-stone-100/70 hover:text-stone-950"
                        }`}
                      >
                        {active && (
                          <span
                            aria-hidden
                            className="absolute left-0 top-1.5 bottom-1.5 w-[3px] rounded-full bg-blue-800"
                          />
                        )}
                        <Icon
                          aria-hidden
                          className={`h-[18px] w-[18px] shrink-0 transition-colors ${
                            active
                              ? "text-blue-800"
                              : "text-stone-500 group-hover/link:text-stone-700"
                          }`}
                          strokeWidth={active ? 2.25 : 1.85}
                        />
                        <span className="min-w-0 flex-1 truncate">{item.label}</span>
                        {item.external && (
                          <span aria-hidden className="text-[0.7rem] text-stone-400">
                            ↗
                          </span>
                        )}
                        {item.badge && (
                          <span className="rounded bg-stone-100 px-1.5 py-0.5 text-[0.65rem] font-semibold uppercase tracking-wider text-stone-600">
                            {item.badge}
                          </span>
                        )}
                      </Link>
                    </li>
                  );
                })}
              </ul>
            </section>
          );
        })}
      </nav>

      {/* Utility footer */}
      {extras && (
        <div className="border-t border-stone-200/80 px-4 py-3">
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
  // FEAT-AUTH role gate: the Admin group only shows up for callers
  // whose session cookie decodes to ``role=admin``. Hook returns
  // ``false`` on SSR / pre-hydration, so the group never flashes
  // for anonymous visitors before the cookie is read.
  const isAdmin = useHasRole("admin");
  const isAuthed = useIsAuthenticated();
  const [open, setOpen] = useState(false);
  // Desktop-only: persisted collapsed/expanded state. SSR renders expanded;
  // the effect below hydrates from localStorage on mount. Brief one-frame
  // flicker is acceptable (no critical CLS) and avoids the cookie/header
  // dance a no-flash impl would need.
  const [desktopCollapsed, setDesktopCollapsed] = useState(false);
  const drawerRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const previouslyFocused = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (typeof window === "undefined") return;
    setDesktopCollapsed(window.localStorage.getItem("protea-sidebar-collapsed") === "1");
  }, []);
  const toggleDesktop = useCallback(() => {
    setDesktopCollapsed((v) => {
      const next = !v;
      if (typeof window !== "undefined") {
        window.localStorage.setItem("protea-sidebar-collapsed", next ? "1" : "0");
      }
      return next;
    });
  }, []);

  const onHome = stripLocale(pathname) === "/";
  const annotateHref = onHome
    ? `/${locale}#annotate-form`
    : `/${locale}/instrument/functional-annotation`;
  // AUTH-PUBLIC-VIEWER: the Swagger href is rendered into HTML and
  // clicked by the user, so it must use the public ingress (e.g.
  // ``/api-proxy``) rather than the SSR-only ``127.0.0.1`` fallback
  // from ``baseUrl()``.
  const swaggerHref = `${publicBaseUrl()}/docs`;

  const NAV_GROUPS: NavGroup[] = [
    {
      id: "pipeline",
      title: t("pipelineGroup"),
      hint: t("pipelineHint"),
      icon: Workflow,
      items: [
        { href: "/instrument/embeddings", label: t("embeddings"), hint: "PLM embedding configs · ESM-2 · ESM3c · ProstT5 · Ankh", icon: Atom },
        { href: "/instrument/functional-annotation", label: t("functionalAnnotation"), hint: "Embedding-similarity GO annotation, BPO / MFO / CCO", icon: Tags },
        { href: "/instrument/scoring", label: t("scoring"), hint: "Combine distance, alignment, taxonomy, evidence", badge: "LAB", icon: Sliders },
        { href: "/instrument/reranker", label: t("reranker"), hint: "LightGBM reranker over scored predictions", badge: "LAB", icon: ArrowUpDown },
        { href: "/instrument/datasets", label: t("datasets"), hint: "Frozen reranker dumps and export dispatcher", icon: Archive },
        { href: "/feature-registry", label: t("featureRegistry"), hint: "What every reranker feature means, who produces it, and whether it is live", icon: ListTree },
      ],
    },
    {
      id: "data",
      title: t("data"),
      hint: t("dataHint"),
      icon: Database,
      items: [
        { href: "/instrument/proteins", label: t("proteins"), hint: "UniProt entries · Swiss-Prot + TrEMBL, isoforms", icon: Dna },
        { href: "/instrument/annotations", label: t("annotations"), hint: "GO ontology snapshots and ground-truth GAF / QuickGO sets", icon: Tag },
        { href: "/instrument/query-sets", label: t("querySets"), hint: "FASTA uploads grouped for batch runs", icon: FolderOpen },
      ],
    },
    {
      id: "results",
      title: t("results"),
      hint: t("resultsHint"),
      icon: BarChart3,
      items: [
        { href: "/instrument/graph", label: t("graph"), hint: "The experiment graph: ten nodes, the strength of each edge, and the nine panels it resolves", icon: Workflow },
        { href: "/instrument/benchmark", label: t("benchmark"), hint: "f_micro_w (IA-weighted, LAFA-comparable; CAFA uses per-protein fmax_w) matrix across embedding × stage × NK / LK / PK", icon: BarChart3 },
        { href: "/instrument/graph", label: t("graph"), hint: "Every decision as a node, with the strength of the evidence behind it", icon: Workflow },
        { href: "/instrument/evaluation", label: t("evaluation"), hint: "CAFA-style delta evaluation (Fmax, Smin, coverage)", icon: Gauge },
      ],
    },
    {
      id: "operations",
      title: t("system"),
      hint: t("operationsHint"),
      icon: Server,
      items: [
        { href: "/instrument/jobs", label: t("jobs"), hint: "Live job queue and event audit trail", icon: Inbox },
        // Admin only, for the same reason the page itself is: it answers 403 to
        // anyone else. Shown to everyone it was a link to a dead end, and worse,
        // Next prefetches sidebar links, so every anonymous visit fired a request
        // that failed before the visitor had clicked anything.
        ...(isAdmin
          ? [
              {
                href: "/maintenance",
                label: t("maintenance"),
                hint: "Vacuum orphan sequences and unindexed embeddings",
                icon: Wrench,
              },
            ]
          : []),
        { href: "/instrument/stack", label: t("stack"), hint: "Eight repositories, open PRs, deploy targets", icon: Boxes },
      ],
    },
    {
      id: "docs",
      title: t("docs"),
      icon: BookOpen,
      items: [
        { href: "/sphinx/", label: t("sphinx"), hint: t("sphinxHint"), external: true, icon: Book },
        { href: swaggerHref, label: t("swagger"), hint: t("swaggerHint"), external: true, icon: Braces },
        { href: "/thesis.pdf", label: t("thesis"), hint: t("thesisHint"), external: true, icon: GraduationCap },
        { href: "/support", label: t("support"), hint: t("supportHint"), icon: ThumbsUp },
      ],
    },
  ];

  // FARM-AUTH.10 account group: surface sign-in / sign-up for guests
  // and profile / sign-out for authenticated callers. Placed before
  // the admin group so the destructive ShieldCheck section stays
  // visually anchored at the bottom of the rail.
  NAV_GROUPS.push({
    id: "account",
    title: isAuthed ? "Account" : "Get started",
    icon: UserCircle2,
    items: isAuthed
      ? [
          { href: "/profile", label: "Profile", hint: "Your account details, role and status", icon: UserCircle2 },
          { href: "/logout", label: "Sign out", hint: "End your current session", icon: LogOut },
        ]
      : [
          { href: "/login", label: "Sign in", hint: "Use your email and password", icon: LogIn },
          { href: "/signup", label: "Create account", hint: "Request a new researcher account", icon: UserPlus },
        ],
  });

  // Append the admin group conditionally so the entire section (group
  // header + items) is invisible to non-admin viewers. Placing it last
  // keeps the destructive surface visually separated from day-to-day
  // pipeline navigation, matching the convention used for /maintenance.
  if (isAdmin) {
    NAV_GROUPS.push({
      id: "admin",
      title: t("adminGroup"),
      hint: t("adminGroupHint"),
      icon: ShieldCheck,
      items: [
        { href: "/admin/users", label: "Users", hint: "Approve sign-ups, change roles, deactivate accounts", icon: Users },
        { href: "/admin/api-keys", label: t("apiKeys"), hint: t("apiKeysHint"), icon: KeyRound },
        { href: "/admin/experiment-runs", label: t("experimentRuns"), hint: "F-EXP campaign narrative: hypothesis / config / findings per run", icon: GraduationCap },
        { href: "/admin/dlq", label: t("dlq"), hint: t("dlqHint"), icon: Archive },
      ],
    });
  }

  const stripped = stripLocale(pathname);
  const annotateActive = stripped.startsWith("/instrument/functional-annotation");
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

  // Body + html scroll lock while the mobile drawer is open.
  // Locking only `body` was insufficient on iOS Safari: the `html` element
  // remained scrollable via touch in the strip to the right of the drawer,
  // letting page content bleed past the backdrop on vertical scroll. Lock
  // both elements (and restore each independently) to fully pin the
  // viewport while the drawer is mounted.
  useEffect(() => {
    if (typeof document === "undefined" || !open) return;
    const html = document.documentElement;
    const body = document.body;
    const previousBody = body.style.overflow;
    const previousHtml = html.style.overflow;
    body.style.overflow = "hidden";
    html.style.overflow = "hidden";
    return () => {
      body.style.overflow = previousBody;
      html.style.overflow = previousHtml;
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
      <aside
        aria-hidden={desktopCollapsed}
        className={`hidden lg:flex lg:shrink-0 transition-[width] duration-300 ease-out ${
          desktopCollapsed ? "lg:w-0 lg:overflow-hidden" : "lg:w-64 xl:w-72"
        }`}
      >
        <div className="protea-sidebar-bg sticky top-0 flex h-screen w-full flex-col border-r border-stone-200/80">
          {/* Brand + collapse toggle */}
          <div className="flex items-stretch border-b border-stone-200/80 h-16 shrink-0">
            <Link
              href={`/${locale}`}
              className="group flex flex-1 min-w-0 items-center gap-2.5 px-4"
              aria-label="PROTEA home"
            >
              <span
                aria-hidden
                className="relative flex h-10 w-10 shrink-0 items-center justify-center transition-transform group-hover:scale-105"
              >
                <Image src="/protea-mark.png" alt="" width={40} height={40} priority className="h-10 w-10 object-contain" />
                <span className="absolute -right-0.5 -bottom-0.5 h-2 w-2 rounded-full bg-emerald-500 ring-2 ring-white" />
              </span>
              <span className="flex min-w-0 flex-col gap-0.5">
                <span className="text-lg font-bold tracking-tight leading-none text-stone-950 transition-colors group-hover:text-blue-800">
                  PROTEA
                </span>
                <span className="line-clamp-2 text-[0.68rem] leading-snug text-stone-500">
                  {t("subtitleShort")}
                </span>
              </span>
            </Link>
            <button
              type="button"
              onClick={toggleDesktop}
              aria-label={t("collapseSidebar")}
              title={t("collapseSidebar")}
              className="flex h-16 w-12 shrink-0 items-center justify-center text-stone-500 transition-colors hover:bg-stone-100 hover:text-stone-900"
            >
              <PanelLeftClose className="h-5 w-5" aria-hidden strokeWidth={1.9} />
            </button>
          </div>

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

      {/* Floating expand pill (desktop, only when collapsed).
          Keeps the PROTEA brand mark visible when the rail is hidden;
          the chevron on the right hints at "open the navigation". */}
      <button
        type="button"
        onClick={toggleDesktop}
        aria-label={t("openSidebar")}
        title={t("openSidebar")}
        className={`group fixed left-3 top-3 z-40 items-center gap-1.5 rounded-full border border-stone-200 bg-white/95 py-1.5 pl-1.5 pr-3 text-stone-700 shadow-md backdrop-blur transition-all hover:bg-stone-50 hover:text-stone-950 hover:shadow-lg ${
          desktopCollapsed ? "inline-flex lg:flex" : "hidden"
        }`}
      >
        <span aria-hidden className="relative flex h-8 w-8 shrink-0 items-center justify-center">
          <Image src="/protea-mark.png" alt="" width={32} height={32} className="h-8 w-8 object-contain" />
          <span className="absolute -right-0.5 -bottom-0.5 h-1.5 w-1.5 rounded-full bg-emerald-500 ring-2 ring-white" />
        </span>
        <ChevronRight className="h-4 w-4 text-stone-500 transition-transform group-hover:translate-x-0.5" aria-hidden strokeWidth={2.2} />
      </button>

      {/* ── Mobile top bar (hamburger + brand + utility cluster) ───
          Fixed (not a flex item) so it never affects the rail/main row;
          <main> reserves space with a top padding on < lg. */}
      <header className="lg:hidden fixed inset-x-0 top-0 z-50 flex h-14 items-center gap-2 border-b border-stone-200/80 bg-white/85 px-2 backdrop-blur-md supports-[backdrop-filter]:bg-white/70">
        <button
          ref={triggerRef}
          className="flex h-11 w-11 shrink-0 flex-col items-center justify-center gap-1.5 rounded-lg text-stone-600 transition-colors hover:bg-stone-100"
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
          <span className="truncate text-[15px] font-bold tracking-tight text-stone-950 group-hover:text-blue-800">
            PROTEA
          </span>
        </Link>
        {mobileTopRight && (
          <div className="ml-auto flex shrink-0 items-center gap-1.5">{mobileTopRight}</div>
        )}
      </header>

      {/* ── Mobile drawer ──────────────────────────────────────────
          Backdrop: opaque enough to fully occlude page content (40% was
          translucent enough that cards still bled through on the strip
          to the right of the drawer); `touch-action: none` +
          `overscroll-behavior: contain` block touch-scroll and rubber-band
          on iOS Safari, where the html-level overflow lock alone is not
          enough. Drawer: same `overscroll-behavior: contain` so internal
          nav scrolling never chains out to the page underneath. */}
      {open && (
        <div
          className="lg:hidden fixed inset-0 z-[60] bg-stone-900/60 backdrop-blur-sm overscroll-contain touch-none"
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
        className={`lg:hidden fixed inset-y-0 left-0 z-[70] flex w-[85vw] max-w-xs flex-col overscroll-contain bg-white shadow-2xl transition-transform duration-300 ease-out ${
          open ? "translate-x-0 visible" : "-translate-x-full pointer-events-none invisible"
        }`}
      >
        {/* Drawer header: brand + close */}
        <div className="flex h-16 shrink-0 items-center justify-between gap-2 border-b border-stone-200/80 px-4">
          <Link
            href={`/${locale}`}
            onClick={closeDrawer}
            className="group flex items-center gap-2.5"
            aria-label="PROTEA home"
          >
            <span aria-hidden className="relative flex h-9 w-9 shrink-0 items-center justify-center">
              <Image src="/protea-mark.png" alt="" width={36} height={36} className="h-9 w-9 object-contain" />
              <span className="absolute -right-0.5 -bottom-0.5 h-2 w-2 rounded-full bg-emerald-500 ring-2 ring-white" />
            </span>
            <span className="text-[16px] font-bold tracking-tight text-stone-950">PROTEA</span>
          </Link>
          <button
            onClick={closeDrawer}
            aria-label={t("closeMenu")}
            className="flex h-11 w-11 items-center justify-center rounded-lg text-stone-500 transition-colors hover:bg-stone-100 hover:text-stone-950"
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
