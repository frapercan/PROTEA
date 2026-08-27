"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import { useTranslations } from "next-intl";
import { useToast } from "@/components/Toast";
import {
  cleanupFarm,
  killFarmTask,
  readFarmToken,
  spawnFarmTask,
} from "@/lib/farmApi";

/**
 * Global cmd+k / ctrl+k command palette.
 *
 * Mounted once in the locale layout. Listens for the open shortcut at
 * window level, opens a centered modal with a fuzzy filter over a
 * static catalog of navigation entries, and quietly suggests two
 * parametric jumps when the query looks like a UUID or a UniProt
 * accession (open job / open protein).
 *
 * FARM-UI.6 adds a ``farm:`` namespace with action verbs that hit the
 * agent-farm sidecar (see lib/farmApi.ts):
 *   farm: spawn next pickable     POST /spawn  {next_pickable: true}
 *   farm: kill <task_id>          POST /tasks/<id>/kill
 *   farm: cleanup                 POST /cleanup {apply: true}
 *   farm: view <task_id>          navigation only, not a write
 *
 * Write verbs require an authenticated session (the AuthChip is not in
 * "anonymous" mode). The same protea.bearer / protea.apiKey value
 * doubles as the X-Farm-Token shared secret; the deployment runbook
 * provisions the sidecar with the same value.
 *
 * Keyboard:
 *   Cmd+K / Ctrl+K   open
 *   Esc              close
 *   ArrowUp/Down     move selection
 *   Enter            activate
 */

type NavItem = {
  kind: "nav";
  id: string;
  label: string;
  href: string;
  group: string;
  hint?: string;
};

type ActionItem = {
  kind: "action";
  id: string;
  label: string;
  group: string;
  hint?: string;
  action: () => Promise<void> | void;
  requiresAuth: boolean;
  authDisabledReason?: string;
};

type Item = NavItem | ActionItem;

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const ACC_RE = /^[A-NR-Z][0-9][A-Z0-9]{3}[0-9](-\d+)?$/i; // UniProt accession

// A task_id from agent-farm: ``<agent>-<unix-ts>-<hex4>`` (lowercase
// alpha + digits + dash). Liberal enough to accept agents with mixed
// case and longer hex suffixes; rejects shell metachars so a stray
// paste cannot exfiltrate beyond the route param.
const TASK_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{2,127}$/;

function isInputElement(el: EventTarget | null): boolean {
  if (!(el instanceof HTMLElement)) return false;
  const tag = el.tagName;
  return (
    tag === "INPUT" ||
    tag === "TEXTAREA" ||
    tag === "SELECT" ||
    el.isContentEditable
  );
}

function fuzzyScore(haystack: string, needle: string): number {
  if (!needle) return 0;
  const h = haystack.toLowerCase();
  const n = needle.toLowerCase();
  if (h.includes(n)) {
    const idx = h.indexOf(n);
    // Earlier match + shorter haystack ranks higher.
    return 1000 - idx - h.length / 100;
  }
  // Sub-sequence match: every char of n appears in order in h.
  let hi = 0;
  let matched = 0;
  for (let ni = 0; ni < n.length; ni++) {
    while (hi < h.length && h[hi] !== n[ni]) hi++;
    if (hi >= h.length) return -1;
    hi++;
    matched++;
  }
  return matched === n.length ? 200 - h.length / 100 : -1;
}

function localePrefix(pathname: string): string {
  const m = pathname.match(/^\/([a-z]{2})(?=\/|$)/);
  return m ? `/${m[1]}` : "";
}

export function CommandPalette() {
  const router = useRouter();
  const pathname = usePathname();
  const t = useTranslations("nav");
  const toast = useToast();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIdx, setActiveIdx] = useState(0);
  const [busy, setBusy] = useState(false);
  // Re-evaluated whenever the palette opens so a sign-in in another
  // tab flips the verb state without a manual refresh.
  const [authToken, setAuthToken] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const baseItems: NavItem[] = useMemo(() => {
    const tk = (k: string, fallback: string): string => {
      try {
        // Type-narrow `as any` because next-intl types only ship known keys.
        return t(k as never);
      } catch {
        return fallback;
      }
    };
    const k = (
      id: string,
      label: string,
      href: string,
      group: string,
    ): NavItem => ({ kind: "nav", id, label, href, group });
    return [
      k("home",        tk("home", "Home"),                                   "/",                       "Navigate"),
      k("graph",       tk("graph", "Experiment graph"),                   "/instrument/graph",                  "Navigate"),
      k("benchmark",   tk("benchmark", "Benchmark"),                         "/instrument/benchmark",              "Navigate"),
      k("proteins",    tk("proteins", "Proteins"),                           "/instrument/proteins",               "Data"),
      k("annotations", tk("annotations", "Annotations"),                     "/instrument/annotations",            "Data"),
      k("querySets",   tk("querySets", "Query Sets"),                        "/instrument/query-sets",             "Data"),
      k("embeddings",  tk("embeddings", "Embeddings"),                       "/instrument/embeddings",             "Pipeline"),
      k("fa",          tk("functionalAnnotation", "Functional Annotation"),  "/instrument/functional-annotation",  "Pipeline"),
      k("reranker",    tk("reranker", "Re-ranker"),                          "/instrument/reranker",               "Pipeline"),
      k("scoring",     tk("scoring", "Scoring"),                             "/instrument/scoring",                "Results"),
      k("evaluation",  tk("evaluation", "Evaluation"),                       "/instrument/evaluation",             "Results"),
      k("jobs",        tk("jobs", "Jobs"),                                   "/instrument/jobs",                   "System"),
      k("maintenance", tk("maintenance", "Maintenance"),                     "/maintenance",            "System"),
      k("stack",       "Stack",                                              "/instrument/stack",                  "System"),
      k("support",     "Support",                                            "/support",                "System"),
      k("farm",        "Agent farm",                                         "/instrument/farm",                   "System"),
    ];
  }, [t]);

  const close = useCallback(() => {
    setOpen(false);
    setQuery("");
    setActiveIdx(0);
  }, []);

  const navigate = useCallback(
    (href: string) => {
      const prefix = localePrefix(pathname);
      router.push(`${prefix}${href}`);
      close();
    },
    [router, pathname, close],
  );

  // Farm verbs. Only appear when the query starts with "farm" (or
  // its substring) so the namespace is opt-in and does not clutter
  // the default catalog. Authenticated check uses readFarmToken();
  // the disabled-tooltip path keeps the verb visible but unclickable.
  const farmItems: ActionItem[] = useMemo(() => {
    const q = query.trim();
    if (!q || !/^f|^farm/i.test(q)) return [];

    const authed = authToken !== null;
    const tokenOrThrow = (): string => {
      const token = readFarmToken();
      if (!token) {
        throw new Error("auth required");
      }
      return token;
    };

    const spawnNext: ActionItem = {
      kind: "action",
      id: "farm-spawn-next",
      label: "farm: spawn next pickable",
      group: "Farm",
      hint: "POST /spawn",
      requiresAuth: true,
      authDisabledReason: "Sign in to spawn farm tasks",
      action: async () => {
        const token = tokenOrThrow();
        const res = await spawnFarmTask({ next_pickable: true }, token);
        toast(
          res.slice_id
            ? `spawned ${res.task_id} (${res.slice_id})`
            : `spawned ${res.task_id}`,
          "success",
        );
        navigate(`/instrument/farm/${res.task_id}`);
      },
    };

    const cleanup: ActionItem = {
      kind: "action",
      id: "farm-cleanup",
      label: "farm: cleanup --apply",
      group: "Farm",
      hint: "POST /cleanup",
      requiresAuth: true,
      authDisabledReason: "Sign in to run cleanup",
      action: async () => {
        const token = tokenOrThrow();
        const res = await cleanupFarm(true, token);
        toast(`cleanup done${res.apply ? " (applied)" : " (dry-run)"}`, "success");
        close();
      },
    };

    const verbs: ActionItem[] = [spawnNext, cleanup];

    // Parametric kill / view: only when the query already includes
    // a plausible task_id token after "farm: kill" / "farm: view".
    const killMatch = q.match(/^farm:?\s*kill\s+(\S+)/i);
    if (killMatch && TASK_ID_RE.test(killMatch[1])) {
      const taskId = killMatch[1];
      verbs.push({
        kind: "action",
        id: "farm-kill",
        label: `farm: kill ${taskId}`,
        group: "Farm",
        hint: "POST /tasks/<id>/kill",
        requiresAuth: true,
        authDisabledReason: "Sign in to kill farm tasks",
        action: async () => {
          const token = tokenOrThrow();
          const res = await killFarmTask(taskId, token);
          toast(`killed ${res.task_id}`, "success");
          close();
        },
      });
    }

    const viewMatch = q.match(/^farm:?\s*view\s+(\S+)/i);
    if (viewMatch && TASK_ID_RE.test(viewMatch[1])) {
      const taskId = viewMatch[1];
      verbs.push({
        kind: "action",
        id: "farm-view",
        label: `farm: view ${taskId}`,
        group: "Farm",
        hint: "navigate",
        // Pure navigation; never gated.
        requiresAuth: false,
        action: () => {
          navigate(`/instrument/farm/${taskId}`);
        },
      });
    }

    // Surface the disabled state for un-authed users: the verbs stay
    // visible (so the operator learns they exist) but tooltip + the
    // dimmed style communicate the gate. We do NOT filter them out
    // here; the renderer reads requiresAuth + authed to decide.
    void authed;
    return verbs;
  }, [query, authToken, toast, navigate, close]);

  // Parametric nav items: only show when the query matches a known shape.
  const parametricNavItems: NavItem[] = useMemo(() => {
    const q = query.trim();
    const out: NavItem[] = [];
    if (UUID_RE.test(q)) {
      out.push({
        kind: "nav",
        id: "open-job",
        label: `Open job ${q.slice(0, 8)}…`,
        href: `/instrument/jobs/${q}`,
        group: "Jump",
        hint: q,
      });
    }
    if (ACC_RE.test(q)) {
      out.push({
        kind: "nav",
        id: "open-protein",
        label: `Open protein ${q.toUpperCase()}`,
        href: `/instrument/proteins/${q.toUpperCase()}`,
        group: "Jump",
        hint: "UniProt accession",
      });
    }
    return out;
  }, [query]);

  const filtered: Item[] = useMemo(() => {
    if (!query.trim()) return baseItems;
    const q = query.trim();
    const scoredNav = baseItems
      .map((it) => ({ it, s: Math.max(fuzzyScore(it.label, q), fuzzyScore(it.id, q)) }))
      .filter((x) => x.s > 0)
      .sort((a, b) => b.s - a.s)
      .map((x) => x.it);
    // Farm verbs first so the operator's most likely action floats.
    return [...farmItems, ...parametricNavItems, ...scoredNav];
  }, [baseItems, parametricNavItems, farmItems, query]);

  const runItem = useCallback(
    async (item: Item) => {
      if (item.kind === "nav") {
        navigate(item.href);
        return;
      }
      // ActionItem
      if (item.requiresAuth && authToken === null) {
        toast(item.authDisabledReason ?? "Sign in required", "error");
        return;
      }
      setBusy(true);
      try {
        await item.action();
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        toast(msg, "error");
      } finally {
        setBusy(false);
      }
    },
    [navigate, toast, authToken],
  );

  // Global open shortcut: cmd+k / ctrl+k. Ignored when typing in inputs.
  // Mouse / mobile users open via the trigger button, which dispatches a
  // ``protea:cmdk:toggle`` window event - same effect, no shared state.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
        if (isInputElement(e.target)) return;
        e.preventDefault();
        setOpen((v) => !v);
      }
    }
    function onToggle() {
      setOpen((v) => !v);
    }
    window.addEventListener("keydown", onKey);
    window.addEventListener("protea:cmdk:toggle", onToggle as EventListener);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("protea:cmdk:toggle", onToggle as EventListener);
    };
  }, []);

  // Reset selection when filtering changes.
  useEffect(() => {
    setActiveIdx(0);
  }, [query]);

  // Refresh the cached auth token whenever the palette opens. We also
  // listen for storage events so signing in inside another tab updates
  // the verb state while the palette is open.
  useEffect(() => {
    if (open) {
      setAuthToken(readFarmToken());
    }
    const onStorage = (e: StorageEvent) => {
      if (!e.key || e.key.startsWith("protea.")) {
        setAuthToken(readFarmToken());
      }
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, [open]);

  // Focus input when opened.
  useEffect(() => {
    if (open) {
      const id = window.setTimeout(() => inputRef.current?.focus(), 10);
      return () => window.clearTimeout(id);
    }
  }, [open]);

  if (!open) return null;

  function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Escape") {
      e.preventDefault();
      close();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIdx((i) => Math.min(i + 1, filtered.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIdx((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      const item = filtered[activeIdx];
      if (item) void runItem(item);
    }
  }

  return (
    <div
      role="dialog"
      aria-label="Command palette"
      aria-modal="true"
      className="fixed inset-0 z-[100] flex items-start justify-center px-4 pt-[12vh]"
      onClick={close}
    >
      <div
        aria-hidden
        className="absolute inset-0 bg-slate-900/40 backdrop-blur-sm"
      />
      <div
        onClick={(e) => e.stopPropagation()}
        className="relative w-full max-w-xl rounded-2xl border border-slate-200 bg-white shadow-2xl overflow-hidden"
      >
        <div className="flex items-center gap-2 px-4 py-3 border-b border-slate-100">
          <svg className="w-4 h-4 text-slate-600" fill="none" viewBox="0 0 16 16" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="7" cy="7" r="5" />
            <path d="M11 11l3 3" />
          </svg>
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder="Search pages, jobs, proteins… try farm:"
            aria-label="Search"
            className="flex-1 bg-transparent text-[15px] text-slate-800 placeholder-slate-400 focus:outline-none"
          />
          {busy && (
            <span
              className="text-[10px] font-medium text-blue-600"
              data-testid="cmdk-busy"
              aria-live="polite"
            >
              working…
            </span>
          )}
          <kbd className="rounded-md border border-slate-200 bg-slate-50 px-1.5 py-0.5 text-[10px] font-medium text-slate-500">
            esc
          </kbd>
        </div>

        <div role="listbox" className="max-h-[60vh] overflow-y-auto py-2">
          {filtered.length === 0 ? (
            <p className="px-4 py-6 text-center text-sm text-slate-600">
              No matches. Try a UniProt accession, a job UUID, or farm:
            </p>
          ) : (
            (() => {
              const groups = new Map<string, Item[]>();
              for (const it of filtered) {
                const arr = groups.get(it.group);
                if (arr) arr.push(it);
                else groups.set(it.group, [it]);
              }
              let cursor = -1;
              return Array.from(groups.entries()).map(([group, items]) => (
                <div key={group} className="mb-1.5 last:mb-0">
                  <div className="px-4 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-600">
                    {group}
                  </div>
                  {items.map((it) => {
                    cursor++;
                    const isActive = cursor === activeIdx;
                    const isDisabled =
                      it.kind === "action" && it.requiresAuth && authToken === null;
                    const tooltip =
                      isDisabled && it.kind === "action"
                        ? it.authDisabledReason ?? "Sign in required"
                        : undefined;
                    return (
                      <button
                        key={it.id}
                        role="option"
                        aria-selected={isActive}
                        aria-disabled={isDisabled || undefined}
                        title={tooltip}
                        data-testid={
                          it.kind === "action" ? `cmdk-${it.id}` : undefined
                        }
                        onMouseEnter={() => setActiveIdx(filtered.indexOf(it))}
                        onClick={() => {
                          if (isDisabled) {
                            toast(tooltip ?? "Sign in required", "error");
                            return;
                          }
                          void runItem(it);
                        }}
                        className={`w-full flex items-center justify-between gap-3 px-4 py-2 text-left text-sm transition-colors ${
                          isDisabled
                            ? "text-slate-400 cursor-not-allowed"
                            : isActive
                              ? "bg-blue-50 text-blue-900"
                              : "text-slate-700 hover:bg-slate-50"
                        }`}
                      >
                        <span className="truncate font-medium">{it.label}</span>
                        <span className="flex items-center gap-2 shrink-0">
                          {isDisabled && (
                            <span className="text-[10px] uppercase tracking-wider text-amber-600">
                              auth
                            </span>
                          )}
                          {it.hint && !isDisabled && (
                            <span className="text-[11px] text-slate-600">
                              {it.hint}
                            </span>
                          )}
                          {it.kind === "nav" && (
                            <span className="text-[11px] font-mono text-slate-600">
                              {it.href}
                            </span>
                          )}
                        </span>
                      </button>
                    );
                  })}
                </div>
              ));
            })()
          )}
        </div>

        <div className="flex items-center justify-between gap-3 px-4 py-2 border-t border-slate-100 bg-slate-50/50 text-[11px] text-slate-500">
          <div className="flex items-center gap-2">
            <kbd className="rounded border border-slate-200 bg-white px-1.5 py-0.5 font-medium text-slate-600">{"↑↓"}</kbd>
            <span>navigate</span>
            <kbd className="rounded border border-slate-200 bg-white px-1.5 py-0.5 font-medium text-slate-600">{"↵"}</kbd>
            <span>open</span>
          </div>
          <div className="flex items-center gap-1">
            <kbd className="rounded border border-slate-200 bg-white px-1.5 py-0.5 font-medium text-slate-600">{"⌘"}</kbd>
            <kbd className="rounded border border-slate-200 bg-white px-1.5 py-0.5 font-medium text-slate-600">K</kbd>
          </div>
        </div>
      </div>
    </div>
  );
}
