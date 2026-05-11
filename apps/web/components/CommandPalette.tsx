"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import { useTranslations } from "next-intl";

/**
 * Global cmd+k / ctrl+k command palette.
 *
 * Mounted once in the locale layout. Listens for the open shortcut at
 * window level, opens a centered modal with a fuzzy filter over a
 * static catalog of navigation entries, and quietly suggests two
 * parametric jumps when the query looks like a UUID or a UniProt
 * accession (open job / open protein).
 *
 * Keyboard:
 *   ⌘K / Ctrl+K   open
 *   Esc            close
 *   ↑ / ↓          move selection
 *   Enter          activate
 */

type Item = {
  id: string;
  label: string;
  href: string;
  group: string;
  hint?: string;
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const ACC_RE = /^[A-NR-Z][0-9][A-Z0-9]{3}[0-9](-\d+)?$/i; // UniProt accession

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
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIdx, setActiveIdx] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  const baseItems: Item[] = useMemo(() => {
    const tk = (k: string, fallback: string): string => {
      try {
        // Type-narrow `as any` because next-intl types only ship known keys.
        return t(k as never);
      } catch {
        return fallback;
      }
    };
    return [
      { id: "home",         label: tk("home", "Home"),                            href: "/",                       group: "Navigate" },
      { id: "benchmark",    label: tk("benchmark", "Benchmark"),                  href: "/benchmark",              group: "Navigate" },
      { id: "proteins",     label: tk("proteins", "Proteins"),                    href: "/proteins",               group: "Data" },
      { id: "annotations",  label: tk("annotations", "Annotations"),              href: "/annotations",            group: "Data" },
      { id: "querySets",    label: tk("querySets", "Query Sets"),                 href: "/query-sets",             group: "Data" },
      { id: "embeddings",   label: tk("embeddings", "Embeddings"),                href: "/embeddings",             group: "Pipeline" },
      { id: "fa",           label: tk("functionalAnnotation", "Functional Annotation"), href: "/functional-annotation", group: "Pipeline" },
      { id: "reranker",     label: tk("reranker", "Re-ranker"),                   href: "/reranker",               group: "Pipeline" },
      { id: "scoring",      label: tk("scoring", "Scoring"),                      href: "/scoring",                group: "Results" },
      { id: "evaluation",   label: tk("evaluation", "Evaluation"),                href: "/evaluation",             group: "Results" },
      { id: "jobs",         label: tk("jobs", "Jobs"),                            href: "/jobs",                   group: "System" },
      { id: "maintenance",  label: tk("maintenance", "Maintenance"),              href: "/maintenance",            group: "System" },
      { id: "stack",        label: "Stack",                                       href: "/stack",                  group: "System" },
      { id: "support",      label: "Support",                                     href: "/support",                group: "System" },
    ];
  }, [t]);

  // Parametric items: only show when the query matches a known shape.
  const parametricItems: Item[] = useMemo(() => {
    const q = query.trim();
    const out: Item[] = [];
    if (UUID_RE.test(q)) {
      out.push({
        id: "open-job",
        label: `Open job ${q.slice(0, 8)}…`,
        href: `/jobs/${q}`,
        group: "Jump",
        hint: q,
      });
    }
    if (ACC_RE.test(q)) {
      out.push({
        id: "open-protein",
        label: `Open protein ${q.toUpperCase()}`,
        href: `/proteins/${q.toUpperCase()}`,
        group: "Jump",
        hint: "UniProt accession",
      });
    }
    return out;
  }, [query]);

  const filtered: Item[] = useMemo(() => {
    if (!query.trim()) return baseItems;
    const q = query.trim();
    const scored = baseItems
      .map((it) => ({ it, s: Math.max(fuzzyScore(it.label, q), fuzzyScore(it.id, q)) }))
      .filter((x) => x.s > 0)
      .sort((a, b) => b.s - a.s)
      .map((x) => x.it);
    return [...parametricItems, ...scored];
  }, [baseItems, parametricItems, query]);

  const close = useCallback(() => {
    setOpen(false);
    setQuery("");
    setActiveIdx(0);
  }, []);

  const activate = useCallback(
    (item: Item) => {
      const prefix = localePrefix(pathname);
      router.push(`${prefix}${item.href}`);
      close();
    },
    [router, pathname, close],
  );

  // Global open shortcut: cmd+k / ctrl+k. Ignored when typing in inputs.
  // Mouse / mobile users open via the trigger button, which dispatches a
  // ``protea:cmdk:toggle`` window event — same effect, no shared state.
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
      if (item) activate(item);
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
            placeholder="Search pages, jobs, proteins…"
            aria-label="Search"
            className="flex-1 bg-transparent text-[15px] text-slate-800 placeholder-slate-400 focus:outline-none"
          />
          <kbd className="rounded-md border border-slate-200 bg-slate-50 px-1.5 py-0.5 text-[10px] font-medium text-slate-500">
            esc
          </kbd>
        </div>

        <div role="listbox" className="max-h-[60vh] overflow-y-auto py-2">
          {filtered.length === 0 ? (
            <p className="px-4 py-6 text-center text-sm text-slate-600">
              No matches. Try a UniProt accession or a job UUID.
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
                    return (
                      <button
                        key={it.id}
                        role="option"
                        aria-selected={isActive}
                        onMouseEnter={() => setActiveIdx(filtered.indexOf(it))}
                        onClick={() => activate(it)}
                        className={`w-full flex items-center justify-between gap-3 px-4 py-2 text-left text-sm transition-colors ${
                          isActive
                            ? "bg-blue-50 text-blue-900"
                            : "text-slate-700 hover:bg-slate-50"
                        }`}
                      >
                        <span className="truncate font-medium">{it.label}</span>
                        <span className="flex items-center gap-2 shrink-0">
                          {it.hint && (
                            <span className="text-[11px] text-slate-600">{it.hint}</span>
                          )}
                          <span className="text-[11px] font-mono text-slate-600">{it.href}</span>
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
            <kbd className="rounded border border-slate-200 bg-white px-1.5 py-0.5 font-medium text-slate-600">↑↓</kbd>
            <span>navigate</span>
            <kbd className="rounded border border-slate-200 bg-white px-1.5 py-0.5 font-medium text-slate-600">↵</kbd>
            <span>open</span>
          </div>
          <div className="flex items-center gap-1">
            <kbd className="rounded border border-slate-200 bg-white px-1.5 py-0.5 font-medium text-slate-600">⌘</kbd>
            <kbd className="rounded border border-slate-200 bg-white px-1.5 py-0.5 font-medium text-slate-600">K</kbd>
          </div>
        </div>
      </div>
    </div>
  );
}
