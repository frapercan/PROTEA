"use client";

import { useEffect, useState } from "react";

/**
 * Discreet header button that opens the global CommandPalette by
 * dispatching ``protea:cmdk:toggle``. The palette listens for the
 * same event, so there is no shared state to manage here.
 *
 * Renders the platform's modifier glyph (⌘ on macOS, Ctrl elsewhere)
 * detected client-side. SSR markup uses Ctrl as a safe default and
 * swaps after hydration.
 */
export function CommandPaletteTrigger() {
  const [isMac, setIsMac] = useState(false);

  useEffect(() => {
    if (typeof navigator === "undefined") return;
    const ua = navigator.userAgent.toLowerCase();
    const platform = (navigator as Navigator & { platform?: string }).platform?.toLowerCase() ?? "";
    setIsMac(/mac|iphone|ipad|ipod/.test(ua) || /mac/.test(platform));
  }, []);

  function open() {
    window.dispatchEvent(new CustomEvent("protea:cmdk:toggle"));
  }

  return (
    <button
      onClick={open}
      aria-label="Open command palette"
      className="hidden sm:flex h-9 items-center gap-2 rounded-lg border border-slate-200 bg-white px-2.5 text-[12px] text-slate-500 hover:border-slate-300 hover:text-slate-700 transition-colors shadow-sm"
    >
      <svg
        className="w-3.5 h-3.5 text-slate-600"
        aria-hidden
        fill="none"
        viewBox="0 0 16 16"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        <circle cx="7" cy="7" r="5" />
        <path d="M11 11l3 3" />
      </svg>
      <span className="hidden md:inline">Search…</span>
      <span className="flex items-center gap-0.5">
        <kbd className="rounded border border-slate-200 bg-slate-50 px-1 py-0.5 text-[10px] font-medium text-slate-500">
          {isMac ? "⌘" : "Ctrl"}
        </kbd>
        <kbd className="rounded border border-slate-200 bg-slate-50 px-1 py-0.5 text-[10px] font-medium text-slate-500">
          K
        </kbd>
      </span>
    </button>
  );
}
