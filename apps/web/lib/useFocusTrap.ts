"use client";

import { useEffect, useRef } from "react";

/**
 * Minimal focus-trap for modal dialogs.
 *
 * When `active` flips to true:
 *   - records the currently-focused element so it can be restored,
 *   - focuses the first tabbable descendant of the returned ref,
 *   - keeps Tab / Shift+Tab inside the container,
 *   - calls `onClose` on Escape.
 *
 * When `active` flips back to false (or the component unmounts) the
 * previously-focused element receives focus again.
 *
 * Usage:
 *   const ref = useFocusTrap<HTMLDivElement>(open, () => setOpen(false));
 *   return <div ref={ref} role="dialog" aria-modal="true" ... />
 */
export function useFocusTrap<T extends HTMLElement>(
  active: boolean,
  onClose?: () => void,
) {
  const containerRef = useRef<T | null>(null);
  const previouslyFocusedRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (!active) return;
    const container: T | null = containerRef.current;
    if (!container) return;
    // Local alias narrowed to non-null for closures below.
    const node: T = container;

    previouslyFocusedRef.current =
      typeof document !== "undefined"
        ? (document.activeElement as HTMLElement | null)
        : null;

    function getTabbables(): HTMLElement[] {
      const selector =
        'a[href], area[href], button:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), iframe, object, embed, [tabindex]:not([tabindex="-1"]), [contenteditable="true"]';
      return Array.from(node.querySelectorAll<HTMLElement>(selector)).filter(
        (el) => !el.hasAttribute("inert") && el.offsetParent !== null,
      );
    }

    // Focus the first tabbable on open; fall back to the container itself.
    const first = getTabbables()[0];
    if (first) {
      first.focus();
    } else {
      node.setAttribute("tabindex", "-1");
      node.focus();
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.stopPropagation();
        onClose?.();
        return;
      }
      if (event.key !== "Tab") return;
      const tabbables = getTabbables();
      if (tabbables.length === 0) {
        event.preventDefault();
        return;
      }
      const firstEl = tabbables[0];
      const lastEl = tabbables[tabbables.length - 1];
      const activeEl = document.activeElement as HTMLElement | null;
      if (event.shiftKey) {
        if (activeEl === firstEl || !node.contains(activeEl)) {
          event.preventDefault();
          lastEl.focus();
        }
      } else {
        if (activeEl === lastEl) {
          event.preventDefault();
          firstEl.focus();
        }
      }
    }

    document.addEventListener("keydown", handleKeyDown, true);
    return () => {
      document.removeEventListener("keydown", handleKeyDown, true);
      const prev = previouslyFocusedRef.current;
      if (prev && typeof prev.focus === "function") {
        prev.focus();
      }
    };
  }, [active, onClose]);

  return containerRef;
}
