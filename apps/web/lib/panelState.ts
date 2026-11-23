"use client";

// The window panel's state, kept in the page's address (the panel's decision e).
//
// Everything the reader chooses (tab, cell, reading, reference, filters, focus,
// axis, band) is a query parameter, so a view is shared by its link and the
// back button undoes the last navigation. Unlike ``useUrlParam``, which binds
// one key and replaces the history entry, a panel change often moves several
// keys at once (opening a cell's strata sets the tab, the cell and the focus),
// and a navigation pushes an entry while a filter replaces it.

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useCallback, useMemo } from "react";

export const PANEL_DEFAULTS = {
  tab: "summary",
  cell: "NK:MFO",
  reading: "percentile_rank",
  ref: "",
  models: "",
  scorings: "",
  depths: "",
  superseded: "0",
  focus: "",
  axis: "homology",
  band: "",
  group: "scoring",
  model: "",
  scoring: "embedding_only",
  rows: "15",
  sort: "f",
  dir: "desc",
  q: "",
  measure: "all",
  cells: "one",
} as const;

export type PanelState = { -readonly [K in keyof typeof PANEL_DEFAULTS]: string };
export type PanelPatch = Partial<PanelState>;

const KEYS = Object.keys(PANEL_DEFAULTS) as (keyof PanelState)[];

/** The state a query string describes; a key it does not carry is at its default. */
export function readPanelState(params: URLSearchParams): PanelState {
  const state = { ...PANEL_DEFAULTS } as PanelState;
  for (const key of KEYS) {
    const value = params.get(key);
    if (value != null) state[key] = value;
  }
  return state;
}

/** The query string after ``patch``: other parameters kept, defaults left out. */
export function writePanelState(params: URLSearchParams, patch: PanelPatch): URLSearchParams {
  const next = new URLSearchParams(params.toString());
  for (const [key, value] of Object.entries(patch) as [keyof PanelState, string][]) {
    if (value == null || value === PANEL_DEFAULTS[key]) next.delete(key);
    else next.set(key, value);
  }
  return next;
}

/** A comma list in one key: ``"a,b"`` -> ``["a", "b"]``. */
export function listOf(value: string): string[] {
  return value ? value.split(",").filter(Boolean) : [];
}

/** Toggle ``item`` in a comma list, keeping the order it was added in. */
export function toggled(value: string, item: string): string {
  const items = listOf(value);
  return (items.includes(item) ? items.filter((x) => x !== item) : [...items, item]).join(",");
}

/** Up to three arms in focus: adding a fourth lets the oldest go. */
export function toggledFocus(value: string, id: string): string {
  const items = listOf(value);
  if (items.includes(id)) return items.filter((x) => x !== id).join(",");
  return [...items, id].slice(-3).join(",");
}

export function usePanelState(): readonly [
  PanelState,
  (patch: PanelPatch, options?: { push?: boolean }) => void,
] {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();
  const state = useMemo(
    () => readPanelState(new URLSearchParams(params.toString())),
    [params],
  );
  const commit = useCallback(
    (patch: PanelPatch, options?: { push?: boolean }) => {
      const qs = writePanelState(new URLSearchParams(params.toString()), patch).toString();
      const href = qs ? `${pathname}?${qs}` : pathname;
      if (options?.push) router.push(href, { scroll: false });
      else router.replace(href, { scroll: false });
    },
    [params, pathname, router],
  );
  return [state, commit] as const;
}
