// FARM-UI.4 client-side DAG helpers for the slice catalog.
//
// The agent-farm plan is small (sub-200 slices) so all of this runs on
// the main thread in O(V+E) — no need for a worker. The critical-path
// helper returns the longest dep-chain whose every node is still in a
// pending-family state (pending / in_progress / blocked); a fully done
// chain has no operational value to highlight.

import type { FarmPlanSlice } from "@/lib/farmApi";

const PENDING_STATES = new Set(["pending", "in_progress", "blocked"]);

/** Pending-family check used for critical-path elligibility. */
export function isPendingFamily(status: string): boolean {
  return PENDING_STATES.has(status);
}

/**
 * Topological order over the slice DAG. Returns the slice ids in order
 * such that every dep appears before any of its dependents. Cycles, if
 * present in the source plan, are broken by emitting the offending node
 * after its already-visited deps (best-effort; the parser is supposed
 * to reject cycles upstream).
 */
export function topologicalOrder(slices: FarmPlanSlice[]): string[] {
  const known = new Set(slices.map((s) => s.id));
  const indeg = new Map<string, number>();
  const adj = new Map<string, string[]>();
  for (const s of slices) {
    indeg.set(s.id, 0);
    adj.set(s.id, []);
  }
  for (const s of slices) {
    for (const dep of s.deps) {
      if (!known.has(dep)) continue;
      adj.get(dep)!.push(s.id);
      indeg.set(s.id, (indeg.get(s.id) ?? 0) + 1);
    }
  }
  const queue: string[] = [];
  for (const [id, d] of indeg.entries()) if (d === 0) queue.push(id);
  const out: string[] = [];
  while (queue.length > 0) {
    const id = queue.shift()!;
    out.push(id);
    for (const next of adj.get(id) ?? []) {
      const nd = (indeg.get(next) ?? 0) - 1;
      indeg.set(next, nd);
      if (nd === 0) queue.push(next);
    }
  }
  // Tail: anything left has a cycle; append in arbitrary order so the
  // caller sees the whole catalog and the UI does not silently drop
  // nodes. This path should not fire on a well-formed plan.
  if (out.length < slices.length) {
    for (const s of slices) if (!out.includes(s.id)) out.push(s.id);
  }
  return out;
}

export type CriticalPath = {
  ids: string[];
  hours: number;
};

/**
 * Longest dep-chain whose every node is in the pending family. Edge
 * weights are estimated_hours (default 1h when missing). Returns the
 * inclusive chain root-to-leaf and the cumulative hours budget so the
 * UI can label the highlight. Empty when no pending root exists.
 */
export function longestPendingPath(slices: FarmPlanSlice[]): CriticalPath {
  const pending = slices.filter((s) => isPendingFamily(s.status));
  if (pending.length === 0) return { ids: [], hours: 0 };

  const idIndex = new Map<string, FarmPlanSlice>();
  for (const s of pending) idIndex.set(s.id, s);

  const order = topologicalOrder(pending);

  // best[id] = { hours, prev } where the path ending at id has the
  // largest cumulative hours seen so far, with prev = predecessor in
  // that path (or null when id is a pending root).
  const best = new Map<string, { hours: number; prev: string | null }>();
  for (const id of order) {
    const slice = idIndex.get(id);
    if (!slice) continue;
    const own = Math.max(1, slice.estimated_hours ?? 1);
    let bestPrev: string | null = null;
    let bestHours = own;
    for (const dep of slice.deps) {
      if (!idIndex.has(dep)) continue;
      const entry = best.get(dep);
      if (!entry) continue;
      const candidate = entry.hours + own;
      if (candidate > bestHours) {
        bestHours = candidate;
        bestPrev = dep;
      }
    }
    best.set(id, { hours: bestHours, prev: bestPrev });
  }

  let leafId: string | null = null;
  let leafHours = -Infinity;
  for (const [id, entry] of best.entries()) {
    if (entry.hours > leafHours) {
      leafHours = entry.hours;
      leafId = id;
    }
  }
  if (leafId == null) return { ids: [], hours: 0 };

  const chain: string[] = [];
  let cursor: string | null = leafId;
  const guard = new Set<string>();
  while (cursor != null && !guard.has(cursor)) {
    guard.add(cursor);
    chain.unshift(cursor);
    cursor = best.get(cursor)?.prev ?? null;
  }
  return { ids: chain, hours: leafHours };
}

/**
 * Distinct values for a categorical field across the slice array,
 * preserving the source order. Used by the filter chip groups.
 */
export function distinctValues(
  slices: FarmPlanSlice[],
  key: "loop" | "phase" | "priority" | "status",
): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const s of slices) {
    const raw = key === "priority" ? s.priority ?? "P2" : s[key];
    if (!raw || seen.has(raw)) continue;
    seen.add(raw);
    out.push(raw);
  }
  return out.sort();
}
