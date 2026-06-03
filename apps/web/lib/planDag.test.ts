// FARM-UI.4 unit tests for the slice-DAG helpers. The plan parser
// publishes a flat PlanSlice list; the helpers reconstruct topological
// order and the longest pending-state chain on the client.

import { describe, expect, it } from "vitest";
import {
  distinctValues,
  isPendingFamily,
  longestPendingPath,
  topologicalOrder,
} from "@/lib/planDag";
import type { FarmPlanSlice } from "@/lib/farmApi";

function slice(
  id: string,
  status: string,
  deps: string[],
  hours = 1,
  loop = "demo",
  phase = "F",
  priority = "P2",
): FarmPlanSlice {
  return {
    id,
    title: id,
    loop,
    phase,
    status,
    deps,
    priority,
    estimated_hours: hours,
    tags: [],
    requires_human: false,
    acceptance: "demo",
  };
}

describe("planDag.isPendingFamily", () => {
  it("treats pending / in_progress / blocked as pending-family", () => {
    expect(isPendingFamily("pending")).toBe(true);
    expect(isPendingFamily("in_progress")).toBe(true);
    expect(isPendingFamily("blocked")).toBe(true);
  });
  it("excludes done and deferred", () => {
    expect(isPendingFamily("done")).toBe(false);
    expect(isPendingFamily("deferred")).toBe(false);
  });
});

describe("planDag.topologicalOrder", () => {
  it("returns deps before their dependents on a chain", () => {
    const slices = [
      slice("C", "pending", ["B"]),
      slice("A", "pending", []),
      slice("B", "pending", ["A"]),
    ];
    const order = topologicalOrder(slices);
    expect(order.indexOf("A")).toBeLessThan(order.indexOf("B"));
    expect(order.indexOf("B")).toBeLessThan(order.indexOf("C"));
  });
  it("ignores deps that point at slices outside the working set", () => {
    const slices = [slice("A", "pending", ["missing"])];
    expect(topologicalOrder(slices)).toEqual(["A"]);
  });
});

describe("planDag.longestPendingPath", () => {
  it("returns the longest pending chain weighted by estimated_hours", () => {
    const slices = [
      slice("A", "pending", [], 2),
      slice("B", "pending", ["A"], 3),
      slice("C", "pending", ["B"], 1),
      slice("D", "pending", ["A"], 5),
    ];
    const path = longestPendingPath(slices);
    // A -> D wins (2 + 5 = 7) over A -> B -> C (2 + 3 + 1 = 6).
    expect(path.ids).toEqual(["A", "D"]);
    expect(path.hours).toBe(7);
  });

  it("skips chains broken by a non-pending node", () => {
    const slices = [
      slice("A", "done", [], 4),
      slice("B", "pending", ["A"], 2),
      slice("C", "pending", ["B"], 3),
    ];
    const path = longestPendingPath(slices);
    // A is shipped, so the pending-only chain is B -> C (2 + 3 = 5).
    expect(path.ids).toEqual(["B", "C"]);
    expect(path.hours).toBe(5);
  });

  it("returns empty when every slice is shipped", () => {
    const slices = [
      slice("A", "done", []),
      slice("B", "done", ["A"]),
    ];
    expect(longestPendingPath(slices)).toEqual({ ids: [], hours: 0 });
  });

  it("treats missing hours as 1h", () => {
    const slices = [
      { ...slice("A", "pending", []), estimated_hours: null },
      { ...slice("B", "pending", ["A"]), estimated_hours: null },
    ];
    expect(longestPendingPath(slices).hours).toBe(2);
  });
});

describe("planDag.distinctValues", () => {
  it("returns sorted distinct loop names", () => {
    const slices = [
      slice("X", "pending", [], 1, "loop-b"),
      slice("Y", "pending", [], 1, "loop-a"),
      slice("Z", "pending", [], 1, "loop-b"),
    ];
    expect(distinctValues(slices, "loop")).toEqual(["loop-a", "loop-b"]);
  });

  it("defaults priority to P2 when missing", () => {
    const a = slice("A", "pending", []);
    a.priority = null;
    const b = slice("B", "pending", []);
    b.priority = "P0";
    expect(distinctValues([a, b], "priority")).toEqual(["P0", "P2"]);
  });
});
