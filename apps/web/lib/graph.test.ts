/**
 * The graph client's pure parts.
 *
 * Two of these encode rules rather than behaviour. `panelLevels` returns a
 * neutral order because ranking levels across panels would need one number
 * per level over the nine, and building that number is the collapse the
 * model forbids. `panelSummary` stays inside one panel for the same
 * reason: a spread is only meaningful against the population it was
 * measured over.
 */

import { describe, it, expect } from "vitest";
import {
  indexPanelResults,
  indexPanels,
  isEmptyGraph,
  panelKey,
  panelLevels,
  panelSummary,
  type GraphPanel,
  type GraphResponse,
} from "@/lib/graph";

const panel = (
  category: string,
  aspect: string,
  units: number,
  results: { level: string; f_micro_w: number; tau: number }[] = [],
): GraphPanel => ({ category, aspect, units, detectable_effect: null, results });

const emptyFrame: GraphResponse["frame"] = {
  declared: false,
  evaluation_set_id: null,
  window: null,
  window_span: null,
  window_role: null,
  mode: null,
  pivot_snapshot: null,
  information_accretion_set: null,
  query_set: null,
  sealed_rows: 0,
  unsealed_rows: 0,
};

describe("panelLevels", () => {
  it("collects every level that appears on any panel", () => {
    const levels = panelLevels([
      panel("NK", "BPO", 10, [{ level: "b", f_micro_w: 0.2, tau: 0.5 }]),
      panel("PK", "CCO", 20, [
        { level: "a", f_micro_w: 0.1, tau: 0.5 },
        { level: "b", f_micro_w: 0.3, tau: 0.5 },
      ]),
    ]);
    expect(levels).toEqual(["a", "b"]);
  });

  it("orders alphabetically, which is a claim about nothing", () => {
    const levels = panelLevels([
      panel("NK", "BPO", 10, [
        { level: "zeta", f_micro_w: 0.9, tau: 0.5 },
        { level: "alpha", f_micro_w: 0.1, tau: 0.5 },
      ]),
    ]);
    expect(levels).toEqual(["alpha", "zeta"]);
  });
});

describe("panelSummary", () => {
  const p = panel("NK", "BPO", 1509, [
    { level: "embedding_only", f_micro_w: 0.1682, tau: 0.99 },
    { level: "composite_no_embedding", f_micro_w: 0.2652, tau: 0.43 },
    { level: "vote_fraction", f_micro_w: 0.2343, tau: 0.31 },
  ]);

  it("names the leader and the spread inside one panel", () => {
    const s = panelSummary(p, "f_micro_w")!;
    expect(s.best.level).toBe("composite_no_embedding");
    expect(s.spread).toBeCloseTo(0.097, 6);
  });

  it("re-reads both when the metric changes", () => {
    const s = panelSummary(p, "tau")!;
    expect(s.best.level).toBe("embedding_only");
    expect(s.spread).toBeCloseTo(0.68, 6);
  });

  it("returns null for a panel with a population and no result", () => {
    expect(panelSummary(panel("LK", "CCO", 821), "f_micro_w")).toBeNull();
  });
});

describe("indexing", () => {
  it("keys panels by category and aspect", () => {
    const panels = [panel("NK", "BPO", 1), panel("PK", "MFO", 2)];
    const idx = indexPanels(panels);
    expect(idx.get(panelKey("PK", "MFO"))?.units).toBe(2);
    expect(idx.get(panelKey("LK", "BPO"))).toBeUndefined();
  });

  it("returns an empty map for a panel that is not there", () => {
    expect(indexPanelResults(undefined).size).toBe(0);
  });
});

describe("isEmptyGraph", () => {
  it("is true only when nothing at all is instantiated", () => {
    expect(
      isEmptyGraph({ frame: emptyFrame, nodes: [], timeline: null, panels: [], blocked: [] }),
    ).toBe(true);
  });

  it("is false as soon as a frame is declared", () => {
    expect(
      isEmptyGraph({
        frame: { ...emptyFrame, declared: true },
        nodes: [],
        timeline: null,
        panels: [],
        blocked: [],
      }),
    ).toBe(false);
  });

  it("is false when a panel has a population but no result", () => {
    // The distinction the page turns on: a panel with 5,811 units and no
    // score is a measurement that has not been made, not an absent panel.
    expect(
      isEmptyGraph({
        frame: emptyFrame,
        timeline: null,
        nodes: [],
        panels: [panel("PK", "BPO", 5811)],
        blocked: [],
      }),
    ).toBe(false);
  });

  it("is false when a node carries a level, whatever its strength", () => {
    expect(
      isEmptyGraph({
        frame: emptyFrame,
        timeline: null,
        nodes: [
          {
            key: "substrate",
            held: [],
            title: "Substrate",
            stage: 1,
            question: "which representation?",
            strength: "inherited",
            levels_instantiated: 1,
            levels_available: 13,
            varying_fields: [],
            constant_fields: [],
            blocked_reason: null,
            results: 0,
          },
        ],
        panels: [],
        blocked: [],
      }),
    ).toBe(false);
  });
});
