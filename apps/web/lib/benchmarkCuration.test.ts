// Unit tests for the reversible benchmark presentation curation. The central
// guarantee: the default view hides probe + superseded-internal rows, the
// "show all" path is the identity, and nothing crashes when the provenance
// columns are null (the mid-recompute state).

import { describe, expect, it } from "vitest";
import type { BenchmarkBestCell, BenchmarkRow } from "@/lib/api";
import {
  bestPerCellFromRows,
  curateBestCells,
  curateRows,
  evalSetIdsWithRows,
  isPreservedEvalSet,
  isProbeRole,
  isSupersededInternal,
  lafaCellKeys,
  perTaskFromRows,
} from "@/lib/benchmarkCuration";

function row(partial: Partial<BenchmarkRow>): BenchmarkRow {
  return {
    embedding_config_id: "emb-1",
    evaluation_set_id: "es-1",
    stage: "embedding_only",
    k: 10,
    category: "NK",
    aspect: "BPO",
    primary: 0.5,
    primary_metric: "f_micro_w",
    f_micro_w: 0.5,
    precision_w: null,
    recall_w: null,
    fmax: 0.5,
    precision: null,
    recall: null,
    coverage: null,
    n_proteins: null,
    evaluation_result_id: "er-1",
    frame: null,
    temporal_window: null,
    arms_enabled: null,
    leakage_role: null,
    ...partial,
  };
}

describe("isProbeRole", () => {
  it("only flags the explicit probe role", () => {
    expect(isProbeRole("probe")).toBe(true);
    expect(isProbeRole("select")).toBe(false);
    expect(isProbeRole("test")).toBe(false);
    expect(isProbeRole(null)).toBe(false);
    expect(isProbeRole(undefined)).toBe(false);
  });
});

describe("isPreservedEvalSet", () => {
  it("matches the canonical eval-set prefixes case-insensitively", () => {
    expect(isPreservedEvalSet("817c6b9f-0000-0000-0000-000000000000")).toBe(true);
    expect(isPreservedEvalSet("3B6F8064-aaaa")).toBe(true);
    expect(isPreservedEvalSet("deadbeef-0000")).toBe(false);
  });
});

describe("lafaCellKeys / isSupersededInternal", () => {
  it("treats internal rows as superseded only when a LAFA row shares the cell", () => {
    const rows = [
      row({ frame: "lafa", embedding_config_id: "e1", category: "NK", aspect: "BPO" }),
      row({ frame: "internal", embedding_config_id: "e1", category: "NK", aspect: "BPO" }),
      row({ frame: "internal", embedding_config_id: "e1", category: "LK", aspect: "BPO" }),
    ];
    const keys = lafaCellKeys(rows);
    expect(isSupersededInternal(rows[1], keys)).toBe(true);
    // No LAFA counterpart for the LK cell -> not superseded.
    expect(isSupersededInternal(rows[2], keys)).toBe(false);
  });
});

describe("curateRows", () => {
  it("is the identity when showAll is true", () => {
    const rows = [row({ leakage_role: "probe" }), row({ frame: "internal" })];
    const out = curateRows(rows, true);
    expect(out.rows).toHaveLength(2);
    expect(out.hiddenTotal).toBe(0);
  });

  it("hides probe rows and superseded internal rows by default", () => {
    const rows = [
      row({ leakage_role: "select", frame: "lafa", embedding_config_id: "e1" }),
      row({ leakage_role: "probe", embedding_config_id: "e2" }),
      row({ frame: "internal", embedding_config_id: "e1" }), // superseded by the lafa row
      row({ frame: "internal", embedding_config_id: "e9" }), // no lafa counterpart -> kept
    ];
    const out = curateRows(rows, false);
    expect(out.hiddenProbe).toBe(1);
    expect(out.hiddenLegacy).toBe(1);
    expect(out.hiddenTotal).toBe(2);
    expect(out.rows).toHaveLength(2);
  });

  it("is a graceful no-op when provenance is null (mid-recompute)", () => {
    const rows = [row({}), row({ embedding_config_id: "e2" })];
    const out = curateRows(rows, false);
    expect(out.rows).toHaveLength(2);
    expect(out.hiddenTotal).toBe(0);
  });
});

describe("evalSetIdsWithRows", () => {
  it("collects the distinct surviving eval-set ids", () => {
    const ids = evalSetIdsWithRows([
      row({ evaluation_set_id: "a" }),
      row({ evaluation_set_id: "a" }),
      row({ evaluation_set_id: "b" }),
    ]);
    expect([...ids].sort()).toEqual(["a", "b"]);
  });
});

describe("bestPerCellFromRows", () => {
  it("keeps the max-primary row per (category, aspect)", () => {
    const rows = [
      row({ category: "NK", aspect: "BPO", primary: 0.3, evaluation_result_id: "lo" }),
      row({ category: "NK", aspect: "BPO", primary: 0.8, evaluation_result_id: "hi" }),
      row({ category: "LK", aspect: "MFO", primary: 0.4, evaluation_result_id: "x" }),
    ];
    const best = bestPerCellFromRows(rows, ["NK", "LK"], ["BPO", "MFO"]);
    const nkBpo = best.find((b) => b.category === "NK" && b.aspect === "BPO");
    expect(nkBpo?.evaluation_result_id).toBe("hi");
    expect(best).toHaveLength(2);
  });
});

describe("perTaskFromRows", () => {
  it("matches the backend mean / CI formula", () => {
    const rows = [
      row({ category: "NK", aspect: "BPO", primary: 0.4 }),
      row({ category: "NK", aspect: "BPO", primary: 0.6 }),
    ];
    const agg = perTaskFromRows(rows, ["NK"], ["BPO"]);
    expect(agg).toHaveLength(1);
    expect(agg[0].mean).toBeCloseTo(0.5, 4);
    // sd = 0.1414 (n-1), ci = 1.96 * 0.1414 / sqrt(2) = 0.196
    expect(agg[0].ci95).toBeCloseTo(0.196, 3);
    expect(agg[0].n_models).toBe(2);
  });

  it("reports a zero CI for a single model", () => {
    const agg = perTaskFromRows([row({ primary: 0.7 })], ["NK"], ["BPO"]);
    expect(agg[0].ci95).toBe(0);
    expect(agg[0].mean).toBe(0.7);
  });
});

describe("curateBestCells", () => {
  function cell(partial: Partial<BenchmarkBestCell>): BenchmarkBestCell {
    return {
      category: "NK",
      aspect: "BPO",
      primary: 0.5,
      primary_metric: "f_micro_w",
      f_micro_w: 0.5,
      precision_w: null,
      recall_w: null,
      fmax: 0.5,
      precision: null,
      recall: null,
      coverage: null,
      embedding_config_id: "e1",
      k: 10,
      stage: "embedding_only",
      evaluation_result_id: "er",
      evaluation_set_id: "es",
      frame: null,
      temporal_window: null,
      arms_enabled: null,
      leakage_role: null,
      ...partial,
    };
  }

  it("drops probe champions in the default view but keeps them under show-all", () => {
    const cells = [cell({ leakage_role: "probe" }), cell({ category: "LK" })];
    expect(curateBestCells(cells, false, new Set())).toHaveLength(1);
    expect(curateBestCells(cells, true, new Set())).toHaveLength(2);
  });
});
