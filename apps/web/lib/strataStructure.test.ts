// The resolvers behind the fine stratification, tested apart from the DOM.
//
// Four rules carry the section, and each one is a test here rather than a
// comment in the component:
//
//   1. populations are summed inside one arm and never across arms;
//   2. a cell is judged by the SMALLEST population any arm reports for it;
//   3. an axis whose composition moves with the arm is measurably different
//      from one whose composition does not;
//   4. a panel with fewer than two clearing cells does not admit the split.

import { describe, it, expect } from "vitest";

import type { ContrastFloor } from "@/lib/graph";
import {
  armCoverage,
  armsIncomplete,
  compositionDrift,
  crossing,
  floorRank,
  panelsPresent,
  routingRegions,
  tripleByPanel,
  tripleVerdict,
  type CompareRow,
  type SettingLoad,
} from "@/lib/strataStructure";

const FLOORS: ContrastFloor[] = [
  { key: "reporting", sigma_paired: 0.081, population: 129, contrast: "cheap" },
  { key: "routing", sigma_paired: 0.13, population: 332, contrast: "dear" },
];

function row(over: Partial<CompareRow> = {}): CompareRow {
  return {
    evaluation_result_id: "arm-1",
    model: "m",
    display_name: "m",
    k: 10,
    category: "NK",
    aspect: "P",
    length: "<=512",
    homology: "60-90",
    n_proteins: 100,
    ...over,
  };
}

describe("crossing", () => {
  it("sums along the collapsed axes inside one arm", () => {
    const cells = crossing(
      [
        row({ homology: "60-90", n_proteins: 100 }),
        row({ homology: "<=30", n_proteins: 40 }),
      ],
      ["category", "aspect", "length"],
    );
    expect(cells).toHaveLength(1);
    expect(cells[0].low).toBe(140);
    expect(cells[0].high).toBe(140);
    expect(cells[0].arms).toBe(1);
  });

  it("never adds two arms together, and keeps their disagreement", () => {
    const cells = crossing(
      [
        row({ evaluation_result_id: "a", n_proteins: 100 }),
        row({ evaluation_result_id: "b", n_proteins: 130 }),
      ],
      ["category", "aspect", "length", "homology"],
    );
    expect(cells).toHaveLength(1);
    // 230 would be the sum. The cell is one cell under two arms.
    expect(cells[0].low).toBe(100);
    expect(cells[0].high).toBe(130);
    expect(cells[0].arms).toBe(2);
  });

  it("drops a row that names no value on an axis being crossed", () => {
    const cells = crossing([row({ homology: undefined })], [
      "category",
      "aspect",
      "homology",
    ]);
    expect(cells).toHaveLength(0);
  });
});

describe("floorRank", () => {
  it("returns the strictest floor a population clears", () => {
    expect(floorRank(400, FLOORS)).toBe(1);
    expect(floorRank(332, FLOORS)).toBe(1);
    expect(floorRank(331, FLOORS)).toBe(0);
    expect(floorRank(128, FLOORS)).toBe(-1);
  });

  it("clears nothing when no floor is declared", () => {
    expect(floorRank(10_000, [])).toBe(-1);
  });
});

describe("compositionDrift", () => {
  const arms = (axis: "length" | "homology", a: number, b: number): CompareRow[] => [
    row({ evaluation_result_id: "a", [axis]: "x", n_proteins: a }),
    row({ evaluation_result_id: "a", [axis]: "y", n_proteins: 100 - a }),
    row({ evaluation_result_id: "b", [axis]: "x", n_proteins: b }),
    row({ evaluation_result_id: "b", [axis]: "y", n_proteins: 100 - b }),
  ];

  it("is zero when the arms disagree about counts but not about shape", () => {
    const rows: CompareRow[] = [
      row({ evaluation_result_id: "a", length: "x", n_proteins: 50 }),
      row({ evaluation_result_id: "a", length: "y", n_proteins: 50 }),
      // Twice the population, identical composition.
      row({ evaluation_result_id: "b", length: "x", n_proteins: 100 }),
      row({ evaluation_result_id: "b", length: "y", n_proteins: 100 }),
    ];
    expect(compositionDrift(rows, "length")).toBeCloseTo(0, 10);
  });

  it("rises with the share the arms disagree about", () => {
    expect(compositionDrift(arms("homology", 50, 70), "homology")).toBeCloseTo(0.2, 10);
  });

  it("is null under a single arm, which cannot disagree with itself", () => {
    expect(compositionDrift([row()], "length")).toBeNull();
  });
});

describe("the triple crossing", () => {
  const rows: CompareRow[] = [
    // One panel with a cell over the routing floor under both arms.
    row({ category: "PK", n_proteins: 900, evaluation_result_id: "a" }),
    row({ category: "PK", n_proteins: 800, evaluation_result_id: "b" }),
    // One panel whose cell clears under one arm only.
    row({ category: "NK", n_proteins: 400, evaluation_result_id: "a" }),
    row({ category: "NK", n_proteins: 300, evaluation_result_id: "b" }),
  ];
  const panels = panelsPresent(rows, ["NK", "LK", "PK"], ["P", "F", "C"]);

  it("keeps the panels the record touched, in the canonical order", () => {
    expect(panels).toEqual([
      { category: "NK", aspect: "P" },
      { category: "PK", aspect: "P" },
    ]);
  });

  it("does not credit a cell that clears under one arm and not the other", () => {
    const byPanel = tripleByPanel(rows, panels, FLOORS);
    const nk = byPanel.find((p) => p.category === "NK");
    const pk = byPanel.find((p) => p.category === "PK");
    expect(nk?.withPopulation).toBe(1);
    // 400 clears 332 and 300 does not, so the cell has not cleared.
    expect(nk?.clearing).toBe(0);
    expect(pk?.clearing).toBe(1);
  });

  it("counts the panels that come back empty", () => {
    const verdict = tripleVerdict(tripleByPanel(rows, panels, FLOORS));
    expect(verdict.cells).toBe(2);
    expect(verdict.clearing).toBe(1);
    expect(verdict.panels).toBe(2);
    expect(verdict.panelsWithNone).toBe(1);
    expect(verdict.categoriesClearing).toEqual(["PK"]);
  });
});

describe("routingRegions", () => {
  it("counts only the cells that clear the strictest floor", () => {
    const cells = crossing(
      [
        row({ length: "<=512", n_proteins: 900 }),
        row({ length: "512-1024", n_proteins: 400 }),
        row({ length: ">2048", n_proteins: 20 }),
      ],
      ["category", "aspect", "length"],
    );
    expect(routingRegions(cells, FLOORS)).toBe(2);
  });

  it("counts nothing when the record declares no floor", () => {
    const cells = crossing([row({ n_proteins: 9_000 })], ["category", "aspect", "length"]);
    expect(routingRegions(cells, [])).toBe(0);
  });
});

describe("arm coverage", () => {
  const load = (over: Partial<SettingLoad> & { setting: string }): SettingLoad => ({
    state: "ok",
    payload: null,
    message: null,
    ...over,
  });

  it("reports a setting whose arms were not all stratified", () => {
    const loads = [
      load({
        setting: "NK",
        payload: {
          evaluation_set_id: "e",
          setting: "NK",
          where: {},
          arms_total: 16,
          arms_with_strata: 8,
          rows: [],
        },
      }),
    ];
    expect(armCoverage(loads)[0]).toEqual({
      setting: "NK",
      total: 16,
      withStrata: 8,
      state: "ok",
    });
    expect(armsIncomplete(armCoverage(loads))).toBe(true);
  });

  it("does not call a setting incomplete because it failed to load", () => {
    const loads = [load({ setting: "LK", state: "absent" })];
    expect(armsIncomplete(armCoverage(loads))).toBe(false);
  });
});
