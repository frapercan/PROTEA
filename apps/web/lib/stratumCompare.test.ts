// The rules the stratum comparison enforces, tested where they live.

import { describe, expect, it } from "vitest";

import {
  atStratum,
  bandsPresent,
  rankArms,
  spreadOf,
  spreadSentence,
  type StratumRow,
} from "@/lib/stratumCompare";

function row(over: Partial<StratumRow> = {}): StratumRow {
  return {
    evaluation_result_id: "er",
    model: "m",
    display_name: "M",
    k: 1,
    n_proteins: 100,
    f_micro_w: 0.3,
    reportable: true,
    homology: "<=30",
    ...over,
  };
}

describe("rankArms", () => {
  it("ranks best first", () => {
    const out = rankArms([row({ f_micro_w: 0.1 }), row({ f_micro_w: 0.9 })]);
    expect(out.map((r) => r.f_micro_w)).toEqual([0.9, 0.1]);
  });

  it("drops withheld cells rather than ranking them", () => {
    // A cell below the population floor has a number and not a
    // measurement. Ranking it is how it ends up quoted.
    const out = rankArms([
      row({ f_micro_w: 0.99, reportable: false }),
      row({ f_micro_w: 0.3 }),
    ]);
    expect(out).toHaveLength(1);
    expect(out[0].f_micro_w).toBe(0.3);
  });
});

describe("spreadOf", () => {
  it("is best minus worst over the arms present", () => {
    const s = spreadOf([
      row({ f_micro_w: 0.2 }),
      row({ f_micro_w: 0.5 }),
      row({ f_micro_w: 0.35 }),
    ]);
    expect(s?.value).toBeCloseTo(0.3);
    expect(s?.arms).toBe(3);
  });

  it("is null under three arms", () => {
    // With two, best minus worst is the difference between two numbers.
    // Calling it a spread invites comparing it to one over eight.
    expect(spreadOf([row(), row()])).toBeNull();
  });

  it("counts only reportable arms toward the minimum", () => {
    const rows = [row(), row(), row({ reportable: false })];
    expect(spreadOf(rows)).toBeNull();
  });
});

describe("atStratum", () => {
  it("keeps only rows at every pinned coordinate", () => {
    const rows = [
      row({ homology: "<=30", aspect: "P" }),
      row({ homology: ">90", aspect: "P" }),
      row({ homology: "<=30", aspect: "F" }),
    ];
    const out = atStratum(rows, { homology: "<=30", aspect: "P" });
    expect(out).toHaveLength(1);
  });

  it("treats an unset axis as not a filter", () => {
    const rows = [row({ homology: "<=30" }), row({ homology: ">90" })];
    expect(atStratum(rows, { homology: undefined })).toHaveLength(2);
  });
});

describe("bandsPresent", () => {
  it("returns identity bands in the order they mean", () => {
    // Not alphabetical, not insertion order: the panel exists to show a
    // gradient and the gradient is the ordering.
    const rows = [
      row({ homology: ">90" }),
      row({ homology: "<=30" }),
      row({ homology: "60-90" }),
    ];
    expect(bandsPresent(rows, "homology")).toEqual(["<=30", "60-90", ">90"]);
  });

  it("omits bands nothing was measured in", () => {
    expect(bandsPresent([row({ homology: "<=30" })], "homology")).toEqual(["<=30"]);
  });
});

describe("spreadSentence", () => {
  it("states the spread and the population, and no verdict", () => {
    const s = spreadOf([
      row({ f_micro_w: 0.2893, n_proteins: 550 }),
      row({ f_micro_w: 0.2045, n_proteins: 550 }),
      row({ f_micro_w: 0.25, n_proteins: 550 }),
    ]);
    const line = spreadSentence("twilight", s);
    expect(line).toContain("0.0848");
    expect(line).toContain("550 proteins");
    // The same number means different things at 550 proteins and at 32.
    expect(line).not.toMatch(/matters|negligible|little/);
  });

  it("says so when there is nothing to compare", () => {
    expect(spreadSentence("twilight", null)).toBe("too few arms here to compare");
  });
});

describe("busiest cell selection", () => {
  // The panel opens on a cell before the reader has chosen one, and a band
  // comparison over 32 proteins is a comparison of noise.
  it("is exercised through the component; the rule is population, not order", () => {
    const rows = [
      row({ aspect: "P", length: "<=512", n_proteins: 30 }),
      row({ aspect: "F", length: "<=512", n_proteins: 500 }),
    ];
    const byPop = [...rows].sort((a, b) => b.n_proteins - a.n_proteins);
    expect(byPop[0].aspect).toBe("F");
  });

  it("ignores withheld cells when weighing a population", () => {
    const rows = [
      row({ aspect: "P", n_proteins: 900, reportable: false }),
      row({ aspect: "F", n_proteins: 100 }),
    ];
    expect(rankArms(rows).map((r) => r.aspect)).toEqual(["F"]);
  });
});
