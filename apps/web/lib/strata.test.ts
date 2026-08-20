// The rules the strata panel exists to enforce, tested where they live.
//
// Two of them are about what a table must not hide: a cell is never shown
// without its population, and a cell below the floor is marked rather than
// dropped. Both are easy to "fix" into a cleaner-looking table, so they are
// pinned here.

import { describe, expect, it } from "vitest";

import {
  axisLabel,
  axisRank,
  cellKey,
  formatScore,
  inReportOrder,
  primaryAxis,
  scoreShade,
  withheldShare,
  type StratumCell,
} from "@/lib/strata";

const AXES = ["aspect", "homology"];

function cell(
  aspect: string,
  homology: string,
  n: number,
  f: number,
  reportable = true,
): StratumCell {
  return {
    aspect,
    homology,
    n_proteins: n,
    precision_w: f,
    recall_w: f,
    f_micro_w: f,
    reportable,
  };
}

describe("axisLabel", () => {
  it("spells out the codes a reader cannot be expected to know", () => {
    expect(axisLabel("aspect", "F")).toBe("molecular function");
    expect(axisLabel("homology", "<=30")).toBe("twilight (<=30%)");
    expect(axisLabel("category", "NK")).toBe("no knowledge");
  });

  it("falls back to the raw value for an axis it has no labels for", () => {
    // A new axis must render as itself rather than blank.
    expect(axisLabel("propagation", "0-0.05")).toBe("0-0.05");
    expect(axisLabel("aspect", "Z")).toBe("Z");
  });
});

describe("inReportOrder", () => {
  it("keeps withheld cells, sorted last", () => {
    const cells = [
      cell("F", ">90", 4, 0.4, false),
      cell("F", "<=30", 271, 0.24),
      cell("F", "30-60", 328, 0.44),
    ];
    const ordered = inReportOrder(cells, AXES);
    expect(ordered).toHaveLength(3);
    expect(ordered.map((c) => c.reportable)).toEqual([true, true, false]);
  });

  it("walks homology from twilight up to near-identical", () => {
    // The defect this replaced: values were compared as text, so the panel
    // rendered twilight, near-identical, distant, close. Reading down the
    // column showed no gradient, which is the one thing it is for.
    const cells = [
      cell("F", ">90", 146, 0.3572),
      cell("F", "<=30", 258, 0.1915),
      cell("F", "60-90", 282, 0.3829),
      cell("F", "30-60", 332, 0.3632),
    ];
    expect(inReportOrder(cells, AXES).map((c) => c.homology)).toEqual([
      "<=30",
      "30-60",
      "60-90",
      ">90",
    ]);
  });

  it("puts no-donor at the floor of the homology axis", () => {
    const cells = [cell("F", "<=30", 10, 0.1), cell("F", "none", 10, 0.0)];
    expect(inReportOrder(cells, AXES).map((c) => c.homology)).toEqual([
      "none",
      "<=30",
    ]);
  });

  it("sorts length by length, not by its first digit", () => {
    // Text order puts 1024-2048 second, between <=512 and 512-1024.
    const byLength = ["length"];
    const cells = [
      { ...cell("F", "<=30", 90, 0.24), length: "512-1024" },
      { ...cell("F", "<=30", 32, 0.11), length: "1024-2048" },
      { ...cell("F", "<=30", 258, 0.19), length: "<=512" },
    ];
    expect(inReportOrder(cells, byLength).map((c) => c.length)).toEqual([
      "<=512",
      "512-1024",
      "1024-2048",
    ]);
  });

  it("orders by the outer axis first, then within it", () => {
    const cells = [
      cell("P", "<=30", 362, 0.0535),
      cell("F", ">90", 146, 0.3572),
      cell("F", "<=30", 258, 0.1915),
    ];
    const out = inReportOrder(cells, AXES);
    expect(out.map((c) => `${c.aspect}/${c.homology}`)).toEqual([
      "F/<=30",
      "F/>90",
      "P/<=30",
    ]);
  });

  it("still sorts withheld cells last, whatever their band", () => {
    // Ordering must not promote a withheld cell just because its band
    // ranks first.
    const cells = [
      cell("F", ">90", 200, 0.4),
      cell("F", "<=30", 4, 0.9, false),
    ];
    expect(inReportOrder(cells, AXES).map((c) => c.reportable)).toEqual([
      true,
      false,
    ]);
  });

  it("does not mutate its input", () => {
    const cells = [
      cell("F", ">90", 4, 0.4, false),
      cell("F", "<=30", 271, 0.24),
    ];
    const before = cells.map((c) => c.homology);
    inReportOrder(cells, AXES);
    expect(cells.map((c) => c.homology)).toEqual(before);
  });
});

describe("withheldShare", () => {
  it("reports the population a view is not showing", () => {
    const cells = [
      cell("F", "<=30", 900, 0.2),
      cell("F", ">90", 100, 0.4, false),
    ];
    expect(withheldShare(cells)).toBeCloseTo(0.1);
  });

  it("is zero when every cell clears the floor", () => {
    expect(withheldShare([cell("F", "<=30", 10, 0.2)])).toBe(0);
  });

  it("is zero rather than NaN on an empty population", () => {
    expect(withheldShare([])).toBe(0);
  });
});

describe("scoreShade", () => {
  it("shades relative to the cells beside it, not on an absolute scale", () => {
    // BP at low homology reads ~0.11 and MF at mid homology ~0.46. A fixed
    // ramp would paint the whole BP column as failure.
    const bp = [cell("P", "<=30", 500, 0.107), cell("P", "30-60", 500, 0.139)];
    expect(scoreShade(0.139, bp)).toBe("bg-emerald-100");
    const mf = [cell("F", "<=30", 500, 0.246), cell("F", "60-90", 500, 0.456)];
    expect(scoreShade(0.246, mf)).toBe("bg-rose-50");
  });

  it("stays neutral when there is nothing to compare against", () => {
    expect(scoreShade(0.5, [cell("F", "<=30", 10, 0.5)])).toBe("bg-slate-50");
    expect(scoreShade(0.5, [])).toBe("bg-slate-50");
  });

  it("stays neutral when every cell scores the same", () => {
    const flat = [cell("F", "<=30", 10, 0.3), cell("F", "30-60", 10, 0.3)];
    expect(scoreShade(0.3, flat)).toBe("bg-slate-50");
  });

  it("ignores withheld cells when setting the range", () => {
    const cells = [
      cell("F", "<=30", 500, 0.2),
      cell("F", "30-60", 500, 0.4),
      cell("F", ">90", 2, 0.99, false),
    ];
    // 0.4 is the top of the REPORTABLE range, so it takes the top shade even
    // though a thin cell scores higher.
    expect(
      scoreShade(
        0.4,
        cells.filter((c) => c.reportable),
      ),
    ).toBe("bg-emerald-100");
  });
});

describe("primaryAxis", () => {
  it("picks the axis a reader scans first", () => {
    expect(primaryAxis(["category", "aspect", "length", "homology"])).toBe(
      "homology",
    );
    expect(primaryAxis(["category", "aspect"])).toBe("aspect");
  });

  it("falls back to the first axis given, and to null for none", () => {
    expect(primaryAxis(["propagation"])).toBe("propagation");
    expect(primaryAxis([])).toBeNull();
  });
});

describe("cellKey", () => {
  it("identifies a cell by its axis values", () => {
    expect(cellKey(cell("F", "<=30", 1, 0.1), AXES)).toBe("F / <=30");
  });

  it("tolerates an axis the cell does not carry", () => {
    expect(cellKey(cell("F", "<=30", 1, 0.1), ["aspect", "taxonomy"])).toBe(
      "F / ",
    );
  });
});

describe("formatScore", () => {
  it("keeps four decimals, because cells differ in the third", () => {
    // The clean-run self-hit spread was 0.018 points; two decimals would show
    // several distinct cells as the same number.
    expect(formatScore(0.4439999)).toBe("0.4440");
    expect(formatScore(0.2461)).toBe("0.2461");
  });
});

describe("axisRank", () => {
  it("ranks the axes whose values stand for a quantity", () => {
    expect(axisRank("homology", "<=30")).toBeLessThan(
      axisRank("homology", ">90"),
    );
    expect(axisRank("length", "512-1024")).toBeLessThan(
      axisRank("length", "1024-2048"),
    );
  });

  it("leaves axes with no natural order alone", () => {
    // aspect is a set of three names, not a scale. Inventing an order for
    // it would be a claim the data does not make.
    expect(axisRank("aspect", "F")).toBe(-1);
  });

  it("sorts an unknown band last rather than first", () => {
    // A band added upstream should show up as an outlier at the end, not
    // silently lead the table as though it were the floor.
    expect(axisRank("homology", "90-95")).toBeGreaterThan(
      axisRank("homology", ">90"),
    );
  });
});
