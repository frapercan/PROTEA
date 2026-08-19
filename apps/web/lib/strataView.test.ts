// The reshaping that makes a 47-row table readable, tested where it lives.
//
// Most of these pin decisions that are easy to "simplify" into something that
// looks better and says less: dropping thin cells, re-scaling colour per row,
// and averaging cell scores as if the strata held equal populations.

import { describe, expect, it } from "vitest";

import type { StratumCell } from "@/lib/strata";
import {
  HOMOLOGY_ORDER,
  LENGTH_ORDER,
  coverage,
  heatColour,
  linePoints,
  profileByAspect,
  scoreRange,
  sortBands,
  textOn,
  toGrid,
} from "@/lib/strataView";

function cell(
  aspect: string,
  length: string,
  homology: string,
  n: number,
  f: number,
  reportable = true,
): StratumCell {
  return {
    aspect,
    length,
    homology,
    n_proteins: n,
    precision_w: f,
    recall_w: f,
    f_micro_w: f,
    reportable,
  };
}

describe("sortBands", () => {
  it("orders homology by transfer strength, not alphabetically", () => {
    expect(
      sortBands([">90", "<=30", "60-90", "30-60"], HOMOLOGY_ORDER),
    ).toEqual(["<=30", "30-60", "60-90", ">90"]);
  });

  it("orders length by residue count, where alphabetical would be wrong", () => {
    // "1024-2048" sorts before "512-1024" as a string.
    expect(sortBands(["1024-2048", "<=512", "512-1024"], LENGTH_ORDER)).toEqual(
      ["<=512", "512-1024", "1024-2048"],
    );
  });

  it("keeps an unknown band rather than dropping it, at the end", () => {
    expect(sortBands(["mystery", "<=30"], HOMOLOGY_ORDER)).toEqual([
      "<=30",
      "mystery",
    ]);
  });
});

describe("toGrid", () => {
  const cells = [
    cell("F", "<=512", "<=30", 271, 0.24),
    cell("F", "<=512", "60-90", 283, 0.45),
    cell("F", "512-1024", "<=30", 100, 0.17),
  ];

  it("indexes cells by their two axes", () => {
    const g = toGrid(cells, "length", "homology", LENGTH_ORDER, HOMOLOGY_ORDER);
    expect(g.at("<=512", "60-90")?.f_micro_w).toBeCloseTo(0.45);
    expect(g.rows).toEqual(["<=512", "512-1024"]);
    expect(g.cols).toEqual(["<=30", "60-90"]);
  });

  it("returns undefined for a combination nobody observed", () => {
    // Not a zero cell: unobserved and scored-zero are different facts, and a
    // grid that renders both as 0.000 erases the difference.
    const g = toGrid(cells, "length", "homology", LENGTH_ORDER, HOMOLOGY_ORDER);
    expect(g.at("512-1024", "60-90")).toBeUndefined();
  });
});

describe("scoreRange", () => {
  it("spans the reportable cells only", () => {
    // A thin cell scoring 0.99 must not stretch the scale that colours the rest.
    const cells = [
      cell("F", "<=512", "<=30", 271, 0.2),
      cell("F", "<=512", "60-90", 283, 0.4),
      cell("F", "<=512", ">90", 3, 0.99, false),
    ];
    expect(scoreRange(cells)).toEqual([0.2, 0.4]);
  });

  it("is a flat range when nothing is reportable", () => {
    expect(scoreRange([cell("F", "<=512", "<=30", 2, 0.5, false)])).toEqual([
      0, 0,
    ]);
  });
});

describe("heatColour", () => {
  it("darkens with the value", () => {
    const low = heatColour(0.1, 0.1, 0.5);
    const high = heatColour(0.5, 0.1, 0.5);
    expect(low).not.toBe(high);
    // lightness falls as the value rises
    const lightness = (s: string) => Number(/(\d+)%\)$/.exec(s)?.[1]);
    expect(lightness(high)).toBeLessThan(lightness(low));
  });

  it("uses one hue, so it survives greyscale and colour-vision differences", () => {
    const hue = (s: string) => /hsl\((\d+)/.exec(s)?.[1];
    expect(hue(heatColour(0.1, 0, 1))).toBe(hue(heatColour(0.9, 0, 1)));
  });

  it("stays neutral when every cell scores the same", () => {
    expect(heatColour(0.3, 0.3, 0.3)).toContain("20%");
  });

  it("clamps a value outside the range instead of overflowing the ramp", () => {
    expect(heatColour(2, 0, 1)).toBe(heatColour(1, 0, 1));
  });
});

describe("textOn", () => {
  it("switches to white only on the dark end", () => {
    expect(textOn(0.95, 0, 1)).toBe("#ffffff");
    expect(textOn(0.1, 0, 1)).toBe("#0f172a");
  });
});

describe("profileByAspect", () => {
  it("weights bands by population, not by cell count", () => {
    // One aspect, one band, two cells of very different size. A plain mean
    // would give 0.30; weighting by population gives 0.11.
    const cells = [
      cell("F", "<=512", "<=30", 900, 0.1),
      cell("F", "512-1024", "<=30", 100, 0.5),
    ];
    const pts = profileByAspect(cells).F;
    expect(pts).toHaveLength(1);
    expect(pts[0].value).toBeCloseTo((900 * 0.1 + 100 * 0.5) / 1000);
    expect(pts[0].n).toBe(1000);
  });

  it("excludes withheld cells from the line", () => {
    const cells = [
      cell("F", "<=512", "<=30", 500, 0.2),
      cell("F", "<=512", ">90", 3, 0.99, false),
    ];
    expect(profileByAspect(cells).F.map((p) => p.band)).toEqual(["<=30"]);
  });

  it("returns bands in transfer order, not insertion order", () => {
    const cells = [
      cell("F", "<=512", ">90", 10, 0.4),
      cell("F", "<=512", "<=30", 10, 0.2),
    ];
    expect(profileByAspect(cells).F.map((p) => p.band)).toEqual([
      "<=30",
      ">90",
    ]);
  });

  it("keeps aspects separate", () => {
    const cells = [
      cell("F", "<=512", "<=30", 10, 0.4),
      cell("P", "<=512", "<=30", 10, 0.1),
    ];
    const out = profileByAspect(cells);
    expect(out.F[0].value).toBeCloseTo(0.4);
    expect(out.P[0].value).toBeCloseTo(0.1);
  });
});

describe("coverage", () => {
  it("reports the population held back and how many cells hold it", () => {
    const cells = [
      cell("F", "<=512", "<=30", 900, 0.2),
      cell("F", "<=512", ">90", 60, 0.4, false),
      cell("P", "<=512", ">90", 40, 0.3, false),
    ];
    expect(coverage(cells)).toEqual({
      total: 1000,
      withheld: 100,
      cells: 3,
      withheldCells: 2,
    });
  });

  it("does not divide by zero on an empty set", () => {
    expect(coverage([]).total).toBe(0);
  });
});

describe("linePoints", () => {
  it("puts the highest value at the top of the box", () => {
    const pts = [
      { band: "<=30", value: 0.1, n: 10 },
      { band: ">90", value: 0.5, n: 10 },
    ];
    const [first, second] = linePoints(pts, 100, 50, 0.1, 0.5).split(" ");
    expect(Number(first.split(",")[1])).toBeCloseTo(50); // lowest value, bottom
    expect(Number(second.split(",")[1])).toBeCloseTo(0); // highest value, top
  });

  it("does not divide by zero for a single point or a flat range", () => {
    expect(
      linePoints([{ band: "<=30", value: 0.3, n: 1 }], 100, 50, 0.3, 0.3),
    ).toBe("0.0,50.0");
    expect(linePoints([], 100, 50, 0, 1)).toBe("");
  });
});
