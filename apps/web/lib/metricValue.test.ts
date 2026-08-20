// Reading a chosen metric off a row, where absence is a real answer.
//
// An evaluation run without an information-accretion set does not compute the
// weighted variants at all. The row then has no `f_micro_w`, which is a
// different fact from scoring zero, and drawing a zero would read as a terrible
// result rather than as "this run never measured that".

import { describe, expect, it } from "vitest";

import { PRIMARY_METRIC, byMetricDesc, metricValue } from "@/lib/metrics";

describe("metricValue", () => {
  it("reads the metric asked for", () => {
    expect(metricValue({ fmax: 0.48, f_micro_w: 0.34 }, "fmax")).toBeCloseTo(
      0.48,
    );
    expect(
      metricValue({ fmax: 0.48, f_micro_w: 0.34 }, "f_micro_w"),
    ).toBeCloseTo(0.34);
  });

  it("returns null when the run never computed it", () => {
    // Not zero: the surface must be able to say "not computed for this run".
    expect(metricValue({ fmax: 0.48 }, "f_micro_w")).toBeNull();
    expect(metricValue({ f_micro_w: null }, "f_micro_w")).toBeNull();
  });

  it("returns null rather than passing through a non-number", () => {
    expect(metricValue({ fmax: "0.48" }, "fmax")).toBeNull();
    expect(metricValue({ fmax: NaN }, "fmax")).toBeNull();
    expect(metricValue({ fmax: Infinity }, "fmax")).toBeNull();
  });

  it("keeps a genuine zero, which is not the same as absent", () => {
    expect(metricValue({ coverage: 0 }, "coverage")).toBe(0);
  });
});

describe("byMetricDesc", () => {
  it("ranks high to low", () => {
    const rows = [{ fmax: 0.1 }, { fmax: 0.9 }, { fmax: 0.5 }];
    expect(rows.sort(byMetricDesc("fmax")).map((r) => r.fmax)).toEqual([
      0.9, 0.5, 0.1,
    ]);
  });

  it("puts rows lacking the metric last, not at zero", () => {
    // Sorting a missing value as 0 would rank a run that never measured the
    // metric below one that measured it and scored badly, which is a claim
    // about the method rather than about the evaluation.
    const rows = [{ f_micro_w: 0.2 }, { fmax: 0.9 }, { f_micro_w: 0.4 }];
    const sorted = rows.sort(byMetricDesc("f_micro_w"));
    expect(sorted.map((r) => r.f_micro_w)).toEqual([0.4, 0.2, undefined]);
  });

  it("is stable when neither row has the metric", () => {
    expect(byMetricDesc("f_micro_w")({ fmax: 1 }, { fmax: 2 })).toBe(0);
  });
});

describe("PRIMARY_METRIC", () => {
  it("is the IA-weighted micro F, matching the API default", () => {
    // protea/api/metrics.py picks the same one; a drift here would make the
    // page rank by a different number than the API says it ranked by.
    expect(PRIMARY_METRIC).toBe("f_micro_w");
  });
});
