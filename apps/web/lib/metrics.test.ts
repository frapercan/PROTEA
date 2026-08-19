// The metric vocabulary, tested because the surface used to print bare numbers.
//
// Two independent axes decide what a figure means: macro against micro, and
// IA-weighted against plain. A table that mixes them without saying so is
// uncomparable with itself, and the mistake is invisible in the number.

import { describe, expect, it } from "vitest";

import {
  METRICS,
  isUnweighted,
  metricGroups,
  metricLabel,
  metricSpec,
  metricTooltip,
} from "@/lib/metrics";

describe("the vocabulary covers what an evaluation stores", () => {
  // These are the keys cafaeval writes into every cell of results JSON.
  const STORED = [
    "fmax",
    "fmax_w",
    "f_micro",
    "f_micro_w",
    "precision",
    "precision_w",
    "recall",
    "recall_w",
    "coverage",
    "coverage_w",
  ];

  it.each(STORED)("%s has a spec", (key) => {
    expect(metricSpec(key)).toBeDefined();
  });

  it("keeps all four F combinations rather than picking one for the reader", () => {
    const fs = METRICS.filter((m) => m.key.startsWith("f"));
    expect(fs.map((m) => `${m.averaging}/${m.weighting}`).sort()).toEqual([
      "macro/ia",
      "macro/plain",
      "micro/ia",
      "micro/plain",
    ]);
  });
});

describe("the two axes are recorded, not conflated", () => {
  it("knows fmax is MACRO", () => {
    // cafaeval averages the per-protein score and maximises over tau.
    expect(metricSpec("fmax")?.averaging).toBe("macro");
    expect(metricSpec("fmax_w")?.averaging).toBe("macro");
  });

  it("knows f_micro is MICRO", () => {
    // cafaeval sums tp/fp/fn over the population, then divides.
    expect(metricSpec("f_micro")?.averaging).toBe("micro");
    expect(metricSpec("f_micro_w")?.averaging).toBe("micro");
  });

  it("treats the _w suffix as weighting, orthogonal to averaging", () => {
    expect(metricSpec("f_micro_w")?.weighting).toBe("ia");
    expect(metricSpec("f_micro")?.weighting).toBe("plain");
    expect(metricSpec("fmax_w")?.weighting).toBe("ia");
    expect(metricSpec("fmax")?.weighting).toBe("plain");
  });
});

describe("labels and tooltips", () => {
  it("never renders a bare key as the label", () => {
    expect(metricLabel("f_micro_w")).toBe("F micro, IA-weighted");
    expect(metricLabel("fmax")).toBe("F-max macro, unweighted");
  });

  it("falls back to the key for a metric added upstream", () => {
    // Better its own name than blank or somebody else's label.
    expect(metricLabel("s_min")).toBe("s_min");
    expect(metricTooltip("s_min")).toBe("s_min");
  });

  it("states both axes in the tooltip", () => {
    const t = metricTooltip("f_micro_w");
    expect(t).toContain("micro-averaged");
    expect(t).toContain("IA-weighted");
  });

  it("says unweighted rather than staying silent about it", () => {
    expect(metricTooltip("fmax")).toContain("unweighted");
  });
});

describe("isUnweighted", () => {
  it("flags the cells that are not comparable to the leaderboards", () => {
    // An evaluation run with no information-accretion set does not compute the
    // weighted variants at all, and the number gives no sign of it.
    expect(isUnweighted("fmax")).toBe(true);
    expect(isUnweighted("f_micro")).toBe(true);
    expect(isUnweighted("f_micro_w")).toBe(false);
  });

  it("is false for an unknown key rather than claiming it is plain", () => {
    expect(isUnweighted("s_min")).toBe(false);
  });
});

describe("metricGroups", () => {
  it("leads with the four F scores", () => {
    expect(metricGroups()[0].keys).toEqual([
      "f_micro_w",
      "fmax_w",
      "f_micro",
      "fmax",
    ]);
  });

  it("groups every metric it has a spec for", () => {
    const grouped = metricGroups().flatMap((g) => g.keys);
    expect(new Set(grouped).size).toBe(grouped.length);
    for (const key of grouped) expect(metricSpec(key)).toBeDefined();
  });
});
