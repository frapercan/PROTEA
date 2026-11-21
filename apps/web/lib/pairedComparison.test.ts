// How the window page reads a paired comparison.

import { describe, expect, it } from "vitest";

import {
  armLabel,
  baselinesOf,
  deltaText,
  panelNote,
  verdictTone,
  type PairedComparison,
  type PanelComparison,
} from "@/lib/pairedComparison";

const PANEL: PanelComparison = {
  delta: 0.012,
  ci_low: 0.004,
  ci_high: 0.02,
  interval_method: "bca",
  verdict: "a_greater",
  status: "ok",
  n_paired: 22681,
};

function comparison(a: string, b: string): PairedComparison {
  return {
    job_id: "job",
    evaluation_result_id: a,
    baseline_evaluation_result_id: b,
    finished_at: null,
    method_axis: ["embedding_config_id"],
    operating_point: "reselected_per_resample",
    weighting: "ia_weighted",
    n_resamples: 2000,
    seed: 0,
    confidence: 0.95,
    panels: { "PK:BPO": PANEL },
  };
}

describe("armLabel", () => {
  it("names a scored set by its model's own name and its scoring", () => {
    expect(armLabel({ model: "facebook/esm2_t33_650M_UR50D", scoring: "vote_fraction", method: null })).toBe(
      "esm2_t33_650M_UR50D · vote_fraction",
    );
  });

  it("names a method's set by the method", () => {
    expect(armLabel({ model: null, scoring: "method-score", method: "own-annotations-nonexp-211" })).toBe(
      "own-annotations-nonexp-211 · method-score",
    );
    expect(armLabel(null)).toBeNull();
  });
});

describe("a panel", () => {
  it("prints the difference with its interval, signed", () => {
    expect(deltaText(PANEL)).toBe("+0.012 [+0.004, +0.020]");
    expect(deltaText({ ...PANEL, delta: -0.03, ci_low: -0.05, ci_high: -0.01 })).toBe(
      "−0.030 [−0.050, −0.010]",
    );
    expect(deltaText({ ...PANEL, delta: null })).toBe("—");
  });

  it("reads its verdict as a direction", () => {
    expect(verdictTone(PANEL)).toBe("higher");
    expect(verdictTone({ ...PANEL, verdict: "b_greater" })).toBe("lower");
    expect(verdictTone({ ...PANEL, verdict: "not_resolved", status: "null_unread" })).toBe("unresolved");
  });

  it("says in its tooltip what was compared and how", () => {
    const note = panelNote(PANEL, comparison("a", "b"));
    expect(note).toContain("95 % BCa interval");
    expect(note).toContain("22,681 paired proteins");
    expect(note).toContain("2,000 resamples, seed 0");
    expect(note).toContain("varies embedding_config_id");
  });
});

describe("baselinesOf", () => {
  it("lists the baselines, the most used first", () => {
    const all = [comparison("a", "x"), comparison("b", "y"), comparison("c", "y")];
    expect(baselinesOf(all)).toEqual(["y", "x"]);
  });
});
