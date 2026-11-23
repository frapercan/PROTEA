// The window page says where each headline was read, and when that is an edge.

import { describe, expect, it } from "vitest";

import { edgeMark, headlineMaximum, maximumNote } from "@/lib/headlineMaximum";

// 211->227, LK CCO of result 5a5b4c20: the unweighted Fmax peaks inside the
// grid, f_micro_w at its top.
const LK_CCO = {
  tau: 0.97,
  maxima: {
    f: { tau: 0.97, edge: null },
    f_micro_w: { tau: 0.99, edge: "top" as const },
  },
};

describe("headlineMaximum", () => {
  it("reads the headline's own threshold, not the cell's tau", () => {
    expect(headlineMaximum(LK_CCO)).toEqual({ tau: 0.99, edge: "top" });
  });

  it("is null for a result that records no maxima", () => {
    expect(headlineMaximum({})).toBeNull();
    expect(headlineMaximum({ maxima: { f: { tau: 0.5, edge: null } } })).toBeNull();
  });
});

describe("edgeMark and maximumNote", () => {
  it("marks a maximum at either edge and says it is not a maximum", () => {
    const top = headlineMaximum(LK_CCO)!;
    expect(edgeMark(top)).not.toBe("");
    expect(maximumNote(top)).toMatch(/threshold 0\.99, the highest threshold/);
    expect(maximumNote(top)).toMatch(/not a maximum/);
    const bottom = { tau: 0.01, edge: "bottom" as const };
    expect(edgeMark(bottom)).not.toBe("");
    expect(edgeMark(bottom)).not.toBe(edgeMark(top));
  });

  it("leaves an interior maximum unmarked", () => {
    const inside = { tau: 0.42, edge: null };
    expect(edgeMark(inside)).toBe("");
    expect(edgeMark(null)).toBe("");
    expect(maximumNote(inside)).toBe("f_micro_w reported at threshold 0.42, an interior maximum");
  });
});
