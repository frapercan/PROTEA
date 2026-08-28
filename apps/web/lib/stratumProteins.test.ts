// The rules the descent into a panel enforces, tested where they live.

import { describe, expect, it } from "vitest";

import {
  ARM_FIELDS,
  STRATUM_PATH,
  armLabel,
  aspectCafa,
  aspectWire,
  groupArms,
  isSort,
  soleCell,
  stratumHref,
  varyingArmFields,
} from "@/lib/stratumProteins";
import type { StratumRow } from "@/lib/stratumCompare";

function row(over: Partial<StratumRow> = {}): StratumRow {
  return {
    evaluation_result_id: "er-1",
    model: "facebook/esm2_t33_650M_UR50D",
    display_name: "esm2_650m",
    k: 10,
    scoring_name: "composite",
    donor_policy: "permissive",
    metric: "cosine",
    n_proteins: 314,
    f_micro_w: 0.28,
    reportable: true,
    category: "NK",
    aspect: "P",
    length: "<=512",
    homology: "30-60",
    ...over,
  };
}

describe("groupArms", () => {
  it("keeps one entry per arm and every cell it holds", () => {
    // The compare endpoint returns one row per (arm, cell), and a panel is
    // crossed by length and homology, so an arm appears once per band.
    const arms = groupArms([
      row({ homology: "<=30" }),
      row({ homology: "30-60" }),
      row({ evaluation_result_id: "er-2", homology: "<=30" }),
    ]);
    expect(arms).toHaveLength(2);
    expect(arms[0].cells).toHaveLength(2);
    expect(arms[1].cells).toHaveLength(1);
  });

  it("keeps the order the endpoint sent, so the picker does not reshuffle", () => {
    const arms = groupArms([
      row({ evaluation_result_id: "b" }),
      row({ evaluation_result_id: "a" }),
    ]);
    expect(arms.map((a) => a.evaluation_result_id)).toEqual(["b", "a"]);
  });
});

describe("soleCell", () => {
  it("is null when the coordinates name more than one cell", () => {
    // This is what stops a score being printed beside an arm that holds every
    // band of the panel. A mean over bands whose populations differ by an
    // order of magnitude promotes the smallest and easiest of them.
    const arms = groupArms([row({ homology: "<=30" }), row({ homology: ">90" })]);
    expect(soleCell(arms[0])).toBeNull();
  });

  it("is the cell when the coordinates name exactly one", () => {
    const arms = groupArms([row({ homology: "<=30", f_micro_w: 0.07 })]);
    expect(soleCell(arms[0])?.f_micro_w).toBe(0.07);
  });
});

describe("varyingArmFields", () => {
  it("names an arm by whichever fields actually moved", () => {
    // Eight arms of this campaign share a prediction set and differ only in
    // the scoring configuration. A label built from the model alone would
    // print eight identical rows at eight different scores.
    const arms = groupArms([
      row({ evaluation_result_id: "a", scoring_name: "composite" }),
      row({ evaluation_result_id: "b", scoring_name: "alignment_only" }),
    ]);
    expect(varyingArmFields(arms)).toEqual(["scoring_name"]);
    expect(armLabel(arms[0], varyingArmFields(arms))).toBe("composite");
  });

  it("falls back to the whole vocabulary when nothing varies", () => {
    // With one arm nothing moves, and a label of the empty string names
    // nothing at all.
    const arms = groupArms([row()]);
    expect(varyingArmFields(arms)).toEqual([...ARM_FIELDS]);
    expect(armLabel(arms[0], varyingArmFields(arms))).toContain("esm2_650m");
    expect(armLabel(arms[0], varyingArmFields(arms))).toContain("k=10");
  });

  it("drops fields an older API build does not send", () => {
    // Rendering "undefined" where the answer is "this build cannot say" is
    // how a gap becomes a value.
    const arms = groupArms([row({ scoring_name: null, metric: null })]);
    expect(armLabel(arms[0], ["scoring_name", "metric"])).toBe("er-1");
  });
});

describe("aspect spelling", () => {
  it("crosses between the form the panels print and the form the column holds", () => {
    // The nine panels are labelled BPO / MFO / CCO; the strata artefact's
    // aspect column holds P / F / C. Pinning an axis with the wrong spelling
    // returns an empty table rather than an error, which reads as "never
    // scored".
    expect(aspectWire("BPO")).toBe("P");
    expect(aspectCafa("P")).toBe("BPO");
    expect(aspectCafa(aspectWire("CCO"))).toBe("CCO");
  });

  it("passes an unknown spelling through rather than inventing one", () => {
    expect(aspectWire("XXX")).toBe("XXX");
    expect(aspectCafa("XXX")).toBe("XXX");
  });
});

describe("isSort", () => {
  it("refuses an ordering the endpoint does not offer", () => {
    expect(isSort("f_asc")).toBe(true);
    expect(isSort("random")).toBe(false);
    expect(isSort(null)).toBe(false);
  });
});

describe("stratumHref", () => {
  it("normalises the aspect so a wire code does not pin an axis to nothing", () => {
    // The strata tables hold P / F / C; the page and the endpoint speak
    // BPO / MFO / CCO. A link built from the raw column value would open a
    // cell that renders empty and reads as "never scored".
    const href = stratumHref({ evaluationSetId: "es", category: "NK", aspect: "P" });
    expect(href.startsWith(`${STRATUM_PATH}?`)).toBe(true);
    const q = new URLSearchParams(href.split("?")[1]);
    expect(q.get("aspect")).toBe("BPO");
    expect(q.get("category")).toBe("NK");
    expect(q.get("set")).toBe("es");
  });

  it("omits the axes the caller did not pin rather than sending them empty", () => {
    // An empty length would pin the axis to the empty string, which no cell
    // carries, instead of leaving the whole panel selected.
    const q = new URLSearchParams(
      stratumHref({ category: "PK", aspect: "CCO", length: null, homology: "" }).split("?")[1],
    );
    expect(q.has("length")).toBe(false);
    expect(q.has("homology")).toBe(false);
    expect(q.has("set")).toBe(false);
    expect(q.has("arm")).toBe(false);
  });

  it("carries the arm when the caller is already inside one", () => {
    const q = new URLSearchParams(
      stratumHref({ category: "LK", aspect: "F", arm: "er-9", homology: "<=30" }).split("?")[1],
    );
    expect(q.get("arm")).toBe("er-9");
    expect(q.get("aspect")).toBe("MFO");
    expect(q.get("homology")).toBe("<=30");
  });
});
