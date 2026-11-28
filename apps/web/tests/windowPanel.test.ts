// The window panel's rules, apart from its drawings.

import { describe, expect, it } from "vitest";
import {
  armLabel,
  armModel,
  armShort,
  bandOf,
  channelsLabel,
  bandRanking,
  championSteps,
  modelName,
  modelsOf,
  referenceOf,
  sameExclusion,
  stepComparison,
  visibleResults,
  type SummaryResult,
} from "@/lib/windowPanel";
import {
  listOf,
  readPanelState,
  toggled,
  toggledFocus,
  writePanelState,
} from "@/lib/panelState";
import type { PairedComparison, PanelComparison } from "@/lib/pairedComparison";

function result(id: string, over: Partial<SummaryResult["arm"]> = {}, extra: Partial<SummaryResult> = {}): SummaryResult {
  return {
    id,
    created_at: null,
    prediction_set_id: `set-${id}`,
    job_id: null,
    arm: {
      model: "facebook/esm2_t33_650M_UR50D",
      embedding_config_id: "e",
      scoring: "embedding_only",
      method: null,
      depth: 30,
      exclude_self: true,
      reading: "percentile_rank",
      ...over,
    },
    cells: { "NK:MFO": { f: 0.44, tau: 0.57, edge: null, targets: 1045 } },
    strata: null,
    ...extra,
  };
}

const own = result("own", { model: null, scoring: null, method: "own-annotations-nonexp-lafa-211", depth: null });

describe("the names people read", () => {
  it("names the models as they are known", () => {
    expect(modelName("facebook/esm2_t33_650M_UR50D")).toBe("ESM-2 650M");
    expect(modelName("facebook/esm2_t36_3B_UR50D")).toBe("ESM-2 3B");
    expect(modelName("esmc_600m")).toBe("ESM C 600M");
    expect(modelName("ElnaggarLab/ankh-base")).toBe("Ankh base");
    expect(modelName("Rostlab/prot_t5_xl_half_uniref50-enc")).toBe("ProtT5-XL");
    expect(modelName("Rostlab/ProstT5")).toBe("ProstT5");
  });

  it("labels a neighbour arm by its factors and a method by its name", () => {
    expect(armLabel(result("a", { depth: 1 }))).toBe("ESM-2 650M · embedding_only · k=1");
    expect(armShort(result("a", { depth: 1, scoring: "vote_fraction" }))).toBe("650M vote k=1");
    expect(armLabel(own)).toBe("own annotations");
    expect(armShort(result("m", { model: null, scoring: null, depth: null, method: "count-map-families-strict-lexico-93k-lafa-211" }))).toBe(
      "count map: families (strict)",
    );
    expect(armLabel(result("t", { model: null, scoring: null, depth: null, method: "count-map-themes-lexico-93k-211" }))).toBe(
      "count map: themes",
    );
  });

  it("tells an arm that excluded only the query from the one that excluded its twins too", () => {
    const byAccession = result("acc", { depth: 30, self_exclusion: "accession" });
    const bySequence = result("seq", { depth: 30 });
    expect(armLabel(byAccession)).toBe("ESM-2 650M · embedding_only · k=30 · self by accession");
    expect(armShort(byAccession)).toBe("650M emb k=30 acc");
    expect(armLabel(bySequence)).toBe("ESM-2 650M · embedding_only · k=30");
    expect(sameExclusion(byAccession.arm, bySequence.arm)).toBe(false);
    expect(sameExclusion(bySequence.arm, result("old", { depth: 30, self_exclusion: null }).arm)).toBe(true);
  });

  it("says when the query left only after the asked depth was retrieved", () => {
    const afterRetrieval = result("lafa", { depth: 5, self_exclusion: "accession_after_retrieval" });
    expect(armLabel(afterRetrieval)).toBe("ESM-2 650M · embedding_only · k=5 · query left after retrieval");
    expect(armShort(afterRetrieval)).toBe("650M emb k=5 after");
    const byAccession = result("acc", { depth: 5, self_exclusion: "accession" });
    expect(sameExclusion(afterRetrieval.arm, byAccession.arm)).toBe(false);
  });

  it("tells the breadth levels and the channel sets of one model apart", () => {
    const level = result("l1", { donor_set: { id: "0d1e2f3a-0000", name: "donors-level-1-experimental-211" } });
    const unnamed = result("l2", { donor_set: { id: "9f8e7d6c-0000", name: null } });
    const recipe = result("r", {
      channels: ["truth", "curated", "phylogenetic", "orthology_electronic", "reviewed_field_electronic"],
    });
    const mirror = result("m", {
      channels: [
        "truth",
        "curated",
        "phylogenetic",
        "orthology_electronic",
        "reviewed_field_electronic",
        "signature_electronic",
      ],
    });
    const truth = result("t", { channels: ["truth"], donor_set: level.arm.donor_set });
    expect(armModel(level)).toBe("ESM-2 650M · donors-level-1-experimental-211");
    expect(armModel(unnamed)).toBe("ESM-2 650M · donor set 9f8e7d6c");
    expect(armModel(recipe)).toBe("ESM-2 650M · recipe channels");
    expect(armModel(mirror)).toBe("ESM-2 650M · recipe + signatures");
    expect(armModel(truth)).toBe("ESM-2 650M · donors-level-1-experimental-211 · truth channel");
    expect(armShort(recipe)).toBe("650M · recipe channels emb k=30");
    expect(modelsOf([level, recipe, mirror, result("plain")])).toEqual([
      "ESM-2 650M · donors-level-1-experimental-211",
      "ESM-2 650M · recipe channels",
      "ESM-2 650M · recipe + signatures",
      "ESM-2 650M",
    ]);
    expect(sameExclusion(level.arm, unnamed.arm)).toBe(false);
    expect(sameExclusion(recipe.arm, mirror.arm)).toBe(false);
    expect(sameExclusion(recipe.arm, result("again", { channels: recipe.arm.channels }).arm)).toBe(true);
  });

  it("names the channels as people do", () => {
    expect(channelsLabel(null)).toBeNull();
    expect(channelsLabel([])).toBeNull();
    expect(channelsLabel(["orthology_electronic"])).toBe("orthology_electronic channel");
    expect(channelsLabel(["truth", "curated"])).toBe("channels truth, curated");
  });

  it("names a method built on the own annotations by its own name, never as them", () => {
    const ranked = result("k", {
      model: "own-annotations-ranked-coan-lafa-194",
      scoring: null,
      depth: null,
      method: "own-annotations-ranked-coan-lafa-194",
    });
    expect(armLabel(ranked)).toBe("own-annotations-ranked-coan");
    expect(armShort(ranked)).toBe("own-annotations-ranked-coan");
    expect(referenceOf([ranked, own], null)?.id).toBe("own");
  });
});

describe("what the panel shows", () => {
  const raw = result("raw", { reading: "raw" });
  const selfVote = result("self", { exclude_self: false });
  const k1 = result("k1", { depth: 1 });
  const ankh = result("ankh", { model: "ElnaggarLab/ankh-base" });
  const all = [own, raw, selfVote, k1, ankh];
  const base = { reading: "percentile_rank", superseded: false, models: [], scorings: [], depths: [] };

  it("reads one reading, hides the superseded arms and keeps the methods", () => {
    expect(visibleResults(all, base).map((r) => r.id)).toEqual(["own", "k1", "ankh"]);
    expect(visibleResults(all, { ...base, superseded: true }).map((r) => r.id)).toContain("self");
  });

  it("keeps the arms that kept their own sequence where nothing in the window excluded it", () => {
    // Production's shape: two method arms with no exclusion and every neighbour arm
    // keeping the first hit on purpose. Hiding them left one arm of 114 on screen.
    const method = result("m", { model: null, scoring: null, method: "own-annotations-nonexp-lafa-211", depth: null, exclude_self: null });
    const a = result("a", { exclude_self: false });
    const b = result("b", { exclude_self: false, model: "ElnaggarLab/ankh-base" });
    expect(visibleResults([method, a, b], base).map((r) => r.id)).toEqual(["m", "a", "b"]);
  });

  it("filters the neighbour arms by model and depth, never the methods", () => {
    expect(visibleResults(all, { ...base, models: ["Ankh base"] }).map((r) => r.id)).toEqual(["own", "ankh"]);
    expect(visibleResults(all, { ...base, depths: [1] }).map((r) => r.id)).toEqual(["own", "k1"]);
  });

  it("refers to own annotations unless another shown arm is asked for", () => {
    const shown = visibleResults(all, base);
    expect(referenceOf(shown, null)?.id).toBe("own");
    expect(referenceOf(shown, "k1")?.id).toBe("k1");
    expect(referenceOf(shown, "raw")?.id).toBe("own");
  });

  it("lists the models in the order the window met them", () => {
    expect(modelsOf(all)).toEqual(["ESM-2 650M", "Ankh base"]);
  });
});

describe("bands, champions and winners", () => {
  const profiles = (f: number) => ({
    job_id: "j",
    variant: "weighted",
    profiles: { NK: { F: { homology: { ">90": [311, 1, 2, 3, f] as [number, number, number, number, number], "<=30": [12, 1, 1, 1, 0.9] as [number, number, number, number, number] } } } },
  });
  const a = result("a", { depth: 1 }, { strata: profiles(0.587) });
  const b = result("b", { depth: 30 }, { strata: profiles(0.511) });
  const o = { ...own, strata: profiles(0.537) };

  it("reads a band from the profile of the cell's aspect", () => {
    expect(bandOf(a, "NK:MFO", "homology", ">90")).toEqual({ n: 311, f: 0.587 });
    expect(bandOf(a, "NK:BPO", "homology", ">90")).toBeNull();
  });

  it("ranks a band and measures the winner against the other family", () => {
    const { ranked, rival } = bandRanking([o, a, b], "NK:MFO", "homology", ">90");
    expect(ranked.map((x) => x.result.id)).toEqual(["a", "own", "b"]);
    expect(rival?.result.id).toBe("own");
  });

  it("leaves out a band under the floor", () => {
    expect(bandRanking([o, a], "NK:MFO", "homology", "<=30").ranked).toEqual([]);
  });

  it("steps only when the best so far improves", () => {
    const steps = championSteps([
      { item: "x", value: 0.4 },
      { item: "y", value: 0.3 },
      { item: "z", value: 0.5 },
    ]);
    expect(steps.map((s) => [s.item, s.before, s.now, s.at])).toEqual([
      ["x", null, 0.4, 0],
      ["z", 0.4, 0.5, 2],
    ]);
  });
});

describe("the state in the address", () => {
  it("reads defaults for what the address does not say", () => {
    const state = readPanelState(new URLSearchParams("tab=strata&cell=PK:BPO&result=abc"));
    expect(state.tab).toBe("strata");
    expect(state.cell).toBe("PK:BPO");
    expect(state.reading).toBe("raw");
  });

  it("keeps the page's own parameters and drops the panel's defaults", () => {
    const next = writePanelState(new URLSearchParams("result=abc&tab=strata"), { tab: "summary", band: ">90" });
    expect(next.get("result")).toBe("abc");
    expect(next.has("tab")).toBe(false);
    expect(next.get("band")).toBe(">90");
  });

  it("toggles lists and keeps three arms in focus at most", () => {
    expect(listOf("")).toEqual([]);
    expect(toggled("a,b", "a")).toBe("b");
    expect(toggled("b", "c")).toBe("b,c");
    expect(toggledFocus("a,b,c", "d")).toBe("b,c,d");
    expect(toggledFocus("a,b", "a")).toBe("b");
  });
});

describe("a trajectory step's paired comparison", () => {
  const panel = (over: Partial<PanelComparison> = {}): PanelComparison => ({
    delta: 0.012,
    ci_low: 0.004,
    ci_high: 0.02,
    interval_method: "bca",
    verdict: "a_greater",
    status: "ok",
    n_paired: 1045,
    ...over,
  });
  const comparison = (arm: string, baseline: string, panels: Record<string, PanelComparison>): PairedComparison => ({
    job_id: `job-${arm}-${baseline}`,
    evaluation_result_id: arm,
    baseline_evaluation_result_id: baseline,
    finished_at: null,
    method_axis: ["scoring"],
    operating_point: "reselected_per_resample",
    weighting: "ia_weighted",
    n_resamples: 2000,
    seed: 0,
    confidence: 0.95,
    panels,
  });

  it("reads a comparison run as the step was, the new best under test", () => {
    const found = stepComparison([comparison("new", "old", { "NK:MFO": panel() })], "new", "old", "NK:MFO");
    expect(found?.panel).toEqual(panel());
  });

  it("turns a comparison run the other way round: difference and interval negated, verdict swapped", () => {
    const found = stepComparison([comparison("old", "new", { "NK:MFO": panel() })], "new", "old", "NK:MFO");
    expect(found?.panel.delta).toBeCloseTo(-0.012);
    expect(found?.panel.ci_low).toBeCloseTo(-0.02);
    expect(found?.panel.ci_high).toBeCloseTo(-0.004);
    expect(found?.panel.verdict).toBe("b_greater");
  });

  it("prefers the comparison run as the step was when both exist", () => {
    const both = [
      comparison("old", "new", { "NK:MFO": panel({ delta: 0.5 }) }),
      comparison("new", "old", { "NK:MFO": panel({ delta: 0.01 }) }),
    ];
    expect(stepComparison(both, "new", "old", "NK:MFO")?.panel.delta).toBe(0.01);
  });

  it("leaves an unresolved verdict as it is when turned over", () => {
    const found = stepComparison(
      [comparison("old", "new", { "NK:MFO": panel({ verdict: "not_resolved", status: "null_unread" }) })],
      "new",
      "old",
      "NK:MFO",
    );
    expect(found?.panel.verdict).toBe("not_resolved");
  });

  it("is null when the pair was never compared, or not on that cell", () => {
    const one = [comparison("new", "old", { "NK:MFO": panel() })];
    expect(stepComparison(one, "new", "other", "NK:MFO")).toBeNull();
    expect(stepComparison(one, "new", "old", "LK:BPO")).toBeNull();
  });
});

