// The window panel's rules, apart from its drawings.

import { describe, expect, it } from "vitest";
import {
  armLabel,
  armShort,
  bandOf,
  bandRanking,
  championSteps,
  modelName,
  modelsOf,
  referenceOf,
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
    expect(armLabel(own)).toBe("own-annotations-nonexp-lafa-211");
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
    expect(state.reading).toBe("percentile_rank");
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
