// Unit tests for the method-surface provenance resolvers
// (slice F-METHOD-EVAL-SURFACE). The central guarantee is that every
// resolver returns a renderable, defined value for any input, including
// the null / undefined / unknown cases that legacy EvaluationResult rows
// produce (they predate the provenance columns).

import { describe, expect, it } from "vitest";
import {
  armsList,
  frameBadge,
  hasAnyProvenance,
  leakageBadge,
  windowLabel,
} from "@/lib/evalProvenance";

describe("frameBadge", () => {
  it("maps known frames to their badge", () => {
    expect(frameBadge("lafa").label).toBe("LAFA-frame");
    expect(frameBadge("lafa").className).toContain("violet");
    expect(frameBadge("internal").label).toBe("internal");
  });

  it("returns the unknown state for null / undefined / unexpected values", () => {
    expect(frameBadge(null).label).toBe("frame ?");
    expect(frameBadge(undefined).label).toBe("frame ?");
    expect(frameBadge("weird").label).toBe("frame ?");
  });
});

describe("windowLabel", () => {
  it("classifies SELECT windows", () => {
    const w = windowLabel("SELECT_220_227");
    expect(w.kind).toBe("select");
    expect(w.label).toBe("SELECT_220_227");
    expect(w.className).toContain("amber");
  });

  it("classifies FINAL / TEST windows as the report-once kind", () => {
    expect(windowLabel("FINAL_227_230").kind).toBe("final");
    expect(windowLabel("TEST_227_230").kind).toBe("final");
  });

  it("treats any other token as other and keeps the raw label", () => {
    const w = windowLabel("adhoc-band");
    expect(w.kind).toBe("other");
    expect(w.label).toBe("adhoc-band");
  });

  it("returns the unknown state for null / empty", () => {
    expect(windowLabel(null).kind).toBe("unknown");
    expect(windowLabel("").kind).toBe("unknown");
    expect(windowLabel("   ").kind).toBe("unknown");
  });
});

describe("armsList", () => {
  it("returns every canonical arm in order with its enabled flag", () => {
    const arms = armsList({ knn: true, reranker: true, mlp_tower: false, interpro: false });
    expect(arms?.map((a) => a.key)).toEqual(["knn", "reranker", "mlp_tower", "interpro"]);
    expect(arms?.find((a) => a.key === "knn")?.enabled).toBe(true);
    expect(arms?.find((a) => a.key === "interpro")?.enabled).toBe(false);
  });

  it("defaults missing arm keys to disabled", () => {
    const arms = armsList({ knn: true });
    expect(arms?.find((a) => a.key === "reranker")?.enabled).toBe(false);
  });

  it("returns null when the composition was never recorded", () => {
    expect(armsList(null)).toBeNull();
    expect(armsList(undefined)).toBeNull();
  });
});

describe("leakageBadge", () => {
  it("maps known roles", () => {
    expect(leakageBadge("select").label).toBe("select");
    expect(leakageBadge("test").className).toContain("emerald");
    expect(leakageBadge("probe").className).toContain("sky");
  });

  it("returns the unknown state otherwise", () => {
    expect(leakageBadge(null).label).toBe("role ?");
    expect(leakageBadge("nonsense").label).toBe("role ?");
  });
});

describe("hasAnyProvenance", () => {
  it("is false for empty / all-null payloads", () => {
    expect(hasAnyProvenance(null)).toBe(false);
    expect(hasAnyProvenance({})).toBe(false);
    expect(
      hasAnyProvenance({ frame: null, temporal_window: null, arms_enabled: null, leakage_role: null }),
    ).toBe(false);
  });

  it("is true when any single field is populated", () => {
    expect(hasAnyProvenance({ frame: "lafa" })).toBe(true);
    expect(hasAnyProvenance({ temporal_window: "FINAL_227_230" })).toBe(true);
    expect(hasAnyProvenance({ leakage_role: "test" })).toBe(true);
    expect(hasAnyProvenance({ arms_enabled: { knn: true } })).toBe(true);
  });
});
