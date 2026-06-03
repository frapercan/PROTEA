// Regression test for the home-showcase stage-badge crash: an unknown
// `best.stage` used to resolve STAGE_LABELS[stage] to undefined, which
// next-intl's t() rejected with MISSING_MESSAGE, crashing the page.

import { describe, expect, it } from "vitest";
import { stageBadgeClass, stageLabelKey } from "@/lib/stageBadge";

describe("stageBadgeClass", () => {
  it("returns the mapped class for known stages", () => {
    expect(stageBadgeClass("baseline")).toContain("bg-slate-100");
    expect(stageBadgeClass("alignment_weighted")).toContain("bg-amber-50");
    expect(stageBadgeClass("reranker")).toContain("bg-blue-50");
  });

  it("falls back to the neutral slate badge for an unknown stage", () => {
    expect(stageBadgeClass("totally_new_stage")).toBe(
      "bg-slate-100 text-slate-700 ring-1 ring-inset ring-slate-200",
    );
    expect(stageBadgeClass("")).toBe(
      "bg-slate-100 text-slate-700 ring-1 ring-inset ring-slate-200",
    );
  });
});

describe("stageLabelKey", () => {
  it("returns the i18n key for known stages", () => {
    expect(stageLabelKey("baseline")).toBe("pipelineStageBaseline");
    expect(stageLabelKey("alignment_weighted")).toBe(
      "pipelineStageAlignmentWeighted",
    );
    expect(stageLabelKey("reranker")).toBe("pipelineStageReranker");
  });

  it("returns null for an unknown stage so t() is never called with undefined", () => {
    expect(stageLabelKey("totally_new_stage")).toBeNull();
    expect(stageLabelKey("")).toBeNull();
  });
});
