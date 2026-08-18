/**
 * A number on the board must say which generation it came from.
 *
 * A cell can hold several generations of one measurement. The API now returns
 * the most trusted and most recent rather than the highest-scoring, which fixes
 * WHICH number is shown; this badge is what lets a reader see which one they are
 * looking at, and in particular tells them when they are looking at a run the
 * self-hit audit graded as damaged.
 */

import { describe, expect, it } from "vitest";
import { generationBadge, hasAnyProvenance } from "@/lib/evalProvenance";

describe("generationBadge", () => {
  it("returns null only when the field is absent entirely", () => {
    expect(generationBadge(undefined)).toBeNull();
  });

  it("says so, rather than looking clean, when the set was never graded", () => {
    const b = generationBadge(null);
    expect(b?.label).toBe("ungraded");
  });

  it("labels each verdict the audit writes", () => {
    expect(generationBadge("current")?.label).toBe("current");
    expect(generationBadge("superseded")?.label).toBe("superseded");
    expect(generationBadge("damaged")?.label).toBe("damaged");
    expect(generationBadge("incomplete")?.label).toBe("incomplete");
  });

  it("warns in the damaged tooltip rather than only colouring it", () => {
    const b = generationBadge("damaged");
    expect(b?.title).toMatch(/do not read this number/i);
  });

  it("treats a status it has not heard of as ungraded, never as clean", () => {
    const b = generationBadge("something-new");
    expect(b?.label).toBe("ungraded");
  });

  it("appends the self-hit rate when it is known", () => {
    const b = generationBadge("current", 0.9891);
    expect(b?.title).toMatch(/98\.9 per cent/);
  });

  it("omits the rate rather than inventing one when it is null", () => {
    const b = generationBadge("current", null);
    expect(b?.title).not.toMatch(/per cent/);
  });
});

describe("hasAnyProvenance", () => {
  it("counts the generation on its own, so a graded row shows its strip", () => {
    expect(hasAnyProvenance({ prediction_set_status: "damaged" })).toBe(true);
  });

  it("still reports nothing for a row with no provenance at all", () => {
    expect(hasAnyProvenance({})).toBe(false);
  });
});
