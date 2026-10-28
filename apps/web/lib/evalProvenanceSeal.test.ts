/**
 * The seal is what says two numbers may be read side by side.
 *
 * The frame badge beside it says which harness wrote a number, which is a
 * different question: on the live record all eight results carry NULL in
 * `frame` and a digest in `frame_digest`, so reading the label alone shows
 * "frame ?" on eight results that are in fact fully attributable. The two
 * badges are separate for that reason, and an unsealed result says so rather
 * than looking like an unknown.
 */

import { describe, expect, it } from "vitest";
import { frameBadge, sealBadge } from "@/lib/evalProvenance";

const DIGEST = "f-c353408429d4871b759c1072";

describe("sealBadge", () => {
  it("shows a prefix long enough to tell the record's frames apart", () => {
    const badge = sealBadge(DIGEST);
    expect(badge.label).toBe("f-c353408429");
    // The full digest stays reachable, because a shortened one cannot be
    // checked against another result's.
    expect(badge.title).toContain(DIGEST);
  });

  it("says unsealed rather than unknown when there is no digest", () => {
    for (const absent of [null, undefined, "", "   "]) {
      const badge = sealBadge(absent);
      expect(badge.label).toBe("unsealed");
      // The copy has to name the way out, because the way out is one job.
      expect(badge.title).toContain("seal_evaluation_frames");
    }
  });

  it("is not the frame label, and does not read like it", () => {
    // The trap this separation exists to close: one key used to carry the
    // digest at one endpoint and the literal "lafa" at another.
    expect(sealBadge(DIGEST).label).not.toBe(frameBadge("lafa").label);
    expect(sealBadge("lafa").label).toBe("lafa");
    expect(frameBadge(DIGEST).label).toBe("frame ?");
  });
});
