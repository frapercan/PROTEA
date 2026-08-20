// What the front door says about the campaign, tested apart from how it
// draws it.
//
// The page argues from a sealed board whose figures are withdrawn and whose
// window closed in March, and said nothing about whether anything is still
// running. The risk in fixing that is a sentence that keeps claiming work
// is open after it stops, which is the same class of unsupported claim the
// rest of this page spent a day losing.
//
// So the assertions here are about the words following the numbers.

import { describe, expect, it } from "vitest";

import { currentRung, progressLabel, rungProgress, type Rung } from "@/lib/rungs";

const rung = (over: Partial<Rung> = {}): Rung =>
  ({
    rung: "1",
    window: "220-230",
    question: "which of 8 representations",
    models: [],
    ks: [],
    arms: 432,
    succeeded: 432,
    running: 0,
    failed: 0,
    evaluated: 432,
    evaluation_set_ids: [],
    window_dates: { from: "2024-04-16", to: "2026-03-04" },
    best: null,
    started_at: "2026-08-19T13:00:00Z",
    ...over,
  }) as Rung;

/** The condition the component branches its verb on. */
const settled = (r: Rung) => {
  const p = rungProgress(r);
  return !p.live && p.scored === p.total;
};

describe("settled is finished, not merely quiet", () => {
  it("is settled when every arm is scored and nothing runs", () => {
    expect(settled(rung())).toBe(true);
  });

  it("is not settled while an arm is still computing", () => {
    // Every arm scored AND one running is a rung that has reopened, which
    // is a real state: an arm can be added to a rung after it looked done.
    expect(settled(rung({ running: 1 }))).toBe(false);
  });

  it("is not settled when nothing runs and arms are unscored", () => {
    // Stalled rather than done. Saying "asked" here would report a
    // finished question over an unfinished measurement.
    expect(settled(rung({ evaluated: 100 }))).toBe(false);
  });
});

describe("the progress sentence keeps the two steps apart", () => {
  it("says all scored only when they are", () => {
    expect(progressLabel(rungProgress(rung()))).toBe("432 arms, all scored");
  });

  it("distinguishes computed from scored", () => {
    // The state that looks finished from the queue and is not.
    expect(progressLabel(rungProgress(rung({ evaluated: 0 })))).toBe(
      "432 arms computed, 0 scored",
    );
  });

  it("never reports more scored than computed", () => {
    // evaluated can exceed succeeded when a set is re-scored, and a line
    // reading "432 of 200 computed" would look like a bug in the campaign
    // rather than in the label.
    const p = rungProgress(rung({ succeeded: 200, evaluated: 432 }));
    expect(p.scored).toBeLessThanOrEqual(p.computed);
  });
});

describe("which rung the door speaks about", () => {
  it("is the highest numbered, not the most recent", () => {
    // A rung reopened to add an arm is still the rung it was, and sorting
    // by time would let a late arm on rung 1 displace rung 2.
    const rs = [
      rung({ rung: "2", started_at: "2026-08-20T09:00:00Z" }),
      rung({ rung: "1", started_at: "2026-08-20T21:00:00Z" }),
    ];
    expect(currentRung(rs)?.rung).toBe("2");
  });

  it("is null when the campaign could not be read", () => {
    // What a failed fetch looks like from here. The component renders
    // nothing rather than a shape with no content.
    expect(currentRung([])).toBeNull();
  });
});
