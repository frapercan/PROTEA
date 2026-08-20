// What the campaign line claims, tested apart from how it draws it.

import { describe, expect, it } from "vitest";

import {
  currentRung,
  defaultEvalSet,
  progressLabel,
  rungProgress,
  type Rung,
} from "@/lib/rungs";

function rung(over: Partial<Rung> = {}): Rung {
  return {
    rung: "1",
    window: "220-230",
    question: "which of 8 representations",
    models: [],
    ks: [],
    arms: 48,
    succeeded: 48,
    running: 0,
    failed: 0,
    evaluated: 32,
    evaluation_set_ids: ["es-1"],
    best: null,
    started_at: "2026-08-19T00:00:00Z",
    ...over,
  };
}

describe("currentRung", () => {
  it("is the highest-numbered rung, not the most recent arm", () => {
    // A rung reopened to add an arm is still the rung it was. Sorting by
    // timestamp would let a late arm on rung 1 displace rung 2.
    const rungs = [
      rung({ rung: "2", started_at: "2026-08-01T00:00:00Z" }),
      rung({ rung: "1", started_at: "2026-08-20T00:00:00Z" }),
    ];
    expect(currentRung(rungs)?.rung).toBe("2");
  });

  it("is null when there are no rungs", () => {
    expect(currentRung([])).toBeNull();
  });
});

describe("defaultEvalSet", () => {
  it("opens the board on the current rung's set", () => {
    expect(defaultEvalSet([rung({ evaluation_set_ids: ["es-9"] })])).toBe("es-9");
  });

  it("declines to choose when the rung produced several", () => {
    // Picking one would be a claim about which of them is the rung.
    expect(defaultEvalSet([rung({ evaluation_set_ids: ["a", "b"] })])).toBeNull();
  });

  it("declines when the rung has produced none yet", () => {
    expect(defaultEvalSet([rung({ evaluation_set_ids: [] })])).toBeNull();
  });
});

describe("rungProgress", () => {
  it("separates computed from scored", () => {
    // They lag independently, and a rung with every arm computed and none
    // scored looks finished from the queue while having no results.
    const p = rungProgress(rung({ arms: 48, succeeded: 48, evaluated: 32 }));
    expect(p).toEqual({ computed: 48, scored: 32, total: 48, live: false });
  });

  it("never reports more scored than computed", () => {
    // Stale evaluations of a withdrawn arm would otherwise read as
    // progress the rung has not made.
    const p = rungProgress(rung({ succeeded: 10, evaluated: 40 }));
    expect(p.scored).toBe(10);
  });

  it("is live while any arm is still computing", () => {
    expect(rungProgress(rung({ running: 1 })).live).toBe(true);
  });
});

describe("progressLabel", () => {
  it("says so when everything is scored", () => {
    expect(progressLabel({ computed: 48, scored: 48, total: 48, live: false }))
      .toBe("48 arms, all scored");
  });

  it("distinguishes computed-but-unscored from still-computing", () => {
    expect(progressLabel({ computed: 48, scored: 32, total: 48, live: false }))
      .toBe("48 arms computed, 32 scored");
    expect(progressLabel({ computed: 40, scored: 32, total: 48, live: true }))
      .toBe("40 of 48 arms computed, 32 scored");
  });
});
