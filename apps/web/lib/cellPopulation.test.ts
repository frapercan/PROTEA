// The population note, tested against the case that motivated it.

import { describe, expect, it } from "vitest";

import {
  POPULATION_SHARE_FLOOR,
  medianPopulation,
  populationNote,
} from "@/lib/cellPopulation";

const row = (n: number | null) => ({ n_proteins: n });

describe("medianPopulation", () => {
  it("takes the middle of an odd list", () => {
    expect(medianPopulation([row(100), row(700), row(680)])).toBe(680);
  });

  it("averages the middle pair of an even list", () => {
    expect(medianPopulation([row(600), row(700)])).toBe(650);
  });

  it("ignores rows carrying no count rather than treating them as zero", () => {
    // Counting a missing value as 0 would drag the median down and stop the
    // marker firing on exactly the rows it exists for.
    expect(medianPopulation([row(null), row(680), row(680)])).toBe(680);
  });

  it("ignores non-positive and non-finite counts", () => {
    expect(medianPopulation([row(0), row(NaN), row(500)])).toBe(500);
  });

  it("is null when nothing carries a count", () => {
    expect(medianPopulation([row(null), row(null)])).toBeNull();
  });
});

describe("populationNote", () => {
  it("marks the case this was built for: 106 against a norm near 680", () => {
    const peers = [row(680), row(696), row(106), row(671), row(682)];
    const note = populationNote(row(106), peers);
    expect(note.underpopulated).toBe(true);
    expect(note.count).toBe(106);
    expect(note.label).toContain("106");
    expect(note.label).toContain("median");
  });

  it("does not mark the ordinary spread between comparable arms", () => {
    // 6,116 against 6,264 is under 3 per cent. Marking that would make the
    // badge meaningless by firing on every row.
    const peers = [row(6116), row(6234), row(6264), row(6237)];
    expect(populationNote(row(6116), peers).underpopulated).toBe(false);
  });

  it("does not mark a count that moved with the threshold", () => {
    // The platform records a 17 per cent move on an identical cohort. The
    // floor has to clear that, or the marker reports the operating point.
    const peers = [row(600), row(680), row(700), row(660)];
    expect(populationNote(row(566), peers).underpopulated).toBe(false);
  });

  it("marks exactly at the floor and not just above it", () => {
    const peers = [row(100), row(100), row(100)];
    expect(populationNote(row(49), peers).underpopulated).toBe(true);
    expect(populationNote(row(50), peers).underpopulated).toBe(false);
    expect(POPULATION_SHARE_FLOOR).toBe(0.5);
  });

  it("never marks a row with no one to be out of step with", () => {
    // One arm in a cell is not anomalous, it is alone.
    expect(populationNote(row(106), [row(106)]).underpopulated).toBe(false);
  });

  it("still reports the count when it cannot mark", () => {
    expect(populationNote(row(106), [row(106)]).label).toBe("over 106 proteins");
  });

  it("says the count is absent rather than implying it is small", () => {
    // "not recorded" and "small" are different facts, and only one of them is
    // a reason to distrust the score.
    const note = populationNote(row(null), [row(680), row(690)]);
    expect(note.count).toBeNull();
    expect(note.underpopulated).toBe(false);
    expect(note.label).toBe("population not recorded");
  });

  it("formats large counts with separators so they can be read at a glance", () => {
    expect(populationNote(row(6234), [row(6234)]).label).toBe(
      `over ${(6234).toLocaleString()} proteins`,
    );
  });
});
