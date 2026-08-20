// The front page's prose, tested for the one thing prose can do silently.
//
// The headline figure was withdrawn by setting it to null. The hero rendered
// it as `HEADLINE.value ?? "being recomputed"` and was fine. The opening
// sentence spliced it in with `+`, and JavaScript stringifies null, so the
// served page read "PROTEA reaches null on the field's headline score" in
// every locale until someone read the page rather than the diff.
//
// A withdrawal is a state the product is meant to be able to occupy. These
// tests are about it staying sayable.

import { describe, expect, it } from "vitest";

import {
  ASPECT_COLS,
  CHAPTER_ZERO,
  HEADLINE,
  KNOWLEDGE_ROWS,
  NINE_CELL,
  PILLARS,
  THESIS_SENTENCE,
  headlineClaim,
} from "@/lib/book";

/** Every string a reader can end up seeing on the front page. */
function frontPageProse(): string[] {
  const out: string[] = [THESIS_SENTENCE];
  for (const m of CHAPTER_ZERO) {
    out.push(m.lead, m.body);
    if (m.link) out.push(m.link.label);
  }
  for (const p of PILLARS) out.push(...Object.values(p).filter((v) => typeof v === "string"));
  return out;
}

describe("no absent value reaches the reader as a word", () => {
  // The class of bug, not the instance. Any future withdrawal spliced into a
  // sentence fails here rather than on the deployed front page.
  it.each(frontPageProse())("%s", (text) => {
    expect(text).not.toMatch(/\b(null|undefined|NaN|\[object Object\])\b/);
  });
});

describe("headlineClaim", () => {
  it("does not name a figure while the figure is withdrawn", () => {
    expect(HEADLINE.value).toBeNull();
    expect(headlineClaim()).not.toMatch(/\bnull\b/);
    expect(headlineClaim()).toContain("withheld while the campaign recomputes");
  });

  it("still makes the claim that survives the withdrawal", () => {
    // The rank is read from the sealed board and the figure is being
    // recomputed, so retracting the second must not retract the first.
    expect(headlineClaim()).toContain("ranks first in seven of the nine");
    expect(headlineClaim()).toContain("sealed board");
  });

  it("reads as one sentence about a score, not as a gap", () => {
    expect(headlineClaim()).toMatch(/^On a fair test, PROTEA ranks first/);
    expect(headlineClaim()).not.toMatch(/\s{2,}|\s\./);
  });
});

describe("the prose agrees with the board it describes", () => {
  it("claims exactly as many carried cells as the board holds", () => {
    // "seven of the nine" is spelled in the sentence and computed in the
    // table. Drift between them is a false claim on the front page, and it is
    // invisible in either file read alone.
    const won = KNOWLEDGE_ROWS.flatMap((k) =>
      ASPECT_COLS.map((a) => NINE_CELL[k][a].won),
    ).filter(Boolean).length;
    const WORD = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"];
    expect(headlineClaim()).toContain(`${WORD[won]} of the nine`);
  });

  it("describes as many cells as the headline counts", () => {
    expect(KNOWLEDGE_ROWS.length * ASPECT_COLS.length).toBe(HEADLINE.totalCells);
  });
});

describe("the withdrawal flag and the figure agree", () => {
  it("does not claim a withdrawal while holding a figure, or the reverse", () => {
    // Two fields say the same thing, so restoring the number and forgetting
    // the flag would leave the front page describing a state it is not in.
    expect(HEADLINE.withdrawn).toBe(HEADLINE.value === null);
  });
});
