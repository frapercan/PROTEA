import { describe, expect, it } from "vitest";
import { CHANGES, LOCALES, PLANS, inLocale } from "./news";

describe("the front page's log and plans", () => {
  it("writes every entry in all five locales", () => {
    for (const entry of [...CHANGES, ...PLANS]) {
      for (const locale of LOCALES) {
        expect(entry.text[locale]?.trim(), `${locale}: ${entry.text.en}`).toBeTruthy();
      }
    }
  });

  it("dates every change as a calendar day, newest first", () => {
    const dates = CHANGES.map((c) => c.date);
    for (const d of dates) expect(d).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect([...dates].sort().reverse()).toEqual(dates);
  });

  it("falls back to English for a locale the site does not carry", () => {
    expect(inLocale(CHANGES[0].text, "fr")).toBe(CHANGES[0].text.en);
    expect(inLocale(CHANGES[0].text, "es")).toBe(CHANGES[0].text.es);
  });
});
