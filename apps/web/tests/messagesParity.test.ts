// Every locale carries every key.
//
// Written on 2026-10-07 after adding five keys to five catalogues by
// hand. A missing key does not fail the build: next-intl renders the key
// path itself, so the page silently shows "home.annotatePausedTitle" to
// whoever reads that language. That had already happened once, with
// functionalAnnotation.resultsTab.tableHeaders.k absent from zh.json.
//
// Keys starting with "_" are notes to translators, not strings the UI
// renders, so they are allowed to differ.

import { describe, expect, it } from "vitest";
import de from "@/messages/de.json";
import en from "@/messages/en.json";
import es from "@/messages/es.json";
import pt from "@/messages/pt.json";
import zh from "@/messages/zh.json";

type Catalogue = Record<string, unknown>;

function leafKeys(node: Catalogue, prefix = ""): string[] {
  return Object.entries(node).flatMap(([key, value]) => {
    if (key.startsWith("_")) return [];
    const path = prefix ? `${prefix}.${key}` : key;
    return value !== null && typeof value === "object" && !Array.isArray(value)
      ? leafKeys(value as Catalogue, path)
      : [path];
  });
}

const reference = leafKeys(en as Catalogue);
const others: ReadonlyArray<[string, Catalogue]> = [
  ["es", es as Catalogue],
  ["de", de as Catalogue],
  ["pt", pt as Catalogue],
  ["zh", zh as Catalogue],
];

describe("message catalogues", () => {
  it("English has keys to compare against", () => {
    expect(reference.length).toBeGreaterThan(1_500);
  });

  for (const [name, catalogue] of others) {
    it(`${name} defines every English key, and no key English lacks`, () => {
      const keys = new Set(leafKeys(catalogue));
      expect(reference.filter((k) => !keys.has(k))).toEqual([]);
      expect([...keys].filter((k) => !reference.includes(k))).toEqual([]);
    });
  }

  it("every locale states the paused predictor", () => {
    // The strings a visitor meets while the corpus is being rebuilt.
    for (const [name, catalogue] of [["en", en as Catalogue], ...others]) {
      const home = (catalogue as { home: Record<string, string> }).home;
      for (const key of [
        "annotatePausedTitle",
        "annotatePausedBody",
        "annotatePausedIngest",
        "annotatePausedApi",
        "annotatePausedInstrument",
      ]) {
        expect(home[key], `${name}.home.${key}`).toBeTruthy();
      }
    }
  });
});
