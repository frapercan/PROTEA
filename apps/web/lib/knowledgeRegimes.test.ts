import { describe, expect, it } from "vitest";

import { KNOWLEDGE_LABEL as BOOK_LABEL } from "@/lib/book";
import {
  KNOWLEDGE_DEFINITION,
  KNOWLEDGE_LABEL,
  KNOWLEDGE_LABEL_LOWER,
  KNOWLEDGE_REGIMES,
  KNOWLEDGE_USES_KNOWN,
  asKnowledgeRegime,
} from "@/lib/knowledgeRegimes";
import { axisLabel } from "@/lib/strata";

describe("the three knowledge regimes are defined once", () => {
  it("names PK Partial and never Prior", () => {
    // Five surfaces defined these independently and disagreed: PK expanded to
    // "Prior knowledge" in book.ts and strata.ts and to "Partial Knowledge" in
    // three others. CAFA's own term, and the one that matches the -known
    // behaviour, is Partial.
    expect(KNOWLEDGE_LABEL.PK).toBe("Partial Knowledge");
    for (const label of [
      ...Object.values(KNOWLEDGE_LABEL),
      ...Object.values(KNOWLEDGE_LABEL_LOWER),
      ...Object.values(KNOWLEDGE_DEFINITION),
    ]) {
      expect(label.toLowerCase()).not.toContain("prior");
    }
  });

  it("covers every regime in every map", () => {
    expect(KNOWLEDGE_REGIMES).toEqual(["NK", "LK", "PK"]);
    for (const r of KNOWLEDGE_REGIMES) {
      expect(KNOWLEDGE_LABEL[r]).toBeTruthy();
      expect(KNOWLEDGE_LABEL_LOWER[r]).toBeTruthy();
      expect(KNOWLEDGE_DEFINITION[r]).toBeTruthy();
      expect(typeof KNOWLEDGE_USES_KNOWN[r]).toBe("boolean");
    }
  });

  it("casing is the only difference between the two label maps", () => {
    for (const r of KNOWLEDGE_REGIMES) {
      expect(KNOWLEDGE_LABEL_LOWER[r]).toBe(KNOWLEDGE_LABEL[r].toLowerCase());
    }
  });

  it("says -known for PK only, which is what the flag does", () => {
    expect(KNOWLEDGE_USES_KNOWN).toEqual({ NK: false, LK: false, PK: true });
    // The definition has to carry the consequence, not just the rule: it is
    // the half a reader needs to interpret the number.
    expect(KNOWLEDGE_DEFINITION.PK).toContain("with -known");
    expect(KNOWLEDGE_DEFINITION.NK).toContain("without -known");
    expect(KNOWLEDGE_DEFINITION.LK).toContain("without -known");
  });

  it("every definition states its t0 rule", () => {
    for (const r of KNOWLEDGE_REGIMES) {
      expect(KNOWLEDGE_DEFINITION[r]).toContain("t0");
      expect(KNOWLEDGE_DEFINITION[r].startsWith(KNOWLEDGE_LABEL[r])).toBe(true);
    }
  });

  it("the consumers that kept their own name now resolve to this one", () => {
    // book.ts re-exports, and strata.ts feeds its prose axis from the
    // lower-case map. If either drifts back to a private copy, this fails.
    expect(BOOK_LABEL).toBe(KNOWLEDGE_LABEL);
    for (const r of KNOWLEDGE_REGIMES) {
      expect(axisLabel("category", r)).toBe(KNOWLEDGE_LABEL_LOWER[r]);
    }
  });

  it("narrows a string only when it is a regime", () => {
    expect(asKnowledgeRegime("PK")).toBe("PK");
    expect(asKnowledgeRegime("pk")).toBeUndefined();
    expect(asKnowledgeRegime("XX")).toBeUndefined();
    expect(asKnowledgeRegime("")).toBeUndefined();
  });
});
