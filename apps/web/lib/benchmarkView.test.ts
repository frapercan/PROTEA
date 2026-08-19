// What the benchmark shows, tested apart from how it draws it.
//
// These were forty lines of intermediate consts inside a 1,133-line component.
// Two of them are fallbacks whose whole job is to stop the page going blank,
// and a fallback nobody can test is a fallback nobody knows still works.

import { describe, expect, it } from "vitest";

import type { BenchmarkEvalSet, BenchmarkRow, BenchmarkStage } from "@/lib/api";
import {
  activeEvalSet,
  emptyKind,
  preferCatalog,
  resolveLineage,
  rowsInCategories,
  stageContext,
  visibleEvalSets,
} from "@/lib/benchmarkView";

const evalSet = (id: string): BenchmarkEvalSet => ({ id }) as BenchmarkEvalSet;
const row = (category: string): BenchmarkRow => ({ category }) as BenchmarkRow;
const stage = (name: string, label: string): BenchmarkStage =>
  ({ name, label }) as BenchmarkStage;

describe("visibleEvalSets", () => {
  const all = [evalSet("a"), evalSet("b"), evalSet("c")];

  it("hides sets with no curated rows", () => {
    // Probe and test-only runs: listing them invites selecting an empty page.
    expect(
      visibleEvalSets(all, new Set(["a"]), "a", false).map((e) => e.id),
    ).toEqual(["a"]);
  });

  it("always keeps the selected set, even with no rows", () => {
    // Otherwise the dropdown loses its own value and the page jumps.
    expect(
      visibleEvalSets(all, new Set(["a"]), "c", false).map((e) => e.id),
    ).toEqual(["a", "c"]);
  });

  it("shows everything when the escape hatch is on", () => {
    expect(visibleEvalSets(all, new Set(), "a", true)).toHaveLength(3);
  });
});

describe("resolveLineage", () => {
  const chip = { key: "pk_only", label: "PK only", cats: ["PK"], tooltip: "" };

  it("keeps the categories the chip asks for", () => {
    const out = resolveLineage(["NK", "LK", "PK"], chip);
    expect(out.categories).toEqual(["PK"]);
    expect(out.fellBack).toBe(false);
  });

  it("falls back to every category when the chip matches nothing", () => {
    // A benchmark with only NK rows, and the reader clicks "PK only". Without
    // the fallback every category-keyed panel empties, which reads as "no
    // data" rather than "this filter excludes everything here".
    const out = resolveLineage(["NK"], chip);
    expect(out.categories).toEqual(["NK"]);
    expect(out.fellBack).toBe(true);
  });

  it("reports the fallback rather than applying it silently", () => {
    expect(resolveLineage(["NK"], chip).fellBack).toBe(true);
  });

  it("does not claim a fallback when there was nothing to filter", () => {
    const out = resolveLineage([], chip);
    expect(out.categories).toEqual([]);
    expect(out.fellBack).toBe(false);
  });

  it("treats a missing chip as matching nothing, then falls back", () => {
    expect(resolveLineage(["NK"], undefined).fellBack).toBe(true);
  });
});

describe("rowsInCategories", () => {
  it("keeps only rows in the surviving categories", () => {
    const rows = [row("NK"), row("PK"), row("LK")];
    expect(rowsInCategories(rows, ["NK", "LK"]).map((r) => r.category)).toEqual(
      ["NK", "LK"],
    );
  });

  it("keeps nothing when no category survived", () => {
    expect(rowsInCategories([row("NK")], [])).toEqual([]);
  });
});

describe("activeEvalSet", () => {
  it("names the selected set", () => {
    expect(activeEvalSet([evalSet("a"), evalSet("b")], "b")?.id).toBe("b");
  });

  it("names the only set even when 'all' is selected", () => {
    // A reader looking at one set's numbers should see whose they are.
    expect(activeEvalSet([evalSet("a")], "all")?.id).toBe("a");
  });

  it("names none when 'all' covers several", () => {
    // Naming one of them would be a lie about what is on screen.
    expect(activeEvalSet([evalSet("a"), evalSet("b")], "all")).toBeNull();
  });

  it("returns null for a selection that is not in the list", () => {
    expect(activeEvalSet([evalSet("a")], "zz")).toBeNull();
  });
});

describe("emptyKind", () => {
  it("separates 'curation hid everything' from 'there is nothing'", () => {
    // The first points at the Show all escape hatch; the second would be a lie
    // if the matrix actually has rows.
    expect(emptyKind([], 40)).toBe("curated-to-empty");
    expect(emptyKind([], 0)).toBe("empty");
    expect(emptyKind([row("NK")], 40)).toBe("none");
  });
});

describe("preferCatalog", () => {
  it("prefers the catalogue when it has anything", () => {
    expect(preferCatalog(["a"], ["b", "c"])).toEqual(["a"]);
  });

  it("falls back to the matrix when the catalogue is empty", () => {
    expect(preferCatalog([], ["b", "c"])).toEqual(["b", "c"]);
  });
});

describe("stageContext", () => {
  const catalogue = [stage("knn", "KNN"), stage("reranker", "Reranker")];

  it("labels the current stage from the list", () => {
    expect(stageContext(catalogue, [], "reranker").label).toBe("Reranker");
  });

  it("falls back to the raw name for a stage with no entry", () => {
    // Better the name than an empty label beside a number.
    expect(stageContext(catalogue, [], "mystery").label).toBe("mystery");
  });

  it("labels nothing when no stage is selected", () => {
    expect(stageContext(catalogue, [], null).label).toBe("");
  });
});
